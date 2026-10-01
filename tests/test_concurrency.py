"""并发转账：不丢更新、不透支、守恒；重复提交不重复入账。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


from ledger.errors import IdempotencyConflict, InsufficientFunds


def test_concurrent_transfers_no_lost_update_and_conservation(
    service, caplog
):
    """a/b 两个账户互相并发转 100 笔，各自串行后的结果必须精确守恒。"""
    # 双方各有 1000，保证双向转账都有足额资金，专注检验丢失更新与守恒。
    service.create_tenant("t1", "租户甲", "key-1", currencies=("CNY",))
    service.open_account("t1", "a", "CNY", initial_balance=1000)
    service.open_account("t1", "b", "CNY", initial_balance=1000)
    n = 100
    errors: list[Exception] = []
    barrier = threading.Barrier(n * 2)  # 让所有线程尽可能同时起跑

    def do_transfer(direction: int, idx: int):
        frm, to = ("a", "b") if direction == 0 else ("b", "a")
        barrier.wait()
        service.transfer(
            tenant_id="t1",
            idempotency_key=f"conc-{direction}-{idx}",
            request_id=f"req-conc-{direction}-{idx}",
            from_account=frm, to_account=to,
            amount=1, fee=0, currency="CNY",
        )

    with ThreadPoolExecutor(max_workers=n * 2) as pool:
        futs = [pool.submit(do_transfer, d % 2, d // 2) for d in range(n * 2)]
        for f in as_completed(futs):
            if f.exception():
                errors.append(f.exception())

    assert errors == [], errors
    bals = {x.account_id: x.balance for x in service.list_accounts("t1")}
    # a -> b 共 100，b -> a 共 100，净额抵消；手续费账户为 0
    assert bals["a"] == 1000
    assert bals["b"] == 1000
    assert bals["_fee_CNY"] == 0
    assert sum(bals.values()) == 2000

    # 事件数 = 建租户1 + 开户2 + 转账200
    events = service.list_events("t1")
    assert len(events) == 203
    # 序号严格连续，无丢失/重复
    assert [e.seq for e in events] == list(range(1, 204))


def test_concurrent_fees_still_conserve(service, two_account_tenant):
    """每笔带 1 手续费：客户余额减少量 == 手续费账户增加量。"""
    n = 60

    def run(idx: int):
        service.transfer(
            tenant_id="t1", idempotency_key=f"fee-{idx}",
            request_id=f"req-fee-{idx}",
            from_account="a", to_account="b", amount=1, fee=1, currency="CNY",
        )

    # 30 笔成功（a 余额 1000 足够，实际上全部 60 笔也够：60*2=120）
    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(run, range(n)))

    accounts = {x.account_id: x.balance for x in service.list_accounts("t1")}
    assert accounts["_fee_CNY"] == n
    assert accounts["a"] + accounts["b"] + accounts["_fee_CNY"] == 1000


def test_concurrent_contention_stops_at_insufficient_funds(
    service
):
    """余额只够部分成功时，成功笔数必须精确，绝不透支。

    自建租户：a=100。每笔 30+0；最多成功 3 笔，其余必须是余额不足。
    """
    service.create_tenant("t1", "租户甲", "key-1", currencies=("CNY",))
    service.open_account("t1", "a", "CNY", initial_balance=100)
    service.open_account("t1", "b", "CNY", initial_balance=0)
    n = 10
    results: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(n)

    def run(idx: int):
        barrier.wait()
        try:
            service.transfer(
                tenant_id="t1", idempotency_key=f"race-{idx}",
                request_id=f"req-race-{idx}",
                from_account="a", to_account="b", amount=30, fee=0,
                currency="CNY",
            )
            with lock:
                results.append("ok")
        except InsufficientFunds:
            with lock:
                results.append("insufficient")

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(run, range(n)))

    assert results.count("ok") == 3
    assert results.count("insufficient") == 7
    assert service.get_balance("t1", "a").balance == 10
    assert service.get_balance("t1", "b").balance == 90
    assert sum(x.balance for x in service.list_accounts("t1")) == 100


def test_concurrent_duplicate_submit_with_same_idempotency_key(
    service, two_account_tenant
):
    """同一笔业务超时重发：N 个并发请求共享一个幂等键，只能入账一次，
    且每个成功响应都指向同一笔 transfer。"""
    n = 20
    barrier = threading.Barrier(n)

    def run(idx: int):
        barrier.wait()
        r = service.transfer(
            tenant_id="t1", idempotency_key="same-key-1",
            request_id=f"req-dup-{idx}",
            from_account="a", to_account="b", amount=77, fee=3, currency="CNY",
        )
        return r

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(run, range(n)))

    transfer_ids = {r.transfer_id for r in results}
    assert len(transfer_ids) == 1  # 全部返回同一笔
    fresh = [r for r in results if not r.replayed]
    replays = [r for r in results if r.replayed]
    assert len(fresh) == 1 and len(replays) == n - 1
    # 余额只变化一次
    bals = {x.account_id: x.balance for x in service.list_accounts("t1")}
    assert bals == {"a": 920, "b": 77, "_fee_CNY": 3}
    committed = [e for e in service.list_events("t1")
                 if e.event_type == "transfer_committed"]
    assert len(committed) == 1


def test_concurrent_same_key_different_body_conflicts(service, two_account_tenant):
    """同键不同体：一个成功，其余不能悄悄变成另一笔转账。"""
    barrier = threading.Barrier(2)

    def run(amount: int):
        barrier.wait()
        try:
            return service.transfer(
                tenant_id="t1", idempotency_key="dup-body",
                request_id=f"req-body-{amount}",
                from_account="a", to_account="b",
                amount=amount, fee=0, currency="CNY",
            )
        except IdempotencyConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outs = list(pool.map(run, [10, 20]))

    assert outs.count("conflict") == 1
    committed = [e for e in service.list_events("t1")
                 if e.event_type == "transfer_committed"]
    assert len(committed) == 1
