"""账本服务测试：并发、幂等、余额不足、回放、事件完整性。"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor

import pytest

from ledger.errors import (
    AccountNotFoundError,
    CorruptEventError,
    CrossTenantReadError,
    CrossTenantWriteError,
    DuplicateEventError,
    InsufficientFundsError,
    OutOfOrderEventError,
    TenantNotFoundError,
)
from ledger.service import FEE_ACCOUNT, LedgerService
from ledger.store import EventStore, compute_checksum

logger = logging.getLogger("test.ledger")


@pytest.fixture()
def svc(tmp_path):
    service = LedgerService(EventStore(tmp_path / "data"))
    service.register_tenant("t1")
    service.register_tenant("t2")
    return service


def test_cross_tenant_read_and_write_have_distinct_reasons(svc):
    svc.open_account("t1", "alice", 100)
    svc.open_account("t2", "bob", 50)

    with pytest.raises(CrossTenantReadError) as exc:
        svc.get_balance("t2", "alice")  # t2 读 t1 的账户
    assert exc.value.reason == "cross_tenant_read"

    with pytest.raises(CrossTenantWriteError) as exc:
        svc.transfer("t2", "x1", "alice", "bob", 10)  # t2 动用 t1 的账户
    assert exc.value.reason == "cross_tenant_write"

    with pytest.raises(AccountNotFoundError) as exc:
        svc.get_balance("t1", "nobody")  # 不存在的账户是另一种原因
    assert exc.value.reason == "account_not_found"

    with pytest.raises(TenantNotFoundError) as exc:
        svc.get_balance("ghost", "alice")
    assert exc.value.reason == "tenant_not_found"


def test_idempotent_duplicate_submission_returns_same_result(svc):
    svc.open_account("t1", "a", 100)
    svc.open_account("t1", "b", 0)
    first = svc.transfer("t1", "req-1", "a", "b", 30, fee=2)
    logger.info("request=req-1 first entries=%s balances=%s",
              [(e.account_id, e.delta, e.kind) for e in first.entries], first.balances)
    # 超时重发：同一幂等键，必须返回完全一致的结果且不得重复入账
    for _ in range(3):
        again = svc.transfer("t1", "req-1", "a", "b", 30, fee=2)
        assert again == first
    assert svc.get_balance("t1", "a") == 68
    assert svc.get_balance("t1", "b") == 30
    assert svc.get_balance("t1", FEE_ACCOUNT) == 2


def test_insufficient_funds_is_distinct_and_no_overdraft(svc):
    svc.open_account("t1", "a", 50)
    svc.open_account("t1", "b", 0)
    with pytest.raises(InsufficientFundsError) as exc:
        svc.transfer("t1", "req-2", "a", "b", 40, fee=20)  # 40+20 > 50
    assert exc.value.reason == "insufficient_funds"
    logger.info("request=req-2 rejected reason=%s", exc.value.reason)
    assert svc.get_balance("t1", "a") == 50  # 未发生任何扣款
    assert svc.get_balance("t1", "b") == 0


def test_concurrent_duplicate_transfers_conserve_and_idempotent(svc):
    svc.open_account("t1", "a", 1000)
    svc.open_account("t1", "b", 0)
    total_before = svc.conservation_total("t1")

    # 20 个线程同时提交同一笔转账（模拟超时重发风暴）
    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(
            lambda _: svc.transfer("t1", "req-dup", "a", "b", 10, fee=1),
            range(20),
        ))
    assert all(r == results[0] for r in results)  # 全部返回同一结果
    assert svc.get_balance("t1", "a") == 989  # 只入账一次
    assert svc.get_balance("t1", "b") == 10
    assert svc.get_balance("t1", FEE_ACCOUNT) == 1

    # 50 个线程各转不同笔，部分余额不足：成功数必须精确，无丢失更新
    def attempt(i: int):
        try:
            svc.transfer("t1", f"req-{i}", "a", "b", 20, fee=1)
            return "ok"
        except InsufficientFundsError:
            return "insufficient"

    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = list(pool.map(attempt, range(50)))
    ok = outcomes.count("ok")
    # 989 可用，每笔扣 21：989 // 21 = 47 笔成功
    assert ok == 47
    assert outcomes.count("insufficient") == 3
    assert svc.get_balance("t1", "a") == 989 - 47 * 21
    assert svc.get_balance("t1", "b") == 10 + 47 * 20
    assert svc.get_balance("t1", FEE_ACCOUNT) == 1 + 47 * 1
    # 守恒：任何时刻各方金额 + 手续费账户之和不变
    assert svc.conservation_total("t1") == total_before
    logger.info("concurrent done ok=%d insufficient=%d balances a=%d b=%d fees=%d",
                ok, outcomes.count("insufficient"),
                svc.get_balance("t1", "a"), svc.get_balance("t1", "b"),
                svc.get_balance("t1", FEE_ACCOUNT))


def test_replay_is_deterministic_and_repeatable(svc, tmp_path):
    svc.open_account("t1", "a", 100)
    svc.open_account("t1", "b", 0)
    svc.transfer("t1", "r1", "a", "b", 30, fee=3)
    cp = svc.save_checkpoint()
    svc.transfer("t1", "r2", "a", "b", 20, fee=2)

    full1 = svc.replay("t1", from_seq=0)
    full2 = svc.replay("t1", from_seq=0)
    assert full1 == full2  # 确定性、可重复
    from_cp = svc.replay("t1", from_seq=cp.seq)
    assert from_cp == full1  # 检查点起放与全量重放一致
    assert full1["a"] == 45 and full1["b"] == 50 and full1[FEE_ACCOUNT] == 5
    logger.info("replay from_seq=0 %s from_seq=%d %s", full1, cp.seq, from_cp)

    # 故障恢复：用同一份日志重建服务，状态必须一致
    recovered = LedgerService(EventStore(tmp_path / "data"))
    assert recovered.get_balance("t1", "a") == 45
    assert recovered.get_balance("t1", "b") == 50
    # 幂等表也被恢复：重启后重发仍返回原结果
    assert recovered.transfer("t1", "r2", "a", "b", 20, fee=2).transfer_id == "r2"
    assert recovered.get_balance("t1", "a") == 45


def _append_raw(store: EventStore, record: dict) -> None:
    with open(store._log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def test_corrupt_duplicate_out_of_order_events_are_distinguished(svc, tmp_path):
    svc.open_account("t1", "a", 100)
    store = EventStore(tmp_path / "data")
    good = store.load_events()

    # 乱序：seq 跳跃
    bad = dict(good[-1].__dict__)
    bad["seq"] = 99
    bad["checksum"] = compute_checksum(bad["tenant_id"], 99, bad["event_type"], bad["payload"])
    _append_raw(store, {**bad, "payload": bad["payload"]})
    with pytest.raises(OutOfOrderEventError) as exc:
        store.load_events()
    assert exc.value.reason == "out_of_order_event"

    # 重复：同一 seq 再出现
    store2 = EventStore(tmp_path / "d2")
    svc2 = LedgerService(store2)
    svc2.register_tenant("t1")
    ev = store2.load_events()[-1]
    _append_raw(store2, {**ev.__dict__})
    with pytest.raises(DuplicateEventError) as exc:
        store2.load_events()
    assert exc.value.reason == "duplicate_event"

    # 损坏：校验和不匹配
    store3 = EventStore(tmp_path / "d3")
    svc3 = LedgerService(store3)
    svc3.register_tenant("t1")
    ev = store3.load_events()[-1]
    _append_raw(store3, {**ev.__dict__, "seq": 2, "checksum": "0" * 64})
    with pytest.raises(CorruptEventError) as exc:
        store3.load_events()
    assert exc.value.reason == "corrupt_event"
