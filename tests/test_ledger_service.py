"""领域服务：复式守恒、幂等、余额不足、故障后恢复。"""

from __future__ import annotations

import pytest

from ledger.errors import (
    AccountNotFound,
    IdempotencyConflict,
    InsufficientFunds,
    InvalidAmount,
    SameAccountTransfer,
)


def _balances(svc, tenant="t1", accounts=("a", "b", "_fee_CNY")):
    return {x: svc.get_balance(tenant, x).balance for x in accounts}


def _total(svc, tenant="t1"):
    return sum(a.balance for a in svc.list_accounts(tenant))


def test_double_entry_balances_and_conservation(service, two_account_tenant):
    r = service.transfer(
        tenant_id="t1", idempotency_key="k1", request_id="req-1",
        from_account="a", to_account="b", amount=300, fee=12, currency="CNY",
    )
    # 三条分录：付款 -(300+12)、收款 +300、手续费 +12，合计恒为 0
    assert [(e.role, e.amount) for e in r.entries] == [
        ("payer", -312), ("payee", 300), ("fee", 12),
    ]
    assert sum(e.amount for e in r.entries) == 0
    assert _balances(service) == {"a": 688, "b": 300, "_fee_CNY": 12}
    assert _total(service) == 1000  # 本金+手续费全局守恒
    assert r.replayed is False


def test_zero_fee_transfer(service, two_account_tenant):
    r = service.transfer(
        tenant_id="t1", idempotency_key="k1", request_id="req-1",
        from_account="a", to_account="b", amount=250, fee=0, currency="CNY",
    )
    assert r.fee_balance_after == 0
    assert _total(service) == 1000


def test_insufficient_funds_is_distinct(service, two_account_tenant, caplog):
    with pytest.raises(InsufficientFunds) as ei:
        service.transfer(
            tenant_id="t1", idempotency_key="k-bad", request_id="req-bad",
            from_account="a", to_account="b", amount=1001, fee=0, currency="CNY",
        )
    assert ei.value.code == "insufficient_funds"
    # 余额不足不能产生任何事件，余额不变
    assert _balances(service) == {"a": 1000, "b": 0, "_fee_CNY": 0}
    # 日志必须带请求标识
    assert any("req-bad" in r.message and "insufficient_funds" in r.message
               for r in caplog.records)


def test_insufficient_due_to_fee(service, two_account_tenant):
    # 本金够，但加上手续费不够
    with pytest.raises(InsufficientFunds):
        service.transfer(
            tenant_id="t1", idempotency_key="k-bad2", request_id="req-bad2",
            from_account="a", to_account="b", amount=1000, fee=1, currency="CNY",
        )


def test_other_validation_errors_are_distinct(service, two_account_tenant):
    kw = dict(tenant_id="t1", from_account="a", to_account="b",
              amount=10, fee=0, currency="CNY")
    with pytest.raises(InvalidAmount):
        service.transfer(idempotency_key="k", request_id="r", amount=0,
                         **{k: v for k, v in kw.items() if k != "amount"})
    with pytest.raises(SameAccountTransfer):
        service.transfer(idempotency_key="k2", request_id="r2",
                         from_account="a", to_account="a", amount=10,
                         fee=0, currency="CNY", tenant_id="t1")
    with pytest.raises(AccountNotFound):
        service.transfer(idempotency_key="k3", request_id="r3",
                         from_account="a", to_account="ghost",
                         amount=10, fee=0, currency="CNY", tenant_id="t1")


def test_idempotent_retry_returns_identical_result(service, two_account_tenant, caplog):
    r1 = service.transfer(
        tenant_id="t1", idempotency_key="k1", request_id="req-1",
        from_account="a", to_account="b", amount=100, fee=2, currency="CNY",
    )
    r2 = service.transfer(
        tenant_id="t1", idempotency_key="k1", request_id="req-1-retry",
        from_account="a", to_account="b", amount=100, fee=2, currency="CNY",
    )
    assert r2.replayed is True
    assert r2.transfer_id == r1.transfer_id
    assert r2.event_seq == r1.event_seq
    assert r2.from_balance_after == r1.from_balance_after
    assert r2.to_balance_after == r1.to_balance_after
    assert r2.fee_balance_after == r1.fee_balance_after
    # 只入账一次
    assert _balances(service) == {"a": 898, "b": 100, "_fee_CNY": 2}
    assert any("transfer_idempotent_hit" in r.message and "req-1-retry" in r.message
               for r in caplog.records)


def test_idempotency_conflict_on_different_body(service, two_account_tenant):
    service.transfer(
        tenant_id="t1", idempotency_key="k1", request_id="req-1",
        from_account="a", to_account="b", amount=100, fee=0, currency="CNY",
    )
    with pytest.raises(IdempotencyConflict, match="已用于另一笔"):
        service.transfer(
            tenant_id="t1", idempotency_key="k1", request_id="req-2",
            from_account="a", to_account="b", amount=200, fee=0, currency="CNY",
        )


def test_failed_request_is_also_idempotent(service, two_account_tenant):
    for rid in ("req-fail-1", "req-fail-2"):
        with pytest.raises(InsufficientFunds) as ei:
            service.transfer(
                tenant_id="t1", idempotency_key="k-fail", request_id=rid,
                from_account="a", to_account="b", amount=9999, fee=0,
                currency="CNY",
            )
        assert ei.value.request_id == rid
    # 两次失败均无事件
    committed = [e for e in service.list_events("t1")
                 if e.event_type == "transfer_committed"]
    assert committed == []
