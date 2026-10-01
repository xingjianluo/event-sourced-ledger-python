"""账本错误类型：每种失败都有可区分的原因码。"""

from __future__ import annotations


class LedgerError(Exception):
    """所有账本错误的基类，携带稳定的 reason 码。"""

    reason: str = "ledger_error"

    def __init__(self, message: str = "") -> None:
        super().__init__(message or self.reason)
        self.message = message or self.reason


# ---- 租户与访问控制 ----

class TenantNotFoundError(LedgerError):
    reason = "tenant_not_found"


class CrossTenantReadError(LedgerError):
    reason = "cross_tenant_read"


class CrossTenantWriteError(LedgerError):
    reason = "cross_tenant_write"


# ---- 账户与转账 ----

class AccountNotFoundError(LedgerError):
    reason = "account_not_found"


class AccountAlreadyExistsError(LedgerError):
    reason = "account_already_exists"


class InsufficientFundsError(LedgerError):
    reason = "insufficient_funds"


class InvalidTransferError(LedgerError):
    reason = "invalid_transfer"


# ---- 事件与回放 ----

class DuplicateEventError(LedgerError):
    reason = "duplicate_event"


class OutOfOrderEventError(LedgerError):
    reason = "out_of_order_event"


class CorruptEventError(LedgerError):
    reason = "corrupt_event"


class CheckpointMismatchError(LedgerError):
    reason = "checkpoint_mismatch"
