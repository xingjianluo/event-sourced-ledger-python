"""账本服务错误类型。

每个错误都带一个稳定的机器可读 code，方便调用方区分失败原因，
也保证重复请求拿到一致的结果语义。
"""

from __future__ import annotations


class LedgerError(Exception):
    """所有账本错误的基类。"""

    code: str = "ledger_error"

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.request_id = request_id

    def to_dict(self) -> dict[str, str | None]:
        return {"code": self.code, "message": self.message, "request_id": self.request_id}


class AuthError(LedgerError):
    """缺少身份凭据。"""

    code = "unauthenticated"


class TenantForbidden(LedgerError):
    """已认证租户无权访问其他租户的资源。"""

    code = "tenant_forbidden"


class TenantNotFound(LedgerError):
    """目标租户不存在（未开户）。"""

    code = "tenant_not_found"


class AccountNotFound(LedgerError):
    """账户不存在。"""

    code = "account_not_found"


class AccountClosed(LedgerError):
    """账户已关闭，不允许再记账。"""

    code = "account_closed"


class InsufficientFunds(LedgerError):
    """账户余额不足，无法完成转账。"""

    code = "insufficient_funds"


class SameAccountTransfer(LedgerError):
    """付款方与收款方相同。"""

    code = "same_account"


class InvalidAmount(LedgerError):
    """金额或手续费非法（<=0 等）。"""

    code = "invalid_amount"


class IdempotencyConflict(LedgerError):
    """同一个幂等键被用于不同的业务请求。"""

    code = "idempotency_conflict"


class AlreadyExists(LedgerError):
    """租户或账户已存在。"""

    code = "already_exists"


class CurrencyNotEnabled(LedgerError):
    """租户未启用该币种（无对应手续费账户）。"""

    code = "currency_not_enabled"


class EventValidationError(LedgerError):
    """事件流校验失败基类。"""

    code = "event_validation_error"


class EventOrderError(EventValidationError):
    """事件乱序（序号或哈希链不连续）。"""

    code = "event_out_of_order"


class EventDuplicateError(EventValidationError):
    """事件重复（同一事件编号/幂等键重复出现）。"""

    code = "event_duplicate"


class EventCorrupted(EventValidationError):
    """事件载荷损坏（校验和不匹配）。"""

    code = "event_corrupted"
