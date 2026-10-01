"""账本领域模型（仅类型定义）。

金额一律使用整数最小单位（如"分"），避免浮点误差。
复式记账中 entry 的 amount 带符号：
  正数 = 贷记（客户存款账户表现为余额增加）
  负数 = 借记（客户存款账户表现为余额减少）
一笔转账的所有分录金额之和恒为 0。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Money:
    amount: int
    currency: str


@dataclass(slots=True)
class Tenant:
    tenant_id: str
    name: str
    api_key: str
    created_at: str


@dataclass(slots=True)
class Account:
    tenant_id: str
    account_id: str
    currency: str
    status: str  # "open" | "closed"
    opened_at: str


@dataclass(frozen=True, slots=True)
class Entry:
    """一条复式分录。"""

    account_id: str
    amount: int  # 带符号
    currency: str
    role: str  # "payer" | "payee" | "fee"


@dataclass(slots=True)
class TransferRequest:
    request_id: str
    tenant_id: str
    idempotency_key: str
    from_account: str
    to_account: str
    amount: int
    fee: int
    currency: str


@dataclass(slots=True)
class TransferResult:
    transfer_id: str
    request_id: str
    idempotency_key: str
    entries: list[Entry]
    from_balance_after: int
    to_balance_after: int
    fee_balance_after: int
    event_seq: int
    replayed: bool  # True 表示由幂等记录重放返回，而非新入账


@dataclass(slots=True)
class Event:
    """追加式事件信封。

    event_hash = sha256(prev_hash + seq + event_id + canonical(payload) ...)
    形成租户内的哈希链，用于检测乱序、重复与损坏。
    """

    seq: int
    event_id: str
    event_type: str
    tenant_id: str
    request_id: str | None
    idempotency_key: str | None
    timestamp: str
    payload: dict
    prev_hash: str
    event_hash: str


@dataclass(slots=True)
class IdempotencyRecord:
    tenant_id: str
    idempotency_key: str
    fingerprint: str  # 归一化业务请求的摘要
    request_id: str
    outcome: str  # "ok" | "error"
    response: dict  # 首次请求的完整结果（成功结果或错误）
    event_seq: int | None
    created_at: str


@dataclass(slots=True)
class AccountBalance:
    tenant_id: str
    account_id: str
    currency: str
    status: str
    balance: int
    version: int  # 已应用到的事件 seq


@dataclass(slots=True)
class Checkpoint:
    """重放检查点：某租户在某个 seq 的完整状态快照。"""

    tenant_id: str
    seq: int
    last_hash: str
    balances: dict[str, int] = field(default_factory=dict)
    currencies: dict[str, str] = field(default_factory=dict)
    status: dict[str, str] = field(default_factory=dict)
    fee_accounts: dict[str, str] = field(default_factory=dict)  # currency -> account_id
    created_at: str = ""
