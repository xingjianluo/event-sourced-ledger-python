"""账本领域模型：账户、记账分录、事件、检查点。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Entry:
    """一条记账分录：某账户的余额变动（正为贷入，负为借出）。"""

    account_id: str
    delta: int
    kind: str  # "principal" | "fee"


@dataclass(frozen=True)
class Event:
    """追加式账目事件。seq 在租户内严格递增。"""

    seq: int
    tenant_id: str
    event_type: str  # "account_opened" | "transfer_recorded"
    payload: dict
    checksum: str = ""


@dataclass(frozen=True)
class TransferResult:
    """一次转账的入账结果，也是幂等重放时返回的原始结果。"""

    transfer_id: str
    tenant_id: str
    entries: tuple[Entry, ...]
    balances: dict[str, int]


@dataclass
class Checkpoint:
    """检查点：截至某 seq 的各租户余额快照，用于确定性重放。"""

    seq: int
    balances: dict[str, dict[str, int]] = field(default_factory=dict)
