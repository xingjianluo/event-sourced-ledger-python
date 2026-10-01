"""账本服务：租户隔离、幂等转账、双记记账、事件回放。

一致性设计：
- 单把互斥锁保护所有状态变更，并发转账不会丢失更新；
- 转账前先校验余额，不足则以 InsufficientFundsError 单独拒绝，不会透支；
- 每笔转账成对记账，手续费单独成录，借贷之和恒为 0（守恒）；
- (tenant_id, transfer_id) 作为幂等键，重复提交直接返回首次结果；
- 所有变动先追加事件再改内存，重启后从检查点+事件日志回放恢复。
"""

from __future__ import annotations

import logging
import threading

from .errors import (
    AccountAlreadyExistsError,
    AccountNotFoundError,
    CheckpointMismatchError,
    CrossTenantReadError,
    CrossTenantWriteError,
    InsufficientFundsError,
    InvalidTransferError,
    TenantNotFoundError,
)
from .models import Checkpoint, Entry, Event, TransferResult
from .store import EventStore, compute_checksum

logger = logging.getLogger("ledger")

FEE_ACCOUNT = "__fees__"


class LedgerService:
    def __init__(self, store: EventStore) -> None:
        self._store = store
        self._lock = threading.Lock()
        # 租户 -> {账户 -> 余额}
        self._balances: dict[str, dict[str, int]] = {}
        # 账户ID -> 所属租户（账户ID全局唯一，用于区分“不存在”与“跨租户”）
        self._account_owner: dict[str, str] = {}
        # (租户, 转账ID) -> 首次结果（幂等表）
        self._idempotency: dict[tuple[str, str], TransferResult] = {}
        self._seq = 0
        self._recover()

    # ---- 启动恢复：检查点 + 事件回放 ----

    def _recover(self) -> None:
        # 全量回放事件日志：既恢复状态，也顺带完成完整性校验
        # （乱序/重复/损坏会在 load_events 阶段以不同错误抛出）。
        events = self._store.load_events()
        for event in events:
            self._apply(event)
        self._seq = events[-1].seq if events else 0
        logger.info("recovered: %d events replayed", len(events))

    # ---- 事件应用（纯函数式，回放与写入共用） ----

    def _apply(self, event: Event) -> None:
        tenant = event.tenant_id
        if event.event_type == "tenant_registered":
            self._balances.setdefault(tenant, {FEE_ACCOUNT: 0})
            self._account_owner.setdefault(f"{tenant}:{FEE_ACCOUNT}", tenant)
            # 手续费账户按租户隔离，不参与跨租户判定
        elif event.event_type == "account_opened":
            p = event.payload
            self._balances[tenant][p["account_id"]] = int(p["balance"])
            self._account_owner[p["account_id"]] = tenant
        elif event.event_type == "transfer_recorded":
            p = event.payload
            for e in p["entries"]:
                acct = e["account_id"]
                self._balances[tenant][acct] = self._balances[tenant].get(acct, 0) + int(e["delta"])
            result = TransferResult(
                transfer_id=p["transfer_id"],
                tenant_id=tenant,
                entries=tuple(
                    Entry(e["account_id"], int(e["delta"]), e["kind"])
                    for e in p["entries"]
                ),
                balances=dict(self._balances[tenant]),
            )
            self._idempotency[(tenant, p["transfer_id"])] = result
        else:  # pragma: no cover - 防御未知事件类型
            raise CheckpointMismatchError(f"unknown event type {event.event_type}")

    def _append_and_apply(self, tenant_id: str, event_type: str, payload: dict) -> Event:
        seq = self._seq + 1
        event = Event(
            seq=seq,
            tenant_id=tenant_id,
            event_type=event_type,
            payload=payload,
            checksum=compute_checksum(tenant_id, seq, event_type, payload),
        )
        self._store.append(event)  # 先落盘
        self._apply(event)         # 再改内存
        self._seq = seq
        return event

    # ---- 租户与账户 ----

    def register_tenant(self, tenant_id: str) -> None:
        with self._lock:
            if tenant_id in self._balances:
                return
            self._append_and_apply(tenant_id, "tenant_registered", {})

    def _require_tenant(self, tenant_id: str) -> None:
        if tenant_id not in self._balances:
            raise TenantNotFoundError(f"tenant {tenant_id!r} not found")

    def open_account(self, tenant_id: str, account_id: str, balance: int = 0) -> None:
        if balance < 0:
            raise InvalidTransferError("initial balance must be >= 0")
        with self._lock:
            self._require_tenant(tenant_id)
            owner = self._account_owner.get(account_id)
            if owner == tenant_id:
                raise AccountAlreadyExistsError(
                    f"account {account_id!r} already exists in tenant {tenant_id!r}"
                )
            if owner is not None:
                raise CrossTenantWriteError(
                    f"account id {account_id!r} already owned by tenant {owner!r}"
                )
            self._append_and_apply(
                tenant_id,
                "account_opened",
                {"account_id": account_id, "balance": balance},
            )
            logger.info(
                "account_opened tenant=%s account=%s balance=%d",
                tenant_id, account_id, balance,
            )

    def get_balance(self, tenant_id: str, account_id: str) -> int:
        with self._lock:
            self._require_tenant(tenant_id)
            if account_id == FEE_ACCOUNT:
                # 手续费账户是租户内置账户，直接按租户读取
                return self._balances[tenant_id][FEE_ACCOUNT]
            owner = self._account_owner.get(account_id)
            if owner is None:
                raise AccountNotFoundError(f"account {account_id!r} not found")
            if owner != tenant_id:
                raise CrossTenantReadError(
                    f"account {account_id!r} belongs to tenant {owner!r}"
                )
            return self._balances[tenant_id][account_id]

    # ---- 转账（双记 + 手续费 + 幂等） ----

    def transfer(
        self,
        tenant_id: str,
        transfer_id: str,
        from_account: str,
        to_account: str,
        amount: int,
        fee: int = 0,
    ) -> TransferResult:
        if amount <= 0:
            raise InvalidTransferError("amount must be > 0")
        if fee < 0:
            raise InvalidTransferError("fee must be >= 0")
        if from_account == to_account:
            raise InvalidTransferError("from and to must differ")
        with self._lock:
            self._require_tenant(tenant_id)

            # 幂等：同一 (租户, 转账ID) 重复提交，直接返回首次结果
            cached = self._idempotency.get((tenant_id, transfer_id))
            if cached is not None:
                logger.info(
                    "idempotent replay transfer_id=%s tenant=%s (duplicate submission)",
                    transfer_id, tenant_id,
                )
                return cached

            # 跨租户写：账户存在但属于别的租户
            for acct in (from_account, to_account):
                owner = self._account_owner.get(acct)
                if owner is None:
                    raise AccountNotFoundError(f"account {acct!r} not found")
                if owner != tenant_id:
                    raise CrossTenantWriteError(
                        f"account {acct!r} belongs to tenant {owner!r}"
                    )

            balances = self._balances[tenant_id]
            total_debit = amount + fee
            if balances[from_account] < total_debit:
                raise InsufficientFundsError(
                    f"account {from_account!r} balance {balances[from_account]} "
                    f"< required {total_debit} (amount={amount} fee={fee})"
                )

            entries: list[Entry] = [
                Entry(from_account, -amount, "principal"),
                Entry(to_account, amount, "principal"),
            ]
            if fee > 0:
                entries.append(Entry(from_account, -fee, "fee"))
                entries.append(Entry(FEE_ACCOUNT, fee, "fee"))

            payload = {
                "transfer_id": transfer_id,
                "from": from_account,
                "to": to_account,
                "amount": amount,
                "fee": fee,
                "entries": [
                    {"account_id": e.account_id, "delta": e.delta, "kind": e.kind}
                    for e in entries
                ],
            }
            self._append_and_apply(tenant_id, "transfer_recorded", payload)
            result = self._idempotency[(tenant_id, transfer_id)]
            logger.info(
                "transfer_recorded transfer_id=%s tenant=%s %s->%s amount=%d fee=%d "
                "entries=%s balances=%s",
                transfer_id, tenant_id, from_account, to_account, amount, fee,
                [(e.account_id, e.delta, e.kind) for e in entries],
                result.balances,
            )
            return result

    # ---- 回放与检查点 ----

    def replay(self, tenant_id: str, from_seq: int = 0) -> dict[str, int]:
        """从检查点（from_seq）之后的事件确定性重建租户余额，并与当前状态对账。

        - from_seq=0：从空账本全量重放；
        - from_seq=检查点 seq：从检查点快照起放；
        - 其他值：没有对应检查点，拒绝（原因可区分）。
        可重复执行：纯函数式重放，不改变任何状态。
        """
        with self._lock:
            self._require_tenant(tenant_id)
            events = self._store.load_events()
            checkpoint = self._store.load_checkpoint()
            if from_seq == 0:
                balances: dict[str, int] = {FEE_ACCOUNT: 0}
            elif from_seq == checkpoint.seq and tenant_id in checkpoint.balances:
                balances = dict(checkpoint.balances[tenant_id])
            else:
                raise CheckpointMismatchError(
                    f"no checkpoint at seq {from_seq} for tenant {tenant_id!r}"
                )
            for event in events:
                if event.seq <= from_seq or event.tenant_id != tenant_id:
                    continue
                if event.event_type == "account_opened":
                    balances[event.payload["account_id"]] = int(event.payload["balance"])
                elif event.event_type == "transfer_recorded":
                    for e in event.payload["entries"]:
                        balances[e["account_id"]] = balances.get(e["account_id"], 0) + int(e["delta"])
            live = dict(self._balances[tenant_id])
            if balances != live:
                raise CheckpointMismatchError(
                    f"replay mismatch for tenant {tenant_id!r}: "
                    f"replayed={balances} live={live}"
                )
            logger.info(
                "replay tenant=%s from_seq=%d events=%d balances=%s",
                tenant_id, from_seq,
                sum(1 for e in events if e.seq > from_seq and e.tenant_id == tenant_id),
                balances,
            )
            return balances

    def save_checkpoint(self) -> Checkpoint:
        """把当前全部租户余额固化为检查点。"""
        with self._lock:
            checkpoint = Checkpoint(
                seq=self._seq,
                balances={t: dict(accts) for t, accts in self._balances.items()},
            )
            self._store.save_checkpoint(checkpoint)
            logger.info("checkpoint saved seq=%d", checkpoint.seq)
            return checkpoint

    def conservation_total(self, tenant_id: str) -> int:
        """租户内全部余额（含手续费账户）之和，用于守恒校验。"""
        with self._lock:
            self._require_tenant(tenant_id)
            return sum(self._balances[tenant_id].values())
