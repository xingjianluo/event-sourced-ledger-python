"""账本领域服务：租户/账户管理、幂等复式转账、事件回放。

一致性策略
----------
1. 每个租户一把可重入锁，单进程内所有"读尾 -> 校验 -> 追加"严格串行，
   并发转账不会丢失更新；余额不足在临界区内判定，杜绝透支。
2. 所有状态变更只通过追加事件完成，内存余额是事件日志的投影；
   进程重启/故障后从事件日志（可配合检查点）确定性重建。
3. 幂等记录与事件分别落盘：提交路径先写事件（fsync）再写幂等记录。
   若在两者之间崩溃，重放时会从 transfer_committed 事件补建幂等记录，
   因此重试仍返回与首次一致的结果。
"""

from __future__ import annotations

import hashlib
import logging
import threading

from .clock import Clock
from .errors import (
    AccountClosed,
    AccountNotFound,
    AlreadyExists,
    CurrencyNotEnabled,
    IdempotencyConflict,
    InsufficientFunds,
    InvalidAmount,
    SameAccountTransfer,
    TenantNotFound,
)
from .events import canonical_payload, make_event, verify_event
from . import errors as _errors
from .models import (
    AccountBalance,
    Checkpoint,
    Entry,
    Event,
    IdempotencyRecord,
    TransferResult,
)
from .storage import Storage

logger = logging.getLogger("ledger")

FEE_ACCOUNT_PREFIX = "_fee_"  # 租户内每币种的手续费账户 id 前缀

_EVENT_TENANT_CREATED = "tenant_created"
_EVENT_ACCOUNT_OPENED = "account_opened"
_EVENT_ACCOUNT_CLOSED = "account_closed"
_EVENT_TRANSFER_COMMITTED = "transfer_committed"


def _fingerprint(
    *,
    from_account: str,
    to_account: str,
    amount: int,
    fee: int,
    currency: str,
) -> str:
    """业务请求的归一化摘要：同键不同请求体 -> 冲突。"""
    body = canonical_payload(
        {
            "from": from_account,
            "to": to_account,
            "amount": amount,
            "fee": fee,
            "currency": currency,
        }
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class _TenantState:
    """租户内存投影。"""

    def __init__(self) -> None:
        self.loaded = False
        self.seq = 0
        self.last_hash = ""
        self.balances: dict[str, int] = {}
        self.currencies: dict[str, str] = {}
        self.status: dict[str, str] = {}
        self.fee_accounts: dict[str, str] = {}

    def fee_account_id(self, currency: str) -> str:
        return f"{FEE_ACCOUNT_PREFIX}{currency}"


class LedgerService:
    def __init__(self, storage: Storage, clock: Clock) -> None:
        self.storage = storage
        self.clock = clock
        self._locks: dict[str, threading.RLock] = {}
        self._states: dict[str, _TenantState] = {}

    # ================= 内部工具 =================

    def _lock_for(self, tenant_id: str) -> threading.RLock:
        # 字典本身的访问用全局锁保护；临界区操作持有的是租户锁。
        if tenant_id not in self._locks:
            self._locks[tenant_id] = threading.RLock()
        return self._locks[tenant_id]

    def _require_tenant(self, tenant_id: str) -> None:
        if not self.storage.registry().exists(tenant_id):
            raise TenantNotFound(f"租户 {tenant_id!r} 不存在或未开户")

    def _state_for(self, tenant_id: str) -> _TenantState:
        if tenant_id not in self._states:
            self._states[tenant_id] = _TenantState()
        return self._states[tenant_id]

    def _ensure_loaded(self, tenant_id: str) -> _TenantState:
        """惰性加载：首次访问租户时从检查点+事件日志重建状态。"""
        self._require_tenant(tenant_id)
        state = self._state_for(tenant_id)
        if state.loaded:
            return state
        with self._lock_for(tenant_id):
            if not state.loaded:
                self._replay_into(tenant_id, state)
        return state

    def _result_from_payload(self, payload: dict, *, event_seq: int, replayed: bool) -> TransferResult:
        entries = [Entry(**e) for e in payload["entries"]]
        return TransferResult(
            transfer_id=payload["transfer_id"],
            request_id=payload["request_id"],
            idempotency_key=payload["idempotency_key"],
            entries=entries,
            from_balance_after=payload["from_balance_after"],
            to_balance_after=payload["to_balance_after"],
            fee_balance_after=payload["fee_balance_after"],
            event_seq=event_seq,
            replayed=replayed,
        )

    def _result_to_response(self, result: TransferResult) -> dict:
        return {
            "transfer_id": result.transfer_id,
            "request_id": result.request_id,
            "idempotency_key": result.idempotency_key,
            "entries": [
                {"account_id": e.account_id, "amount": e.amount,
                 "currency": e.currency, "role": e.role}
                for e in result.entries
            ],
            "from_balance_after": result.from_balance_after,
            "to_balance_after": result.to_balance_after,
            "fee_balance_after": result.fee_balance_after,
            "event_seq": result.event_seq,
        }

    # ================= 事件应用（投影） =================

    def _apply_event(self, tenant_id: str, state: _TenantState, event: Event) -> None:
        """把单条事件应用到内存投影。纯函数式推进，不做 IO。"""
        p = event.payload
        et = event.event_type

        if et == _EVENT_TENANT_CREATED:
            for currency, fee_id in p.get("fee_accounts", {}).items():
                state.fee_accounts[currency] = fee_id
                state.balances.setdefault(fee_id, 0)
                state.currencies.setdefault(fee_id, currency)
                state.status.setdefault(fee_id, "open")

        elif et == _EVENT_ACCOUNT_OPENED:
            aid = p["account_id"]
            if aid in state.balances:
                raise AlreadyExists(f"账户 {aid!r} 已存在（事件 {event.seq}）")
            state.balances[aid] = int(p.get("initial_balance", 0))
            state.currencies[aid] = p["currency"]
            state.status[aid] = "open"

        elif et == _EVENT_ACCOUNT_CLOSED:
            aid = p["account_id"]
            if aid not in state.balances:
                raise AccountNotFound(f"关户事件引用了不存在的账户 {aid!r}")
            state.status[aid] = "closed"

        elif et == _EVENT_TRANSFER_COMMITTED:
            total = 0
            for raw_entry in p["entries"]:
                aid = raw_entry["account_id"]
                amount = int(raw_entry["amount"])
                if aid not in state.balances:
                    raise AccountNotFound(
                        f"转账事件 {event.seq} 引用了不存在的账户 {aid!r}"
                    )
                state.balances[aid] += amount
                total += amount
            if total != 0:
                raise InvalidAmount(
                    f"转账事件 {event.seq} 分录金额之和为 {total}，违反复式记账守恒"
                )
            # 回放中顺便校验：客户账户余额不应为负
            if state.balances[p["from_account"]] < 0:
                raise InsufficientFunds(
                    f"重放事件 {event.seq} 后付款账户余额为负，日志不一致"
                )
        else:
            raise ValueError(f"未知事件类型: {et!r}（事件 {event.seq}）")

        state.seq = event.seq
        state.last_hash = event.event_hash

    # ================= 开户与租户 =================

    def create_tenant(
        self,
        tenant_id: str,
        name: str,
        api_key: str,
        *,
        currencies: tuple[str, ...] = ("CNY",),
        request_id: str | None = None,
    ) -> dict:
        reg = self.storage.registry()
        if reg.exists(tenant_id):
            raise AlreadyExists(f"租户 {tenant_id!r} 已存在")
        with self._lock_for(tenant_id):
            now = self.clock.now_iso()
            state = self._state_for(tenant_id)
            fee_accounts = {c: state.fee_account_id(c) for c in currencies}

            event = make_event(
                seq=state.seq + 1,
                event_type=_EVENT_TENANT_CREATED,
                tenant_id=tenant_id,
                payload={
                    "name": name,
                    "currencies": list(currencies),
                    "fee_accounts": fee_accounts,
                },
                prev_hash=state.last_hash,
                timestamp=now,
                request_id=request_id,
            )
            # 注册表先行：注册表是租户存在的权威依据，事件紧随其后。
            reg.register(tenant_id, name, api_key, now)
            self.storage.tenant(tenant_id).append_event(event)
            self._apply_event(tenant_id, state, event)
            state.loaded = True

        logger.info(
            "tenant_created request_id=%s tenant=%s currencies=%s event_seq=%d",
            request_id, tenant_id, ",".join(currencies), event.seq,
        )
        return {"tenant_id": tenant_id, "name": name,
                "currencies": list(currencies), "fee_accounts": fee_accounts}

    def open_account(
        self,
        tenant_id: str,
        account_id: str,
        currency: str,
        *,
        initial_balance: int = 0,
        request_id: str | None = None,
    ) -> dict:
        if initial_balance < 0:
            raise InvalidAmount("开户初始余额不能为负")
        state = self._ensure_loaded(tenant_id)
        if currency not in state.fee_accounts:
            raise CurrencyNotEnabled(f"租户 {tenant_id!r} 未启用币种 {currency!r}")
        with self._lock_for(tenant_id):
            if account_id in state.balances:
                raise AlreadyExists(f"账户 {account_id!r} 已存在")
            event = make_event(
                seq=state.seq + 1,
                event_type=_EVENT_ACCOUNT_OPENED,
                tenant_id=tenant_id,
                payload={
                    "account_id": account_id,
                    "currency": currency,
                    "initial_balance": initial_balance,
                },
                prev_hash=state.last_hash,
                timestamp=self.clock.now_iso(),
                request_id=request_id,
            )
            self.storage.tenant(tenant_id).append_event(event)
            self._apply_event(tenant_id, state, event)
        logger.info(
            "account_opened request_id=%s tenant=%s account=%s currency=%s "
            "initial=%d event_seq=%d",
            request_id, tenant_id, account_id, currency,
            initial_balance, event.seq,
        )
        return {"tenant_id": tenant_id, "account_id": account_id,
                "currency": currency, "balance": initial_balance}

    def close_account(self, tenant_id: str, account_id: str, *,
                      request_id: str | None = None) -> dict:
        state = self._ensure_loaded(tenant_id)
        with self._lock_for(tenant_id):
            if account_id not in state.balances:
                raise AccountNotFound(f"账户 {account_id!r} 不存在")
            if state.status[account_id] == "closed":
                raise AccountClosed(f"账户 {account_id!r} 已关闭")
            event = make_event(
                seq=state.seq + 1,
                event_type=_EVENT_ACCOUNT_CLOSED,
                tenant_id=tenant_id,
                payload={"account_id": account_id},
                prev_hash=state.last_hash,
                timestamp=self.clock.now_iso(),
                request_id=request_id,
            )
            self.storage.tenant(tenant_id).append_event(event)
            self._apply_event(tenant_id, state, event)
        logger.info("account_closed request_id=%s tenant=%s account=%s event_seq=%d",
                    request_id, tenant_id, account_id, event.seq)
        return {"tenant_id": tenant_id, "account_id": account_id, "status": "closed"}

    # ================= 查询 =================

    def get_balance(self, tenant_id: str, account_id: str) -> AccountBalance:
        state = self._ensure_loaded(tenant_id)
        with self._lock_for(tenant_id):
            if account_id not in state.balances:
                raise AccountNotFound(f"账户 {account_id!r} 不存在")
            return AccountBalance(
                tenant_id=tenant_id,
                account_id=account_id,
                currency=state.currencies[account_id],
                status=state.status[account_id],
                balance=state.balances[account_id],
                version=state.seq,
            )

    def list_accounts(self, tenant_id: str) -> list[AccountBalance]:
        state = self._ensure_loaded(tenant_id)
        with self._lock_for(tenant_id):
            return [
                AccountBalance(
                    tenant_id=tenant_id,
                    account_id=aid,
                    currency=state.currencies[aid],
                    status=state.status[aid],
                    balance=state.balances[aid],
                    version=state.seq,
                )
                for aid in sorted(state.balances)
            ]

    # ================= 转账（幂等 + 复式） =================

    def transfer(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
        request_id: str,
        from_account: str,
        to_account: str,
        amount: int,
        fee: int,
        currency: str,
    ) -> TransferResult:
        state = self._ensure_loaded(tenant_id)
        fp = _fingerprint(
            from_account=from_account, to_account=to_account,
            amount=amount, fee=fee, currency=currency,
        )
        store = self.storage.tenant(tenant_id)

        with self._lock_for(tenant_id):
            # 1) 幂等命中：同键必须同体，且返回与首次完全一致的结果。
            existing = store.load_idempotency(idempotency_key)
            if existing is not None:
                if existing.fingerprint != fp:
                    raise IdempotencyConflict(
                        f"幂等键 {idempotency_key!r} 已用于另一笔业务请求，"
                        f"首次请求 {existing.request_id!r}",
                        request_id=request_id,
                    )
                if existing.outcome == "ok":
                    payload = existing.response["transfer"]
                    result = self._result_from_payload(
                        payload, event_seq=existing.event_seq or 0, replayed=True
                    )
                    logger.info(
                        "transfer_idempotent_hit request_id=%s original_request=%s "
                        "idem_key=%s transfer=%s event_seq=%s",
                        request_id, existing.request_id, idempotency_key,
                        result.transfer_id, result.event_seq,
                    )
                    return result
                # 首次失败：回放完全相同的业务错误
                err = existing.response["error"]
                logger.info(
                    "transfer_idempotent_error_replay request_id=%s "
                    "original_request=%s idem_key=%s code=%s",
                    request_id, existing.request_id, idempotency_key, err["code"],
                )
                self._raise_saved_error(err, request_id=request_id)

            # 2) 业务校验（错误同样要被幂等记录，保证重试结果一致）。
            try:
                self._validate_transfer(
                    state,
                    from_account=from_account, to_account=to_account,
                    amount=amount, fee=fee, currency=currency,
                )
            except Exception as exc:
                self._save_error_outcome(
                    store, tenant_id=tenant_id, key=idempotency_key, fp=fp,
                    request_id=request_id, exc=exc,
                )
                if isinstance(exc, _errors.LedgerError):
                    exc.request_id = request_id
                raise

            # 3) 提交：构造复式分录 -> 追加事件 -> 落幂等记录。
            fee_id = state.fee_accounts[currency]
            entries = [
                Entry(account_id=from_account, amount=-(amount + fee),
                      currency=currency, role="payer"),
                Entry(account_id=to_account, amount=amount,
                      currency=currency, role="payee"),
                Entry(account_id=fee_id, amount=fee,
                      currency=currency, role="fee"),
            ]
            payload = {
                "transfer_id": _new_transfer_id(),
                "request_id": request_id,
                "idempotency_key": idempotency_key,
                "from_account": from_account,
                "to_account": to_account,
                "amount": amount,
                "fee": fee,
                "currency": currency,
                "entries": [
                    {"account_id": e.account_id, "amount": e.amount,
                     "currency": e.currency, "role": e.role}
                    for e in entries
                ],
                # 余额快照随事件留痕，回放时可逐笔核对。
                "from_balance_after": state.balances[from_account] - amount - fee,
                "to_balance_after": state.balances[to_account] + amount,
                "fee_balance_after": state.balances[fee_id] + fee,
            }
            event = make_event(
                seq=state.seq + 1,
                event_type=_EVENT_TRANSFER_COMMITTED,
                tenant_id=tenant_id,
                payload=payload,
                prev_hash=state.last_hash,
                timestamp=self.clock.now_iso(),
                request_id=request_id,
                idempotency_key=idempotency_key,
            )
            store.append_event(event)  # 事件先落盘（fsync）
            self._apply_event(tenant_id, state, event)

            result = self._result_from_payload(payload, event_seq=event.seq,
                                               replayed=False)
            store.save_idempotency(
                IdempotencyRecord(
                    tenant_id=tenant_id,
                    idempotency_key=idempotency_key,
                    fingerprint=fp,
                    request_id=request_id,
                    outcome="ok",
                    response={"transfer": self._result_to_response(result)},
                    event_seq=event.seq,
                    created_at=self.clock.now_iso(),
                )
            )
            logger.info(
                "transfer_committed request_id=%s tenant=%s transfer=%s "
                "idem_key=%s %s->%s amount=%d fee=%d event_seq=%d "
                "balances after: from=%d to=%d fee=%d",
                request_id, tenant_id, result.transfer_id, idempotency_key,
                from_account, to_account, amount, fee, event.seq,
                result.from_balance_after, result.to_balance_after,
                result.fee_balance_after,
            )
            return result

    def _validate_transfer(
        self, state: _TenantState, *,
        from_account: str, to_account: str,
        amount: int, fee: int, currency: str,
    ) -> None:
        if amount <= 0:
            raise InvalidAmount(f"转账金额必须为正整数，收到 {amount}")
        if fee < 0:
            raise InvalidAmount(f"手续费不能为负，收到 {fee}")
        if from_account == to_account:
            raise SameAccountTransfer("付款账户与收款账户不能相同")
        if currency not in state.fee_accounts:
            raise CurrencyNotEnabled(f"租户未启用币种 {currency!r}")
        if from_account not in state.balances:
            raise AccountNotFound(f"付款账户 {from_account!r} 不存在")
        if to_account not in state.balances:
            raise AccountNotFound(f"收款账户 {to_account!r} 不存在")
        if state.currencies[from_account] != currency:
            raise InvalidAmount(
                f"付款账户币种 {state.currencies[from_account]!r} 与 {currency!r} 不一致"
            )
        if state.currencies[to_account] != currency:
            raise InvalidAmount(
                f"收款账户币种 {state.currencies[to_account]!r} 与 {currency!r} 不一致"
            )
        if state.status[from_account] != "open":
            raise AccountClosed(f"付款账户 {from_account!r} 已关闭")
        if state.status[to_account] != "open":
            raise AccountClosed(f"收款账户 {to_account!r} 已关闭")
        # 余额不足单独识别：付款方需要覆盖本金+手续费。
        if state.balances[from_account] < amount + fee:
            raise InsufficientFunds(
                f"账户 {from_account!r} 余额 {state.balances[from_account]} "
                f"不足以支付 {amount + fee}（本金 {amount} + 手续费 {fee}）"
            )

    def _save_error_outcome(self, store, *, tenant_id: str, key: str, fp: str,
                            request_id: str, exc: Exception) -> None:
        from .errors import LedgerError

        if isinstance(exc, LedgerError):
            err = {"code": exc.code, "message": exc.message}
        else:
            err = {"code": "internal_error", "message": str(exc)}
        logger.info(
            "transfer_rejected request_id=%s tenant=%s idem_key=%s code=%s msg=%s",
            request_id, tenant_id, key, err["code"], err["message"],
        )
        store.save_idempotency(
            IdempotencyRecord(
                tenant_id=tenant_id,
                idempotency_key=key,
                fingerprint=fp,
                request_id=request_id,
                outcome="error",
                response={"error": err},
                event_seq=None,
                created_at=self.clock.now_iso(),
            )
        )

    def _raise_saved_error(self, err: dict, *, request_id: str) -> None:
        from .errors import (
            AccountClosed as _AC, AccountNotFound as _ANF,
            CurrencyNotEnabled as _CNE, IdempotencyConflict as _IC,
            InsufficientFunds as _IF, InvalidAmount as _IA,
            SameAccountTransfer as _SA,
        )

        mapping = {
            "insufficient_funds": _IF,
            "account_not_found": _ANF,
            "account_closed": _AC,
            "same_account": _SA,
            "invalid_amount": _IA,
            "currency_not_enabled": _CNE,
            "idempotency_conflict": _IC,
        }
        cls = mapping.get(err["code"], InvalidAmount)
        raise cls(err["message"], request_id=request_id)

    # ================= 事件与回放 =================

    def list_events(self, tenant_id: str) -> list[Event]:
        self._require_tenant(tenant_id)
        return self.storage.tenant(tenant_id).read_events()

    def _replay_into(self, tenant_id: str, state: _TenantState,
                     *, verbose: bool = False,
                     use_checkpoint: bool = True) -> dict:
        """从检查点（若有）续读事件日志，确定性重建 state。

        乱序/重复/损坏事件在此被 verify_event 拦截并给出可区分原因。
        use_checkpoint=False 时忽略磁盘检查点，强制从第 1 条事件全量重放。
        """
        store = self.storage.tenant(tenant_id)
        replayed = 0
        checkpoint = store.load_checkpoint() if use_checkpoint else None
        if checkpoint is not None:
            # 检查点必须和日志在该 seq 的事件哈希吻合，否则是脏检查点。
            events = store.read_events()
            if checkpoint.seq > len(events):
                from .errors import EventCorrupted
                raise EventCorrupted(
                    f"检查点(seq={checkpoint.seq})超出事件日志长度 {len(events)}，"
                    "日志被截断或检查点失效"
                )
            if checkpoint.seq > 0:
                anchor = events[checkpoint.seq - 1]
                if anchor.event_hash != checkpoint.last_hash or anchor.seq != checkpoint.seq:
                    from .errors import EventCorrupted
                    raise EventCorrupted(
                        f"检查点(seq={checkpoint.seq})与事件日志锚点不一致，"
                        "检查点已失效"
                    )
            state.balances = dict(checkpoint.balances)
            state.currencies = dict(checkpoint.currencies)
            state.status = dict(checkpoint.status)
            state.fee_accounts = dict(checkpoint.fee_accounts)
            state.seq = checkpoint.seq
            state.last_hash = checkpoint.last_hash
            tail = events[checkpoint.seq:]
        else:
            tail = store.read_events()

        for event in tail:
            verify_event(
                event,
                expected_seq=state.seq + 1,
                prev_hash=state.last_hash,
            )
            self._apply_event(tenant_id, state, event)
            replayed += 1
            # 崩溃恢复补洞：幂等记录缺失（事件已提交但记录未及落盘），
            # 从事件重建，保证后续重试拿到一致结果。
            if event.event_type == _EVENT_TRANSFER_COMMITTED:
                key = event.idempotency_key
                if key and store.load_idempotency(key) is None:
                    payload = event.payload
                    result = self._result_from_payload(payload,
                                                       event_seq=event.seq,
                                                       replayed=True)
                    fp = _fingerprint(
                        from_account=payload["from_account"],
                        to_account=payload["to_account"],
                        amount=payload["amount"],
                        fee=payload["fee"],
                        currency=payload["currency"],
                    )
                    store.save_idempotency(
                        IdempotencyRecord(
                            tenant_id=tenant_id,
                            idempotency_key=key,
                            fingerprint=fp,
                            request_id=payload["request_id"],
                            outcome="ok",
                            response={"transfer": self._result_to_response(result)},
                            event_seq=event.seq,
                            created_at=event.timestamp,
                        )
                    )
                    if verbose:
                        logger.info(
                            "replay_repair_idempotency tenant=%s idem_key=%s "
                            "transfer=%s event_seq=%d",
                            tenant_id, key, payload["transfer_id"], event.seq,
                        )
            if verbose:
                logger.info(
                    "replay_apply tenant=%s seq=%d type=%s request_id=%s",
                    tenant_id, event.seq, event.event_type, event.request_id,
                )

        state.loaded = True
        return {
            "tenant_id": tenant_id,
            "checkpoint_seq": checkpoint.seq if checkpoint else 0,
            "events_replayed": replayed,
            "head_seq": state.seq,
            "head_hash": state.last_hash,
            "balances": dict(state.balances),
        }

    def replay(self, tenant_id: str, *, from_checkpoint: bool = True) -> dict:
        """对外的重放入口：在租户锁内用全新投影重放，结果替换内存状态。

        确定性、可重复执行：同样的事件日志必然得到同样的余额。
        """
        self._require_tenant(tenant_id)
        with self._lock_for(tenant_id):
            fresh = _TenantState()
            summary = self._replay_into(
                tenant_id, fresh, verbose=True, use_checkpoint=from_checkpoint
            )
            self._states[tenant_id] = fresh
            return summary

    def save_checkpoint(self, tenant_id: str) -> Checkpoint:
        state = self._ensure_loaded(tenant_id)
        with self._lock_for(tenant_id):
            cp = Checkpoint(
                tenant_id=tenant_id,
                seq=state.seq,
                last_hash=state.last_hash,
                balances=dict(state.balances),
                currencies=dict(state.currencies),
                status=dict(state.status),
                fee_accounts=dict(state.fee_accounts),
                created_at=self.clock.now_iso(),
            )
            self.storage.tenant(tenant_id).save_checkpoint(cp)
        logger.info("checkpoint_saved tenant=%s seq=%d", tenant_id, state.seq)
        return cp

    def verify_log(self, tenant_id: str) -> dict:
        """只校验事件日志完整性（哈希链/序号），不改动内存状态。"""
        self._require_tenant(tenant_id)
        events = self.storage.tenant(tenant_id).read_events()
        expected_seq = 1
        prev_hash = ""
        for event in events:
            verify_event(event, expected_seq=expected_seq, prev_hash=prev_hash)
            expected_seq += 1
            prev_hash = event.event_hash
        return {"tenant_id": tenant_id, "events": len(events),
                "head_seq": expected_seq - 1, "head_hash": prev_hash,
                "intact": True}


# 延迟导入，避免 models -> ids 的循环观感问题。
def _new_transfer_id() -> str:
    from .ids import new_transfer_id
    return new_transfer_id()
