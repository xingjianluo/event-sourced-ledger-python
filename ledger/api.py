"""FastAPI HTTP 接口。

鉴权约定
--------
  X-API-Key      业务租户凭据（必填）
  X-Tenant-Id    显式声明访问的租户（必填，必须与 API Key 所属租户一致）
  X-Request-Id   调用方请求标识；缺省时服务端生成。日志全程携带。
  Idempotency-Key 转账幂等键（转账接口必填）
  X-Admin-Key    管理接口凭据（建租户、触发回放）

拒绝原因是可区分的：
  401 unauthenticated       —— 缺少/无效凭据或租户头
  403 tenant_forbidden      —— 凭据有效但试图访问其他租户
  404 tenant_not_found/...  —— 资源不存在
  409 idempotency_conflict  —— 幂等键复用但请求体不同
  422 insufficient_funds    —— 余额不足（独立于其他校验错误）
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .clock import SystemClock
from .errors import AuthError, InvalidAmount, LedgerError, TenantForbidden
from .ids import new_request_id
from .service import LedgerService
from .storage import Storage

logger = logging.getLogger("ledger")

# 业务错误 -> HTTP 状态码
_STATUS_MAP: dict[str, int] = {
    "unauthenticated": 401,
    "tenant_forbidden": 403,
    "tenant_not_found": 404,
    "account_not_found": 404,
    "already_exists": 409,
    "account_closed": 409,
    "idempotency_conflict": 409,
    "same_account": 422,
    "invalid_amount": 422,
    "currency_not_enabled": 422,
    "insufficient_funds": 422,
    "event_out_of_order": 422,
    "event_duplicate": 422,
    "event_corrupted": 422,
}


# ================= 请求/响应模型 =================

class CreateTenantBody(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64)
    name: str
    api_key: str = Field(min_length=8)
    currencies: list[str] = Field(default_factory=lambda: ["CNY"])


class OpenAccountBody(BaseModel):
    account_id: str = Field(min_length=1, max_length=64)
    currency: str = "CNY"
    initial_balance: int = 0


class TransferBody(BaseModel):
    from_account: str
    to_account: str
    amount: int
    fee: int = 0
    currency: str = "CNY"


# ================= 鉴权 =================

@dataclass(slots=True)
class Principal:
    tenant_id: str
    request_id: str


def _authenticate(
    request: Request,
    *,
    x_api_key: str | None,
    x_tenant_id: str | None,
    x_request_id: str | None,
) -> Principal:
    """解析并校验调用方身份，失败原因按类别区分。"""
    request_id = x_request_id or new_request_id()
    request.state.request_id = request_id

    path_tenant = request.path_params.get("tenant_id")

    if not x_api_key:
        raise AuthError("缺少凭据：请求头 X-API-Key 必填", request_id=request_id)
    resolved = request.app.state.service.storage.registry().resolve_api_key(x_api_key)
    if resolved is None:
        raise AuthError("无效的 API Key", request_id=request_id)

    # 路径参数是租户的权威声明；X-Tenant-Id 若提供则必须与之一致。
    target = path_tenant or x_tenant_id
    if target is None:
        raise AuthError("缺少租户声明：路径需要 tenant_id 或 X-Tenant-Id 请求头",
                        request_id=request_id)
    if x_tenant_id is not None and x_tenant_id != path_tenant and path_tenant is not None:
        raise TenantForbidden(
            f"X-Tenant-Id={x_tenant_id!r} 与路径租户 {path_tenant!r} 不一致",
            request_id=request_id,
        )
    if target != resolved:
        # 凭据本身有效，但试图访问别的租户 -> 明确的越权，而非笼统 401。
        raise TenantForbidden(
            f"API Key 属于租户 {resolved!r}，无权访问租户 {target!r} 的资源",
            request_id=request_id,
        )
    return Principal(tenant_id=resolved, request_id=request_id)


def _check_admin(x_admin_key: str | None) -> None:
    expected = os.environ.get("LEDGER_ADMIN_KEY", "dev-admin-key")
    if not x_admin_key:
        raise AuthError("缺少管理员凭据：请求头 X-Admin-Key 必填")
    if x_admin_key != expected:
        raise AuthError("无效的管理员 Key")


# ================= 应用工厂 =================

def create_app(service: LedgerService | None = None) -> FastAPI:
    app = FastAPI(title="多租户事件溯源账本", version="1.0.0")

    if service is None:
        home = os.environ.get("LEDGER_HOME", os.path.join(os.getcwd(), ".ledger-data"))
        service = LedgerService(Storage(home), SystemClock())
        logger.info("ledger initialized storage=%s", home)
    app.state.service = service

    # ---------- 统一错误处理 ----------
    @app.exception_handler(LedgerError)
    async def _ledger_error_handler(request: Request, exc: LedgerError):
        request_id = getattr(request.state, "request_id", exc.request_id)
        status = _STATUS_MAP.get(exc.code, 400)
        logger.warning(
            "request_rejected request_id=%s code=%s status=%d path=%s msg=%s",
            request_id, exc.code, status, request.url.path, exc.message,
        )
        return JSONResponse(
            status_code=status,
            content={"error": {"code": exc.code, "message": exc.message,
                               "request_id": request_id}},
        )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    # ---------- 管理接口 ----------
    @app.post("/admin/tenants", status_code=201)
    async def create_tenant(
        body: CreateTenantBody,
        x_admin_key: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        _check_admin(x_admin_key)
        rid = x_request_id or new_request_id()
        result = service.create_tenant(
            body.tenant_id, body.name, body.api_key,
            currencies=tuple(body.currencies), request_id=rid,
        )
        return {"request_id": rid, "tenant": result}

    @app.post("/admin/tenants/{tenant_id}/replay")
    async def replay_tenant(
        tenant_id: str,
        x_admin_key: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        _check_admin(x_admin_key)
        rid = x_request_id or new_request_id()
        logger.info("replay_request request_id=%s tenant=%s", rid, tenant_id)
        summary = service.replay(tenant_id, from_checkpoint=True)
        return {"request_id": rid, "replay": summary}

    # ---------- 业务接口 ----------
    @app.post("/tenants/{tenant_id}/accounts", status_code=201)
    async def open_account(
        tenant_id: str,
        body: OpenAccountBody,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        result = service.open_account(
            principal.tenant_id, body.account_id, body.currency,
            initial_balance=body.initial_balance, request_id=principal.request_id,
        )
        return {"request_id": principal.request_id, "account": result}

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/close", status_code=200)
    async def close_account(
        tenant_id: str,
        account_id: str,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        result = service.close_account(
            principal.tenant_id, account_id, request_id=principal.request_id,
        )
        return {"request_id": principal.request_id, "account": result}

    @app.get("/tenants/{tenant_id}/accounts")
    async def list_accounts(
        tenant_id: str,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        accounts = service.list_accounts(principal.tenant_id)
        return {
            "request_id": principal.request_id,
            "accounts": [
                {"account_id": a.account_id, "currency": a.currency,
                 "status": a.status, "balance": a.balance, "version": a.version}
                for a in accounts
            ],
        }

    @app.get("/tenants/{tenant_id}/accounts/{account_id}")
    async def get_account(
        tenant_id: str,
        account_id: str,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        bal = service.get_balance(principal.tenant_id, account_id)
        return {
            "request_id": principal.request_id,
            "account": {
                "account_id": bal.account_id, "currency": bal.currency,
                "status": bal.status, "balance": bal.balance,
                "version": bal.version,
            },
        }

    @app.post("/tenants/{tenant_id}/transfers", status_code=201)
    async def transfer(
        tenant_id: str,
        body: TransferBody,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        if not idempotency_key:
            raise InvalidAmount("转账请求缺少 Idempotency-Key 请求头",
                                request_id=principal.request_id)
        result = service.transfer(
            tenant_id=principal.tenant_id,
            idempotency_key=idempotency_key,
            request_id=principal.request_id,
            from_account=body.from_account,
            to_account=body.to_account,
            amount=body.amount,
            fee=body.fee,
            currency=body.currency,
        )
        return {
            "request_id": principal.request_id,
            "replayed": result.replayed,
            "transfer": {
                "transfer_id": result.transfer_id,
                "idempotency_key": result.idempotency_key,
                "event_seq": result.event_seq,
                "entries": [
                    {"account_id": e.account_id, "amount": e.amount,
                     "currency": e.currency, "role": e.role}
                    for e in result.entries
                ],
                "balances_after": {
                    result.entries[0].account_id: result.from_balance_after,
                    result.entries[1].account_id: result.to_balance_after,
                    result.entries[2].account_id: result.fee_balance_after,
                },
            },
        }

    @app.get("/tenants/{tenant_id}/events")
    async def list_events(
        tenant_id: str,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        events = service.list_events(principal.tenant_id)
        return {
            "request_id": principal.request_id,
            "events": [
                {
                    "seq": e.seq, "event_id": e.event_id,
                    "event_type": e.event_type, "request_id": e.request_id,
                    "idempotency_key": e.idempotency_key,
                    "timestamp": e.timestamp,
                    "prev_hash": e.prev_hash[:12],
                    "event_hash": e.event_hash[:12],
                    "payload": e.payload,
                }
                for e in events
            ],
        }

    @app.post("/tenants/{tenant_id}/checkpoints", status_code=201)
    async def create_checkpoint(
        tenant_id: str,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        cp = service.save_checkpoint(principal.tenant_id)
        return {"request_id": principal.request_id,
                "checkpoint": {"seq": cp.seq, "last_hash": cp.last_hash,
                               "created_at": cp.created_at}}

    @app.get("/tenants/{tenant_id}/verify")
    async def verify_log(
        tenant_id: str,
        request: Request,
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
    ):
        principal = _authenticate(
            request, x_api_key=x_api_key,
            x_tenant_id=x_tenant_id, x_request_id=x_request_id,
        )
        return {"request_id": principal.request_id,
                "verify": service.verify_log(principal.tenant_id)}

    return app
