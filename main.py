"""多租户事件溯源账本服务的 HTTP 入口。

- 每个请求必须带 X-Tenant-Id 头，且与路径租户一致；
- 读/写越权分别返回 cross_tenant_read / cross_tenant_write；
- 转账用 Idempotency-Key 头做幂等键，重复提交返回首次结果。
"""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ledger.errors import (
    AccountAlreadyExistsError,
    AccountNotFoundError,
    CheckpointMismatchError,
    CorruptEventError,
    CrossTenantReadError,
    CrossTenantWriteError,
    DuplicateEventError,
    InsufficientFundsError,
    InvalidTransferError,
    LedgerError,
    OutOfOrderEventError,
    TenantNotFoundError,
)
from ledger.service import LedgerService
from ledger.store import EventStore

# 错误原因 -> HTTP 状态码（不同原因不同响应，便于客户端区分）
_STATUS_BY_REASON = {
    TenantNotFoundError.reason: 404,
    CrossTenantReadError.reason: 403,
    CrossTenantWriteError.reason: 403,
    AccountNotFoundError.reason: 404,
    AccountAlreadyExistsError.reason: 409,
    InsufficientFundsError.reason: 422,
    InvalidTransferError.reason: 400,
    DuplicateEventError.reason: 409,
    OutOfOrderEventError.reason: 409,
    CorruptEventError.reason: 422,
    CheckpointMismatchError.reason: 409,
}

DATA_DIR = Path(os.environ.get("LEDGER_DATA_DIR", "data"))
service = LedgerService(EventStore(DATA_DIR))
app = FastAPI(title="event-sourced-ledger")


@app.exception_handler(LedgerError)
async def ledger_error_handler(_: Request, exc: LedgerError) -> JSONResponse:
    status = _STATUS_BY_REASON.get(exc.reason, 500)
    return JSONResponse(
        status_code=status,
        content={"detail": {"reason": exc.reason, "message": exc.message}},
    )


def _require_tenant_match(path_tenant: str, header_tenant: str | None, write: bool) -> str:
    if not header_tenant:
        raise HTTPException(status_code=401, detail={"reason": "missing_tenant", "message": "X-Tenant-Id header required"})
    if header_tenant != path_tenant:
        reason = "cross_tenant_write" if write else "cross_tenant_read"
        raise HTTPException(status_code=403, detail={"reason": reason, "message": f"tenant {header_tenant!r} cannot access tenant {path_tenant!r}"})
    return path_tenant


class OpenAccountRequest(BaseModel):
    account_id: str
    balance: int = Field(default=0, ge=0)


class TransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: int = Field(gt=0)
    fee: int = Field(default=0, ge=0)


@app.post("/tenants/{tenant}")
def register_tenant(tenant: str, x_tenant_id: str | None = Header(default=None)):
    _require_tenant_match(tenant, x_tenant_id, write=True)
    service.register_tenant(tenant)
    return {"tenant": tenant}


@app.post("/tenants/{tenant}/accounts", status_code=201)
def open_account(tenant: str, body: OpenAccountRequest, x_tenant_id: str | None = Header(default=None)):
    _require_tenant_match(tenant, x_tenant_id, write=True)
    service.open_account(tenant, body.account_id, body.balance)
    return {"tenant": tenant, "account_id": body.account_id, "balance": body.balance}


@app.get("/tenants/{tenant}/accounts/{account_id}/balance")
def get_balance(tenant: str, account_id: str, x_tenant_id: str | None = Header(default=None)):
    _require_tenant_match(tenant, x_tenant_id, write=False)
    return {"tenant": tenant, "account_id": account_id, "balance": service.get_balance(tenant, account_id)}


@app.post("/tenants/{tenant}/transfers", status_code=201)
def transfer(
    tenant: str,
    body: TransferRequest,
    x_tenant_id: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None),
):
    _require_tenant_match(tenant, x_tenant_id, write=True)
    if not idempotency_key:
        raise HTTPException(status_code=400, detail={"reason": "missing_idempotency_key", "message": "Idempotency-Key header required"})
    result = service.transfer(
        tenant, idempotency_key, body.from_account, body.to_account, body.amount, body.fee
    )
    return {
        "transfer_id": result.transfer_id,
        "tenant": result.tenant_id,
        "entries": [e.__dict__ for e in result.entries],
        "balances": result.balances,
    }


@app.post("/tenants/{tenant}/replay")
def replay(tenant: str, from_seq: int = 0, x_tenant_id: str | None = Header(default=None)):
    _require_tenant_match(tenant, x_tenant_id, write=False)
    return {"tenant": tenant, "from_seq": from_seq, "balances": service.replay(tenant, from_seq)}


@app.post("/tenants/{tenant}/checkpoint", status_code=201)
def checkpoint(tenant: str, x_tenant_id: str | None = Header(default=None)):
    _require_tenant_match(tenant, x_tenant_id, write=True)
    cp = service.save_checkpoint()
    return {"seq": cp.seq}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
