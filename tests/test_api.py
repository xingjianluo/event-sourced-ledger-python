"""HTTP 层测试：租户头校验、幂等键、错误原因区分。"""

from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("LEDGER_DATA_DIR", str(tmp_path / "data"))
    import main
    importlib.reload(main)
    return TestClient(main.app)


def _setup(c: TestClient) -> None:
    h1 = {"X-Tenant-Id": "t1"}
    c.post("/tenants/t1", headers=h1)
    c.post("/tenants/t1/accounts", headers=h1, json={"account_id": "a", "balance": 100})
    c.post("/tenants/t1/accounts", headers=h1, json={"account_id": "b", "balance": 0})


def test_tenant_header_required_and_cross_tenant_codes(client):
    _setup(client)
    r = client.get("/tenants/t1/accounts/a/balance")
    assert r.status_code == 401 and r.json()["detail"]["reason"] == "missing_tenant"
    r = client.get("/tenants/t1/accounts/a/balance", headers={"X-Tenant-Id": "t2"})
    assert r.status_code == 403 and r.json()["detail"]["reason"] == "cross_tenant_read"
    r = client.post("/tenants/t1/accounts", headers={"X-Tenant-Id": "t2"},
                    json={"account_id": "x"})
    assert r.status_code == 403 and r.json()["detail"]["reason"] == "cross_tenant_write"


def test_transfer_idempotency_and_insufficient_over_http(client):
    _setup(client)
    h = {"X-Tenant-Id": "t1", "Idempotency-Key": "req-1"}
    body = {"from_account": "a", "to_account": "b", "amount": 30, "fee": 2}
    r1 = client.post("/tenants/t1/transfers", headers=h, json=body)
    assert r1.status_code == 201
    r2 = client.post("/tenants/t1/transfers", headers=h, json=body)  # 超时重发
    assert r2.json() == r1.json()  # 返回与首次完全一致
    bal = client.get("/tenants/t1/accounts/a/balance", headers={"X-Tenant-Id": "t1"})
    assert bal.json()["balance"] == 68  # 只入账一次

    h2 = {"X-Tenant-Id": "t1", "Idempotency-Key": "req-2"}
    r = client.post("/tenants/t1/transfers", headers=h2,
                    json={"from_account": "a", "to_account": "b", "amount": 1000})
    assert r.status_code == 422 and r.json()["detail"]["reason"] == "insufficient_funds"

    r = client.post("/tenants/t1/transfers", headers={"X-Tenant-Id": "t1"}, json=body)
    assert r.status_code == 400 and r.json()["detail"]["reason"] == "missing_idempotency_key"


def test_replay_over_http(client):
    _setup(client)
    h = {"X-Tenant-Id": "t1", "Idempotency-Key": "req-1"}
    client.post("/tenants/t1/transfers", headers=h,
                json={"from_account": "a", "to_account": "b", "amount": 30, "fee": 2})
    r = client.post("/tenants/t1/replay", headers={"X-Tenant-Id": "t1"})
    assert r.status_code == 200
    assert r.json()["balances"]["a"] == 68
    assert r.json()["balances"]["b"] == 30
    assert r.json()["balances"]["__fees__"] == 2
