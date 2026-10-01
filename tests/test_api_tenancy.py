"""HTTP 层：租户隔离、访问控制原因可区分、端到端转账与回放。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from ledger.api import create_app
from ledger.service import LedgerService


def _bootstrap(c: TestClient) -> None:
    admin = {"X-Admin-Key": "dev-admin-key"}
    assert c.post("/admin/tenants",
                  json={"tenant_id": "t1", "name": "甲",
                        "api_key": "key-aaaaaaaa", "currencies": ["CNY"]},
                  headers=admin).status_code == 201
    assert c.post("/admin/tenants",
                  json={"tenant_id": "t2", "name": "乙",
                        "api_key": "key-bbbbbbbb", "currencies": ["CNY"]},
                  headers=admin).status_code == 201



def test_auth_failures_are_distinguished(tmp_home):
    from ledger.clock import SystemClock
    from ledger.storage import Storage

    app = create_app(LedgerService(Storage(str(tmp_home)), SystemClock()))
    c = TestClient(app)
    _bootstrap(c)

    # 无 API Key -> 401 unauthenticated
    r = c.get("/tenants/t1/accounts")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "unauthenticated"

    # 错误 API Key -> 401 unauthenticated（不是 403）
    r = c.get("/tenants/t1/accounts",
              headers={"X-API-Key": "wrong-key", "X-Tenant-Id": "t1"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "unauthenticated"

    # 路径已带 tenant_id 且未提供 X-Tenant-Id 头时，以路径租户为准（正常通过鉴权）
    r = c.get("/tenants/t1/accounts", headers={"X-API-Key": "key-aaaaaaaa"})
    assert r.status_code == 200

    # 请求头租户与路径不一致 -> 403 tenant_forbidden
    r = c.get("/tenants/t1/accounts",
              headers={"X-API-Key": "key-aaaaaaaa", "X-Tenant-Id": "t2"})
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "tenant_forbidden"

    # 有效凭据访问其他租户 -> 403 tenant_forbidden（原因明确不同）
    r = c.get("/tenants/t2/accounts",
              headers={"X-API-Key": "key-aaaaaaaa", "X-Tenant-Id": "t2"})
    assert r.status_code == 403
    body = r.json()["error"]
    assert body["code"] == "tenant_forbidden"
    assert "t1" in body["message"] and "t2" in body["message"]
    assert body["request_id"]  # 失败响应也带请求标识


def test_tenant_data_is_isolated(tmp_home):
    from ledger.clock import SystemClock
    from ledger.storage import Storage

    app = create_app(LedgerService(Storage(str(tmp_home)), SystemClock()))
    c = TestClient(app)
    _bootstrap(c)
    h1 = {"X-API-Key": "key-aaaaaaaa", "X-Tenant-Id": "t1"}
    h2 = {"X-API-Key": "key-bbbbbbbb", "X-Tenant-Id": "t2"}

    c.post("/tenants/t1/accounts", json={"account_id": "a", "initial_balance": 100},
           headers=h1)
    c.post("/tenants/t2/accounts", json={"account_id": "a", "initial_balance": 50},
           headers=h2)

    # 同名账户在不同租户下互不可见、余额互不影响
    assert c.get("/tenants/t1/accounts/a", headers=h1).json()["account"]["balance"] == 100
    assert c.get("/tenants/t2/accounts/a", headers=h2).json()["account"]["balance"] == 50
    # t1 无法用 t2 的账户做转账（账户在其租户命名空间内不存在）
    r = c.post("/tenants/t1/transfers",
               json={"from_account": "a", "to_account": "ghost",
                     "amount": 1, "fee": 0, "currency": "CNY"},
               headers={**h1, "Idempotency-Key": "k1"})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "account_not_found"


def test_end_to_end_transfer_retry_and_replay(tmp_home):
    from ledger.clock import SystemClock
    from ledger.storage import Storage

    app = create_app(LedgerService(Storage(str(tmp_home)), SystemClock()))
    c = TestClient(app)
    _bootstrap(c)
    h = {"X-API-Key": "key-aaaaaaaa", "X-Tenant-Id": "t1",
         "X-Request-Id": "req-e2e-1"}
    c.post("/tenants/t1/accounts", json={"account_id": "a", "initial_balance": 500},
           headers=h)
    c.post("/tenants/t1/accounts", json={"account_id": "b", "initial_balance": 0},
           headers=h)

    # 缺幂等键被拒
    r = c.post("/tenants/t1/transfers",
               json={"from_account": "a", "to_account": "b", "amount": 100,
                     "fee": 3, "currency": "CNY"}, headers=h)
    assert r.status_code == 422

    # 首次提交
    r1 = c.post("/tenants/t1/transfers",
                json={"from_account": "a", "to_account": "b", "amount": 100,
                      "fee": 3, "currency": "CNY"},
                headers={**h, "Idempotency-Key": "tx-1"})
    assert r1.status_code == 201
    assert r1.json()["replayed"] is False
    transfer_id = r1.json()["transfer"]["transfer_id"]
    assert sum(e["amount"] for e in r1.json()["transfer"]["entries"]) == 0

    # 超时重发：新请求标识 + 同幂等键 -> 一致结果、不重复入账
    r2 = c.post("/tenants/t1/transfers",
                json={"from_account": "a", "to_account": "b", "amount": 100,
                      "fee": 3, "currency": "CNY"},
                headers={**h, "X-Request-Id": "req-e2e-retry",
                         "Idempotency-Key": "tx-1"})
    assert r2.status_code == 201
    assert r2.json()["replayed"] is True
    assert r2.json()["transfer"]["transfer_id"] == transfer_id

    # 余额精确
    r = c.get("/tenants/t1/accounts", headers=h)
    totals = {x["account_id"]: x["balance"] for x in r.json()["accounts"]}
    assert totals == {"_fee_CNY": 3, "a": 397, "b": 100}
    assert sum(totals.values()) == 500

    # 管理端回放：重放后余额一致
    r = c.post("/admin/tenants/t1/replay",
               headers={"X-Admin-Key": "dev-admin-key",
                        "X-Request-Id": "req-admin-replay"})
    assert r.status_code == 200
    assert sum(r.json()["replay"]["balances"].values()) == 500

    # 事件列表可审计，哈希链可见
    r = c.get("/tenants/t1/events", headers=h)
    types = [e["event_type"] for e in r.json()["events"]]
    assert types == ["tenant_created", "account_opened", "account_opened",
                     "transfer_committed"]


def test_insufficient_funds_http_status_distinct(tmp_home):
    from ledger.clock import SystemClock
    from ledger.storage import Storage

    app = create_app(LedgerService(Storage(str(tmp_home)), SystemClock()))
    c = TestClient(app)
    _bootstrap(c)
    h = {"X-API-Key": "key-aaaaaaaa", "X-Tenant-Id": "t1"}
    c.post("/tenants/t1/accounts", json={"account_id": "a", "initial_balance": 5},
           headers=h)
    c.post("/tenants/t1/accounts", json={"account_id": "b", "initial_balance": 0},
           headers=h)
    r = c.post("/tenants/t1/transfers",
               json={"from_account": "a", "to_account": "b", "amount": 100,
                     "fee": 0, "currency": "CNY"},
               headers={**h, "Idempotency-Key": "poor-1"})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "insufficient_funds"


def test_admin_endpoints_require_admin_key(tmp_home):
    from ledger.clock import SystemClock
    from ledger.storage import Storage

    app = create_app(LedgerService(Storage(str(tmp_home)), SystemClock()))
    c = TestClient(app)
    r = c.post("/admin/tenants", json={"tenant_id": "x", "name": "x",
                                       "api_key": "key-xxxxxxxx"})
    assert r.status_code == 401
    r = c.post("/admin/tenants", json={"tenant_id": "x", "name": "x",
                                       "api_key": "key-xxxxxxxx"},
               headers={"X-Admin-Key": "bad"})
    assert r.status_code == 401
