# 多租户事件溯源账本（event-sourced-ledger-python）

一套可以真正管账的多租户账本服务：本地文件存储模拟真实账目，
保证**并发转账、重复提交、故障回放**三种场景下结果一致且可解释。

- 多租户隔离与可区分的访问控制失败原因
- 复式记账（双记转账）+ 独立手续费账户，任意时刻金额守恒
- 租户级串行化 + 追加式事件日志，杜绝丢失更新与透支
- 幂等键 + 首次结果留痕，超时重发返回与原请求完全一致的结果
- 哈希链事件日志，支持从检查点确定性回放；乱序/重复/损坏可区分识别
- FastAPI HTTP 接口 + 37 项 pytest 测试（含并发、幂等、故障回放）

---

## 1. 目录结构

```
ledger/
  errors.py    # 全部错误类型，每个错误带稳定 code（失败原因可区分）
  models.py    # 领域模型：账户/分录/事件/幂等记录/检查点
  clock.py     # 时钟（SystemClock / 测试用可控时钟）
  ids.py       # transfer_id / event_id / request_id 生成
  events.py    # 事件规范化哈希、哈希链校验（乱序/重复/损坏识别）
  storage.py   # 本地文件存储：事件日志、幂等记录、检查点、租户注册表
  service.py   # 领域服务：开户、复式转账、幂等、回放
  api.py       # FastAPI：鉴权、租户隔离、HTTP 接口、统一错误响应
main.py        # 应用入口
tests/         # 测试套件（事件/服务/并发/回放/HTTP 隔离）
```

本地数据目录（默认 `./.ledger-data`，可用环境变量 `LEDGER_HOME` 覆盖）：

```
.ledger-data/
  registry.json                     # 租户注册表（api_key <-> tenant_id）
  <tenant_id>/
    events.log                      # 追加式事件日志，每行一条 JSON
    idem/<Idempotency-Key>          # 每个幂等键一条首次结果记录
    checkpoint.json                 # 余额检查点快照（可选）
```

不同租户的数据位于不同目录，命名空间天然隔离。

---

## 2. 账本模型

### 2.1 金额

金额一律为**整数最小单位**（如"分"），不使用浮点数。同一转账限定单一币种。

### 2.2 复式记账（双记）

一笔转账产生 **3 条带符号分录**，分录金额之和恒为 `0`：

| 角色  | 账户              | 金额             | 含义                 |
|-------|-------------------|------------------|----------------------|
| payer | 付款方账户        | `-(amount + fee)`| 本金与手续费一并扣减 |
| payee | 收款方账户        | `+amount`        | 本金到账             |
| fee   | 租户手续费账户 `_fee_<币种>` | `+fee`  | 手续费单独体现       |

因此对租户内全部账户求和：

```
付款方余额 + 收款方余额 + 手续费账户余额（+ 其余账户）= 不变量
```

转账前后总额严格守恒。手续费账户按币种在租户创建时自动开立
（如 `_fee_CNY`），流水与客户账户分离、可独立对账。

### 2.3 账户状态

`open` / `closed`。关闭后的账户不能再作为转账双方。

---

## 3. 事件模型

所有账目变动都只通过**追加事件**完成，内存余额是事件日志的纯投影。

事件类型：

- `tenant_created`：建租户，声明启用币种与手续费账户
- `account_opened` / `account_closed`：开户 / 关户
- `transfer_committed`：转账提交，内含 3 条分录与三账户的事后余额快照

### 3.1 哈希链

每条事件包含连续序号 `seq`（租户内从 1 开始）以及：

```
event_hash = sha256( schema_version | prev_hash | seq | event_id |
                     event_type | tenant_id | request_id | idempotency_key |
                     timestamp | canonical_json(payload) )
```

`canonical_json` 对键排序、去空白，保证哈希确定。事件首尾相接形成链。

### 3.2 损坏 / 乱序 / 重复的区分

回放时逐条 `verify_event`，三类问题分别抛出**不同错误码**，绝不静默接受：

| 情况                               | 错误码               | 判定依据                         |
|------------------------------------|----------------------|----------------------------------|
| 载荷/哈希被篡改、行不是合法 JSON、字段缺失 | `event_corrupted`    | 重算哈希不等于 `event_hash`      |
| 缺事件 / 序号跳跃 / prev_hash 断裂 | `event_out_of_order` | `seq` 大于期望值或哈希链接不上   |
| 旧事件再次出现                     | `event_duplicate`    | `seq` 小于期望值                 |
| 检查点与日志锚点不一致             | `event_corrupted`    | 检查点 seq 处哈希对不上 / 日志被截断 |

---

## 4. 幂等规则

- 转账接口必须携带 `Idempotency-Key`（租户内唯一），以及 `X-Request-Id`。
- 服务端对请求体（收付方、金额、手续费、币种）做归一化摘要 `fingerprint`：
  - **同键 + 同体**：返回首次结果（含相同 `transfer_id`、相同 `event_seq`、
    相同事后余额），响应中 `replayed=true`，且**只入账一次**。
  - **同键 + 异体**：返回 `409 idempotency_conflict`，不会把旧键悄悄变成另一笔。
- **失败也是一种结果**：余额不足等校验失败同样按幂等键记录，重试得到完全相同的
  业务错误（同一错误码、同一信息），且失败不产生任何事件。
- 幂等记录保存首次的完整响应，因此"返回一致"是按记录回放，而非重新计算。

### 4.1 并发正确性

单进程内每个租户一把可重入锁，"读链尾 → 校验 → 追加事件 → 落幂等记录"严格串行；
事件以 `O_APPEND` 单行写入并 `fsync`。因此：

- 同一账户的并发转账不会丢失更新；
- 余额不足在临界区内判定，**绝不透支**；
- N 个并发请求共享一个幂等键时，只有 1 笔真正入账，其余全部命中同一结果。

### 4.2 故障回放与"提交窗口"补洞

提交流程为：**先追加事件并 fsync，再写幂等记录**。若进程在两步之间崩溃：

1. 重启后惰性（或通过管理接口主动）回放事件日志重建余额；
2. 回放遇到 `transfer_committed` 但缺失对应幂等记录时，**自动从事件补建**；
3. 因此客户端随后用原幂等键重试，仍拿到与首次一致的结果，不会重复入账。

---

## 5. 回放规则

- `replay` 使用**全新投影**，从检查点（若有）续读其后事件，或忽略检查点全量重放；
  重放完成后用新投影替换内存状态。
- 回放是**确定性**的：同一事件日志重放任意次，得到完全相同的余额与链头哈希。
- 检查点保存某 `seq` 的全部余额与链头哈希；续读前先校验它与日志锚点一致，
  不一致即拒绝（防止用过时/脏快照）。
- 回放还会逐笔校验：分录之和必须为 0，付款方余额不得为负，引用的账户必须存在。

---

## 6. HTTP 接口与访问控制

### 6.1 请求头

| 头                | 说明                                            |
|-------------------|-------------------------------------------------|
| `X-API-Key`       | 租户业务凭据（业务接口必填）                    |
| `X-Tenant-Id`     | 租户声明；若提供必须与路径中的 tenant_id 一致   |
| `X-Request-Id`    | 请求标识；缺省服务端生成，日志与响应全程携带    |
| `Idempotency-Key` | 转账幂等键（转账接口必填）                      |
| `X-Admin-Key`     | 管理接口凭据（默认 `dev-admin-key`，可用 `LEDGER_ADMIN_KEY` 覆盖） |

### 6.2 失败原因区分（不是笼统报错）

| HTTP | code                     | 触发情形                                   |
|------|--------------------------|--------------------------------------------|
| 401  | `unauthenticated`        | 缺少/无效 API Key、缺少租户声明            |
| 403  | `tenant_forbidden`       | 凭据有效但访问其他租户 / 头与路径租户冲突  |
| 404  | `tenant_not_found` / `account_not_found` | 租户或账户不存在              |
| 409  | `already_exists`         | 租户/账户重复                              |
| 409  | `account_closed`         | 账户已关闭仍要记账                         |
| 409  | `idempotency_conflict`   | 幂等键复用于不同请求体                     |
| 422  | `insufficient_funds`     | 余额不足（独立于其他校验错误）             |
| 422  | `invalid_amount` / `same_account` / `currency_not_enabled` | 参数类错误 |
| 422  | `event_out_of_order` / `event_duplicate` / `event_corrupted` | 事件日志问题 |

错误响应体形如：

```json
{"error": {"code": "tenant_forbidden",
           "message": "API Key 属于租户 't1'，无权访问租户 't2' 的资源",
           "request_id": "req_..."}}
```

### 6.3 端点

```
POST   /admin/tenants                              # 建租户（管理员）
POST   /admin/tenants/{tid}/replay                 # 触发回放（管理员）
POST   /tenants/{tid}/accounts                     # 开户
POST   /tenants/{tid}/accounts/{aid}/close         # 关户
GET    /tenants/{tid}/accounts                     # 列出账户与余额
GET    /tenants/{tid}/accounts/{aid}               # 查单个账户余额
POST   /tenants/{tid}/transfers                    # 复式转账（需 Idempotency-Key）
GET    /tenants/{tid}/events                       # 查看事件流水（哈希链）
GET    /tenants/{tid}/verify                       # 仅校验事件日志完整性
POST   /tenants/{tid}/checkpoints                  # 保存余额检查点
GET    /health
```

---

## 7. 本地验证方法

### 7.1 环境准备

```bash
uv sync --extra dev          # 安装 fastapi / pytest / httpx
```

### 7.2 运行测试

```bash
uv run pytest                # 全部 37 项，实时打印请求标识与账目流水日志
uv run pytest -q             # 简洁输出
```

测试分布：

- `tests/test_events.py`：哈希链、篡改/乱序/重复/损坏识别（9 项）
- `tests/test_ledger_service.py`：复式守恒、余额不足、幂等成功/失败/冲突（8 项）
- `tests/test_concurrency.py`：200 线程双向并发、手续费守恒、竞争不透支、
  并发重复提交只入账一次、同键异体冲突（5 项）
- `tests/test_replay.py`：全量/增量回放确定性、重启重建、检查点失效、
  事件与幂等记录之间崩溃补洞、损坏/重复/乱序/脏行识别（10 项）
- `tests/test_api_tenancy.py`：401 与 403 区分、跨租户数据隔离、
  端到端转账/重发/回放（5 项）

日志中可看到类似关键流水（含 request_id 与三账户余额）：

```
INFO ledger transfer_committed request_id=req-1 tenant=t1 transfer=txn_.. \
     idem_key=k1 a->b amount=300 fee=10 event_seq=4 \
     balances after: from=690 to=300 fee=10
INFO ledger transfer_idempotent_hit request_id=req-1-retry original_request=req-1 \
     idem_key=k1 transfer=txn_.. event_seq=4
```

### 7.3 启动服务并手工验证

```bash
uv run python main.py         # 默认 127.0.0.1:8080，数据在 ./.ledger-data
```

另开终端：

```bash
ADMIN="X-Admin-Key: dev-admin-key"

# 1) 建两个租户
curl -s -XPOST localhost:8080/admin/tenants -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"tenant_id":"t1","name":"甲","api_key":"key-aaaaaaaa","currencies":["CNY"]}'
curl -s -XPOST localhost:8080/admin/tenants -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"tenant_id":"t2","name":"乙","api_key":"key-bbbbbbbb","currencies":["CNY"]}'

# 2) 开户
H1='X-API-Key: key-aaaaaaaa'
curl -s -XPOST localhost:8080/tenants/t1/accounts -H "$H1" \
  -H 'Content-Type: application/json' -d '{"account_id":"a","initial_balance":1000}'
curl -s -XPOST localhost:8080/tenants/t1/accounts -H "$H1" \
  -H 'Content-Type: application/json' -d '{"account_id":"b","initial_balance":0}'

# 3) 转账（带幂等键与请求标识）
curl -s -XPOST localhost:8080/tenants/t1/transfers -H "$H1" \
  -H 'Idempotency-Key: tx-1' -H 'X-Request-Id: req-1' \
  -H 'Content-Type: application/json' \
  -d '{"from_account":"a","to_account":"b","amount":300,"fee":10,"currency":"CNY"}'

# 4) 超时重发（换 request_id，幂等键与请求体不变）-> replayed=true，同一 transfer_id
curl -s -XPOST localhost:8080/tenants/t1/transfers -H "$H1" \
  -H 'Idempotency-Key: tx-1' -H 'X-Request-Id: req-1-retry' \
  -H 'Content-Type: application/json' \
  -d '{"from_account":"a","to_account":"b","amount":300,"fee":10,"currency":"CNY"}'

# 5) 越权访问：t1 的 key 访问 t2 -> 403 tenant_forbidden
curl -s localhost:8080/tenants/t2/accounts -H "$H1"

# 6) 事件流水 / 完整性 / 回放
curl -s localhost:8080/tenants/t1/events -H "$H1"
curl -s localhost:8080/tenants/t1/verify -H "$H1"
curl -s -XPOST localhost:8080/admin/tenants/t1/replay -H "$ADMIN"
```

---

## 8. 设计说明与边界

- 持久化用"临时文件 + fsync + rename"写幂等记录/检查点，事件日志用
  `O_APPEND` 单行追加 + fsync，崩溃后不会读到半条记录。
- 并发控制为**单进程多线程**模型（租户锁）。多进程/多机部署时需把租户锁与
  事件追加下沉为数据库/共识日志，接口与不变量保持不变。
- 注册表（api_key）是简化的演示实现，生产应替换为专用密钥管理。
