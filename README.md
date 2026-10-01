# event-sourced-ledger-python

多租户事件溯源账本服务（FastAPI + 本地 JSONL 存储），保证并发转账、重复提交与故障回放下的结果一致且可解释。

## 账本与事件模型

- **追加式事件日志**：所有账目变动以事件形式追加到 `data/events.jsonl`（一行一事件，写后 fsync）。事件携带租户内严格递增的 `seq` 与对关键字段的 SHA-256 `checksum`。
- **事件类型**：
  - `tenant_registered`：注册租户，同时建立租户内置手续费账户 `__fees__`；
  - `account_opened`：开户（可带初始余额）；
  - `transfer_recorded`：转账，payload 内含完整记账分录。
- **双记转账**：每笔转账成对记账，手续费单独成录：
  `(from, -amount, principal)`、`(to, +amount, principal)`、`(from, -fee, fee)`、`(__fees__, +fee, fee)`。
  分录借贷之和恒为 0，因此任意时刻「各方余额 + 手续费账户」之和守恒（`conservation_total` 可校验）。
- **检查点**：`POST /tenants/{t}/checkpoint` 把当前各租户余额固化到 `data/checkpoint.json`，供快速重放。

## 租户隔离与访问控制

每个请求必须带 `X-Tenant-Id` 头且与路径租户一致。失败原因可区分（响应体 `detail.reason`）：

| reason | HTTP | 含义 |
|---|---|---|
| `missing_tenant` | 401 | 缺少租户头 |
| `cross_tenant_read` | 403 | 跨租户读 |
| `cross_tenant_write` | 403 | 跨租户写（含动用他租户账户转账） |
| `tenant_not_found` / `account_not_found` | 404 | 租户/账户不存在 |
| `insufficient_funds` | 422 | 余额不足（含手续费），单独识别，不发生任何扣款 |
| `invalid_transfer` | 400 | 非法金额/自转账等 |

## 幂等规则

- 转账必须带 `Idempotency-Key` 头，`(租户, 幂等键)` 唯一标识一笔业务请求。
- 超时重发/重复提交：直接返回**首次入账的原始结果**（相同分录与余额快照），不会重复入账。
- 幂等表由事件日志派生，服务重启回放后仍然有效。

## 回放规则

- 启动时全量回放事件日志恢复状态（同时完成完整性校验）。
- `POST /tenants/{t}/replay?from_seq=N` 确定性重建余额：`from_seq=0` 全量重放；`from_seq=检查点 seq` 从检查点起放；两者结果必须一致且与实时余额对账，不符抛 `checkpoint_mismatch`。重放是纯函数，可重复执行。
- 事件完整性校验（读取日志时强制）：
  - `corrupt_event`：校验和不匹配或记录不可解析；
  - `duplicate_event`：seq 重复；
  - `out_of_order_event`：seq 跳跃/乱序。
  三类问题分别以不同错误拒绝，不会被静默接受。

## 并发一致性

服务内以互斥锁保护「校验余额 → 追加事件 → 应用状态」全过程：并发转账不会丢失更新、不会透支；余额不足的交易被单独拒绝且不影响其他交易。

## 本地验证

```bash
uv run python -m pytest tests/ -q -s   # 运行全部测试并查看账目流水日志
uv run python main.py                  # 启动服务（数据目录由 LEDGER_DATA_DIR 指定，默认 ./data）
```

测试覆盖：并发重复转账（守恒 + 幂等 + 精确成功数）、余额不足、跨租户读写原因区分、检查点重放确定性、故障恢复、乱序/重复/损坏事件识别。单测日志会打印请求标识（transfer_id）与关键账目流水（分录与余额）。
