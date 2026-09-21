# Message Delivery Lab — 连续前缀水位的并发交付工作台

修复“并发消费时序号 12 先于 11 确认，服务端把水位直接设为 12，崩溃重启后 11 永不重放”的工作台。
核心是**连续已确认前缀（committed watermark）+ 前方离散确认集合（ahead set）**：水位只在缺口补齐时推进。

运行：

```bash
npm install
npm run dev          # tsx watch 服务端 (4174) + vite 前端 (4173)
npm test             # vitest，25 个用例
npm run build        # tsc 类型检查 + vite 构建
npm start            # 仅启动服务端
```

状态持久化在 `.delivery-state.json`（已加入 `.gitignore`），写入走临时文件 + 原子 rename。

## 核心数据结构

`src/server/tracker.ts` — `AcknowledgmentTracker`

- `watermark: bigint`：连续已确认前缀的最后一个序号，`<= watermark` 的消息**永不重投**。
- `ahead: Set<bigint>`：乱序到达、停在水位前方的离散确认。例如确认 12 而 11 未确认时，12 进 `ahead`，水位不动。
- `confirm(seq)`：水位之下重复确认 → 幂等 `false`；否则进 `ahead`。
- `advance(holes?)`：从 `watermark+1` 起连续吞并 `ahead`；遇到**已发布但未确认**的真实消息停住；
  对**从未发布**的序号（稀疏发布的大缺口）配合 `HoleSkip` 一次跳过，不扫描序号区间，10^12 的缺口也是 O(1)。
- `retract(seq)`：nack/超时/取消时，仅能撤掉水位前方的离散确认；已并入前缀的不可回退。
- 序号用 `bigint`，HTTP/JSON 层以字符串传输。

`src/server/queue.ts` — `DeliveryQueue`

- **三类状态分离**
  - 可交付（`deliverable`，有序）；
  - 在途（`inFlight`：seq → `{deliveryId, consumerId, attempt, deadlineUntil}`）；
  - 已确认（`tracker`：watermark / ahead）。
- 每次 fetch 生成新 `deliveryId` 并把该 seq 的 `attempt` +1，attempt 计数器**持久化**。
  ack/nack 必须同时匹配 `consumerId + deliveryId + attempt`，旧投递迟到确认返回 `stale_attempt`。
- **租约**：消费者有 `leaseUntil`；每次操作前检查，过期即围栏（`lease_lost`，HTTP 410）。
  前端按 TTL/3 周期心跳；租约丢失时该消费者**所有**在途消息回收到可交付，即使单条投递截止时间还没到。
- **投递超时**：`deadlineUntil` 到期，消息以新 attempt 重回可交付。
- **nack / 超时 / 取消**：从在途删除 → `retract`（绝不保留成功确认）→ 插回有序可交付集合。
  前方有离散确认（如 parked 的 1000000）不会掩盖被 nack 的低序号消息。
- **批量 ack/nack**：租约只校验一次，逐项返回 `ok / duplicate / error`，全部处理完只折叠一次水位。
- **崩溃恢复**：消息、watermark、ahead、attempt 计数持久化；租约和在途是临时态，重启后：
  - 前缀内消息不再交付；
  - 缺口消息（未确认且未 parked）必定回到可交付；
  - 旧客户端持有的旧 `(deliveryId, attempt)` 因 attempt 递增而被围栏。

## HTTP 接口

| 方法 路径 | 说明 |
| --- | --- |
| `POST /api/messages` | 发布；可带 `seq` 稀疏发布（必须 ≥ 下一个序号） |
| `POST /api/consumers` | 注册消费者，返回 `consumerId` 与租约/投递 TTL |
| `POST /api/consumers/heartbeat` | 续租（头 `x-consumer-id`） |
| `POST /api/consumers/cancel` | 主动取消，在途全部回到可交付 |
| `POST /api/fetch` | 拉取（受租约 + maxInFlight 约束） |
| `POST /api/ack` / `POST /api/nack` | 绑定 `seq + deliveryId + attempt` |
| `POST /api/ack/batch` / `POST /api/nack/batch` | 批量，逐项状态 |
| `GET /api/state` | **展示缺口区间** `gaps:[{from,to}]`、`pendingAhead`、`watermark`、`blockingGap`、在途、消费者 |
| `POST /api/admin/restart` | 丢弃临时态并从快照重载（模拟重启） |

错误码：`lease_lost`(410)、`unknown_consumer`(404)、`not_in_flight`/`stale_attempt`/`sequence_already_used`(409)。

## 前端展示

中栏不展示单个“最大确认值”，而是：水位指标、**缺口区间列表**（`3→6`、`8→11`…，带条数和“水位正在等待”标记）、
停在水位前的离散确认、以及按 已提交/缺口/在途/离散确认/可交付 五段着色的序号轨道（百万级缺口压缩为单个色块）。
右栏列出每条在途消息的 `attempt`、deliveryId、consumerId 及 ack/nack 操作；左栏提供乱序 ack、巨大缺口、崩溃重启三个剧本。

## 测试覆盖（`test/`）

乱序 ack（12 先于 11）、重复 ack、nack 后旧 attempt 迟到确认、投递超时、租约丢失围栏与主动取消、
批量确认/否定、崩溃重启恢复（前缀不重投 + 缺口必恢复 + 旧 attempt 被围栏）、巨大序号缺口（O(1) 折叠）、
nack 不被前方离散确认掩盖、缺口区间检查，以及 5 个 supertest HTTP 集成用例。
