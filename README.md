# 消息交付工作台 · 确认水位与缺口

并发消费时的乱序确认演示与测试台。核心是一个「连续已确认前缀 + 前方离散确认集合」的状态机,
修复了「12 先于 11 确认就把提交水位设为 12,崩溃后 11 永不重放」的问题。

## 核心不变量

- **committedSeq(提交水位)** 是连续已确认前缀的右端。只有当缺口从前往后连续补齐时,水位才逐格推进;
  乱序 ack(如先 ack 12 再 ack 11)只会进入离散集合,不会越过缺口。
- **ackedAhead(前方离散确认集合)** 保存水位之后、已确认但无法并入前缀的序号。
- 已提交前缀(`seq <= committedSeq`)内的消息**永远不再交付**,重启后同样成立(随快照持久化)。
- ack / nack / cancel 全部绑定 **消费者租约 + delivery attempt**:租约过期或 attempt 不匹配的
  (含迟到)确认一律拒绝。
- nack、attempt 超时、取消、租约丢失只把消息从在途集合放回可交付状态,**绝不保留成功确认**;
  因此缺口消息必定可恢复,nack 再入队不会被任何水位掩盖。
- 前端展示水位与**缺口区间列表**,而不是单个最大确认值。

## 运行

```bash
npm install
npm run dev            # 前端 http://localhost:4173,API 在 4174
```

带快照持久化启动(崩溃重启可跨进程恢复):

```bash
DELIVERY_SNAPSHOT=./data/snapshot.json npm start
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/publish` | `{count}` 发布连续序号消息 |
| POST | `/api/consumers` | `{consumerId, leaseMs}` 注册消费者获取租约 |
| POST | `/api/consumers/:id/renew` | 续租 |
| POST | `/api/deliver` | `{consumerId, maxMessages, attemptTimeoutMs}` 拉取在途交付 |
| POST | `/api/ack` | `{consumerId, seq, attempt}` 单条确认 |
| POST | `/api/ack/batch` | `{consumerId, acks:[{seq, attempt}]}` 原子批量确认 |
| POST | `/api/nack` | `{consumerId, seq, attempt, requeueDelayMs}` 否定并回队 |
| POST | `/api/cancel` | 主动放弃并回队 |
| POST | `/api/sweep` | 处理 attempt 超时与租约丢失,统一回队 |
| POST | `/api/restart` | 模拟崩溃重启(快照恢复 + 在途消息重新可交付) |
| GET | `/api/state` | 水位、离散确认集合、缺口区间、在途、租约、事件流 |

## 测试

```bash
npm test
```

覆盖:乱序 ack、重复 ack、nack 后 ack 迟到(stale attempt)、租约丢失、attempt 超时与取消、
原子批量确认、崩溃重启恢复(含跨进程快照)、十万级序号的巨大缺口压缩展示与恢复。
