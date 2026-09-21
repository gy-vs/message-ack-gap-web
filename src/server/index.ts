import express from 'express';
import {fileURLToPath} from 'node:url';
import {DeliveryError, DeliveryStore, type StoreOptions} from './delivery';
import {FilePersistence} from './persistence';

export type AppOptions = {store?: DeliveryStore; snapshotFile?: string};

export function createApp(options: AppOptions = {}) {
  const storeOptions: StoreOptions = {};
  if (!options.store && options.snapshotFile) {
    storeOptions.persistence = new FilePersistence(options.snapshotFile);
  }
  const store = options.store ?? new DeliveryStore(storeOptions);
  const app = express();
  app.use(express.json({limit: '1mb'}));

  /** 发布消息,序号严格单调连续 */
  app.post('/api/publish', (req, res) => {
    const count = Number(req.body?.count ?? 1);
    const seqs = store.publish(count);
    res.json({published: seqs.length, firstSeq: seqs[0], lastSeq: seqs[seqs.length - 1]});
  });

  /** 注册消费者并获取租约 */
  app.post('/api/consumers', (req, res) => {
    const id = String(req.body?.consumerId ?? '');
    const leaseMs = Number(req.body?.leaseMs ?? 30_000);
    if (!id) throw new DeliveryError('invalid_body', 'consumerId required');
    res.json(store.registerConsumer(id, leaseMs));
  });

  /** 续租 */
  app.post('/api/consumers/:id/renew', (req, res) => {
    const leaseMs = Number(req.body?.leaseMs ?? 30_000);
    res.json(store.renewLease(req.params.id, leaseMs));
  });

  /** 拉取可交付消息(绑定消费者租约) */
  app.post('/api/deliver', (req, res) => {
    const consumerId = String(req.body?.consumerId ?? '');
    const maxMessages = Number(req.body?.maxMessages ?? 1);
    const attemptTimeoutMs = Number(req.body?.attemptTimeoutMs ?? 60_000);
    if (!consumerId) throw new DeliveryError('invalid_body', 'consumerId required');
    const deliveries = store.deliver(consumerId, maxMessages, attemptTimeoutMs);
    res.json({deliveries});
  });

  /** 单条 ack,必须携带当前 delivery attempt */
  app.post('/api/ack', (req, res) => {
    const {consumerId, seq, attempt} = readBinding(req.body);
    res.json(store.ack(consumerId, seq, attempt));
  });

  /** 批量 ack,原子成功或整批拒绝 */
  app.post('/api/ack/batch', (req, res) => {
    const consumerId = String(req.body?.consumerId ?? '');
    if (!consumerId) throw new DeliveryError('invalid_body', 'consumerId required');
    const acks = Array.isArray(req.body?.acks) ? req.body.acks : null;
    if (!acks) throw new DeliveryError('invalid_body', 'acks[] required');
    const result = store.ackBatch(
      consumerId,
      acks.map((a: {seq?: unknown; attempt?: unknown}) => ({
        seq: Number(a?.seq),
        attempt: Number(a?.attempt),
      })),
    );
    res.json(result);
  });

  /** 否定确认:回队,不保留成功确认 */
  app.post('/api/nack', (req, res) => {
    const {consumerId, seq, attempt} = readBinding(req.body);
    const requeueDelayMs = Number(req.body?.requeueDelayMs ?? 0);
    store.nack(consumerId, seq, attempt, requeueDelayMs);
    res.json({ok: true, requeued: seq});
  });

  /** 主动取消:回队 */
  app.post('/api/cancel', (req, res) => {
    const {consumerId, seq, attempt} = readBinding(req.body);
    store.cancel(consumerId, seq, attempt);
    res.json({ok: true, requeued: seq});
  });

  /** 扫描:attempt 超时与租约丢失统一回队 */
  app.post('/api/sweep', (_req, res) => {
    res.json(store.sweep());
  });

  /** 模拟崩溃重启:从快照恢复水位与离散确认,在途消息重新可交付 */
  app.post('/api/restart', (_req, res) => {
    store.restart();
    res.json({ok: true, state: store.state()});
  });

  /** 工作台状态:展示连续前缀 + 缺口区间,而非单个最大值 */
  app.get('/api/state', (_req, res) => {
    res.json(store.state());
  });

  app.use((error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    if (error instanceof DeliveryError) {
      return res.status(error.status).json({error: error.reason, message: error.message});
    }
    res.status(500).json({error: 'internal'});
  });

  return {app, store};
}

function readBinding(body: Record<string, unknown> | undefined): {
  consumerId: string;
  seq: number;
  attempt: number;
} {
  const consumerId = String(body?.consumerId ?? '');
  const seq = Number(body?.seq);
  const attempt = Number(body?.attempt);
  if (!consumerId) throw new DeliveryError('invalid_body', 'consumerId required');
  if (!Number.isInteger(seq) || !Number.isInteger(attempt)) {
    throw new DeliveryError('invalid_body', 'seq and attempt must be integers');
  }
  return {consumerId, seq, attempt};
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const snapshotFile = process.env.DELIVERY_SNAPSHOT ?? '';
  const {app} = createApp(snapshotFile ? {snapshotFile} : {});
  app.listen(4174, '127.0.0.1', () => console.log('server http://127.0.0.1:4174'));
}
