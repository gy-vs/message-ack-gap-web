import {mkdtempSync, readFileSync, existsSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import request from 'supertest';
import {describe, expect, it} from 'vitest';
import {DeliveryStore} from '../src/server/delivery';
import {createApp} from '../src/server/index';
import {FilePersistence} from '../src/server/persistence';

/** 可控时钟 */
function fakeClock(start = 1000) {
  let t = start;
  return {now: () => t, advance: (ms: number) => (t += ms)};
}

type TestApp = ReturnType<typeof createApp>['app'];

async function register(app: TestApp, id: string, leaseMs = 30_000) {
  await request(app).post('/api/consumers').send({consumerId: id, leaseMs}).expect(200);
}

describe('HTTP API', () => {
  it('乱序 ack:先 12 后 11,水位先停在 10,补齐后推进 12,且缺口可见', async () => {
    const {app} = createApp({store: new DeliveryStore()});
    await register(app, 'w1');
    await request(app).post('/api/publish').send({count: 12}).expect(200);
    const delivered = await request(app)
      .post('/api/deliver')
      .send({consumerId: 'w1', maxMessages: 12, attemptTimeoutMs: 60_000})
      .expect(200);
    const items = delivered.body.deliveries as Array<{seq: number; attempt: number}>;

    for (const item of items.slice(0, 10)) {
      await request(app).post('/api/ack').send({consumerId: 'w1', ...item}).expect(200);
    }
    await request(app).post('/api/ack').send({consumerId: 'w1', ...items[11]}).expect(200);

    let state = (await request(app).get('/api/state').expect(200)).body;
    expect(state.committedSeq).toBe(10);
    expect(state.ackedAhead).toEqual([12]);
    // 11 仍在途:它不是可恢复缺口,而是待处理的在途序号
    expect(state.gaps.pendingRanges).toEqual([]);
    expect(state.gaps.inFlightSeqs).toEqual([11]);

    await request(app).post('/api/ack').send({consumerId: 'w1', ...items[10]}).expect(200);
    state = (await request(app).get('/api/state').expect(200)).body;
    expect(state.committedSeq).toBe(12);
    expect(state.gaps.pendingRanges).toEqual([]);
    expect(state.gaps.inFlightSeqs).toEqual([]);
    // 已提交前缀不再交付
    const empty = await request(app)
      .post('/api/deliver')
      .send({consumerId: 'w1', maxMessages: 10, attemptTimeoutMs: 60_000})
      .expect(200);
    expect(empty.body.deliveries).toEqual([]);
  });

  it('重复 ack:前缀内幂等成功,离散集合内幂等,伪造 attempt 被拒', async () => {
    const {app} = createApp({store: new DeliveryStore()});
    await register(app, 'w1');
    await request(app).post('/api/publish').send({count: 2}).expect(200);
    const items = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w1', maxMessages: 2, attemptTimeoutMs: 60_000})
    ).body.deliveries as Array<{seq: number; attempt: number}>;

    const first = await request(app).post('/api/ack').send({consumerId: 'w1', ...items[0]}).expect(200);
    expect(first.body.duplicated).toBe(false);
    const dup = await request(app).post('/api/ack').send({consumerId: 'w1', ...items[0]}).expect(200);
    expect(dup.body.duplicated).toBe(true);

    // 伪造 attempt
    await request(app)
      .post('/api/ack')
      .send({consumerId: 'w1', seq: items[1].seq, attempt: 42})
      .expect(409, {error: 'stale_attempt', message: 'stale_attempt'});
    // 别的消费者不能确认
    await register(app, 'w2');
    await request(app)
      .post('/api/ack')
      .send({...items[1], consumerId: 'w2'})
      .expect(409, {error: 'foreign_consumer', message: 'foreign_consumer'});
  });

  it('nack 后旧 ack 迟到被拒,缺口不被掩盖,重新交付后确认补齐', async () => {
    const {app} = createApp({store: new DeliveryStore()});
    await register(app, 'w1');
    await register(app, 'w2');
    await request(app).post('/api/publish').send({count: 12}).expect(200);
    const items = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w1', maxMessages: 12, attemptTimeoutMs: 60_000})
    ).body.deliveries as Array<{seq: number; attempt: number}>;
    for (const item of items.slice(0, 10)) {
      await request(app).post('/api/ack').send({consumerId: 'w1', ...item});
    }
    await request(app).post('/api/ack').send({consumerId: 'w1', ...items[11]});
    expect((await request(app).get('/api/state')).body.committedSeq).toBe(10);

    // 11 nack 回队
    await request(app).post('/api/nack').send({consumerId: 'w1', ...items[10]}).expect(200);
    // 旧 attempt 迟到 ack:已不在途 -> 409
    await request(app).post('/api/ack').send({consumerId: 'w1', ...items[10]}).expect(409);
    // 缺口仍清晰可见
    const state = (await request(app).get('/api/state')).body;
    expect(state.gaps.pendingRanges).toEqual([{from: 11, to: 11, cooling: false}]);

    // 其他消费者接管,attempt=2
    const redelivered = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w2', maxMessages: 1, attemptTimeoutMs: 60_000})
    ).body.deliveries;
    expect(redelivered).toEqual([
      expect.objectContaining({seq: 11, consumerId: 'w2', attempt: 2}),
    ]);
    await request(app).post('/api/ack').send({consumerId: 'w2', seq: 11, attempt: 2}).expect(200);
    expect((await request(app).get('/api/state')).body.committedSeq).toBe(12);
  });

  it('租约丢失:过期消费者的 ack 被拒,sweep 回队后消息可被接管', async () => {
    const clock = fakeClock();
    const {app} = createApp({store: new DeliveryStore({now: clock.now})});
    await register(app, 'w1', 1000);
    await register(app, 'w2', 10_000);
    await request(app).post('/api/publish').send({count: 1}).expect(200);
    const [item] = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w1', maxMessages: 1, attemptTimeoutMs: 60_000})
    ).body.deliveries;

    clock.advance(1001);
    await request(app).post('/api/ack').send({consumerId: 'w1', ...item}).expect(409, /lease_lost/);
    const swept = (await request(app).post('/api/sweep').send({}).expect(200)).body;
    expect(swept.leaseLost).toEqual(['w1']);

    const taken = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w2', maxMessages: 1, attemptTimeoutMs: 60_000})
    ).body.deliveries[0];
    expect(taken.seq).toBe(1);
    expect(taken.attempt).toBe(2);
    await request(app).post('/api/ack').send({consumerId: 'w2', seq: 1, attempt: 2}).expect(200);
  });

  it('attempt 超时:sweep 回队,超时后的旧 ack 不再有效', async () => {
    const clock = fakeClock();
    const {app} = createApp({store: new DeliveryStore({now: clock.now})});
    await register(app, 'w1', 60_000);
    await request(app).post('/api/publish').send({count: 1}).expect(200);
    const [item] = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w1', maxMessages: 1, attemptTimeoutMs: 500})
    ).body.deliveries;
    clock.advance(501);
    const swept = (await request(app).post('/api/sweep').send({}).expect(200)).body;
    expect(swept.timeouts).toEqual([1]);
    await request(app).post('/api/ack').send({consumerId: 'w1', ...item}).expect(409, /not_in_flight/);
  });

  it('批量确认:原子成功或整批拒绝', async () => {
    const {app} = createApp({store: new DeliveryStore()});
    await register(app, 'w1');
    await request(app).post('/api/publish').send({count: 4}).expect(200);
    const items = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w1', maxMessages: 4, attemptTimeoutMs: 60_000})
    ).body.deliveries as Array<{seq: number; attempt: number}>;
    const ok = await request(app)
      .post('/api/ack/batch')
      .send({
        consumerId: 'w1',
        acks: [
          {seq: items[0].seq, attempt: items[0].attempt},
          {seq: items[2].seq, attempt: items[2].attempt},
        ],
      })
      .expect(200);
    expect(ok.body.advancedTo).toBe(1);

    // 含一个伪造 attempt -> 整批 409
    await request(app)
      .post('/api/ack/batch')
      .send({consumerId: 'w1', acks: [{...items[1]}, {...items[3], attempt: 9}]})
      .expect(409, /stale_attempt/);
    const state = (await request(app).get('/api/state')).body;
    expect(state.committedSeq).toBe(1);
    expect(state.ackedAhead).toEqual([3]);
  });

  it('崩溃重启:快照恢复水位与离散确认,在途消息重新可交付,前缀不再交付', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'delivery-snap-'));
    const file = join(dir, 'snapshot.json');
    try {
      const persistenceA = new FilePersistence(file);
      const a = createApp({store: new DeliveryStore({persistence: persistenceA})});
      await register(a.app, 'w1');
      await request(a.app).post('/api/publish').send({count: 12}).expect(200);
      const items = (
        await request(a.app)
          .post('/api/deliver')
          .send({consumerId: 'w1', maxMessages: 12, attemptTimeoutMs: 60_000})
      ).body.deliveries as Array<{seq: number; attempt: number}>;
      for (const item of items.slice(0, 10)) {
        await request(a.app).post('/api/ack').send({consumerId: 'w1', ...item});
      }
      await request(a.app).post('/api/ack').send({consumerId: 'w1', ...items[11]}); // 12 离散
      // 11 仍在途时"进程崩溃"
      expect(existsSync(file)).toBe(true);
      const onDisk = JSON.parse(readFileSync(file, 'utf8'));
      expect(onDisk.committedSeq).toBe(10);
      expect(onDisk.ackedAhead).toEqual([12]);

      // 全新进程:仅用快照文件构建 app
      const b = createApp({snapshotFile: file});
      const state = (await request(b.app).get('/api/state')).body;
      expect(state.committedSeq).toBe(10);
      expect(state.ackedAhead).toEqual([12]);
      expect(state.gaps.pendingRanges).toEqual([{from: 11, to: 11, cooling: false}]);
      // 消费者租约不持久化,需重新注册
      await request(b.app).post('/api/deliver').send({consumerId: 'w1', maxMessages: 1}).expect(409);
      await register(b.app, 'w1');
      const recovered = (
        await request(b.app)
          .post('/api/deliver')
          .send({consumerId: 'w1', maxMessages: 5, attemptTimeoutMs: 60_000})
      ).body.deliveries;
      // 只有缺口 11;1..10 永不再交付,12 已确认
      expect(recovered).toHaveLength(1);
      expect(recovered[0].seq).toBe(11);
      expect(recovered[0].attempt).toBe(2);
      await request(b.app)
        .post('/api/ack')
        .send({consumerId: 'w1', seq: 11, attempt: 2})
        .expect(200);
      expect((await request(b.app).get('/api/state')).body.committedSeq).toBe(12);
    } finally {
      rmSync(dir, {recursive: true, force: true});
    }
  });

  it('巨大序号缺口:发布十万级,远端确认不越位,缺口以区间/在途两种形态压缩展示', async () => {
    const {app} = createApp({store: new DeliveryStore()});
    await register(app, 'w1');
    await request(app).post('/api/publish').send({count: 100_000}).expect(200);
    // 交付全部序号,仅确认最后两条:水位保持 0,中间 99_998 条在途
    const tail = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w1', maxMessages: 100_000, attemptTimeoutMs: 60_000})
    ).body.deliveries as Array<{seq: number; attempt: number}>;
    const lastTwo = tail.filter(i => i.seq >= 99_999);
    await request(app).post('/api/ack/batch').send({consumerId: 'w1', acks: lastTwo}).expect(200);
    let state = (await request(app).get('/api/state')).body;
    expect(state.committedSeq).toBe(0);
    expect(state.ackedAhead).toEqual([99_999, 100_000]);
    expect(state.gaps.pendingRanges).toEqual([]);
    expect(state.gaps.inFlightSeqs).toHaveLength(99_998);
    expect(state.gaps.inFlightSeqs[0]).toBe(1);

    // 回退 seq 1 后,缺口立即以单个区间 [1,1] 呈现且可恢复
    await request(app).post('/api/nack').send({consumerId: 'w1', seq: 1, attempt: 1}).expect(200);
    state = (await request(app).get('/api/state')).body;
    expect(state.gaps.pendingRanges).toEqual([{from: 1, to: 1, cooling: false}]);
    const redelivered = (
      await request(app)
        .post('/api/deliver')
        .send({consumerId: 'w1', maxMessages: 1, attemptTimeoutMs: 60_000})
    ).body.deliveries;
    expect(redelivered).toEqual([expect.objectContaining({seq: 1, attempt: 2})]);
  });
});
