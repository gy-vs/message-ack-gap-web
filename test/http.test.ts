import {describe, expect, it, afterEach} from 'vitest';
import request from 'supertest';
import type {Express} from 'express';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {mkdtempSync, rmSync} from 'node:fs';
import {createApp} from '../src/server/index';

const dirs: string[] = [];
function makeApp(): Express {
  const dir = mkdtempSync(join(tmpdir(), 'delivery-http-'));
  dirs.push(dir);
  return createApp({stateFile: join(dir, 'state.json')});
}
afterEach(() => {
  for (const dir of dirs) rmSync(dir, {recursive: true, force: true});
  dirs.length = 0;
});

async function register(app: Express, options: Record<string, number> = {}) {
  const res = await request(app)
    .post('/api/consumers')
    .send({leaseTtlMs: 60_000, deliveryTtlMs: 60_000, maxInFlight: 100, ...options});
  return res.body.consumerId as string;
}

async function publishSeq(app: Express, n: number, seqStart = 1) {
  for (let i = 0; i < n; i++) {
    await request(app).post('/api/messages').send({body: `m${i}`, seq: seqStart + i});
  }
}

describe('HTTP delivery API', () => {
  it('out-of-order acks expose watermark + gap ranges, never a single maximum', async () => {
    const a = makeApp();
    await publishSeq(a, 12);
    const consumer = await register(a);

    const fetched = await request(a).post('/api/fetch').set('x-consumer-id', consumer).send({maxMessages: 12});
    const bySeq = new Map<string, any>(fetched.body.deliveries.map((d: any) => [d.seq, d]));

    for (const seq of [1, 2, 12]) {
      const d = bySeq.get(String(seq));
      await request(a)
        .post('/api/ack')
        .set('x-consumer-id', consumer)
        .send({seq, deliveryId: d.deliveryId, attempt: d.attempt})
        .expect(200);
    }

    const state = await request(a).get('/api/state').expect(200);
    expect(state.body.watermark).toBe('2');
    expect(state.body.pendingAhead).toEqual(['12']);
    expect(state.body.gaps).toEqual([{from: '3', to: '11'}]);
    expect(state.body.blockingGap).toBe('3');

    // Closing the gap collapses the parked ack.
    for (let seq = 3; seq <= 11; seq++) {
      const d = bySeq.get(String(seq));
      await request(a)
        .post('/api/ack')
        .set('x-consumer-id', consumer)
        .send({seq: String(seq), deliveryId: d.deliveryId, attempt: d.attempt});
    }
    const finalState = (await request(a).get('/api/state')).body;
    expect(finalState.watermark).toBe('12');
    expect(finalState.pendingAhead).toEqual([]);
    expect(finalState.gaps).toEqual([]);
    expect(finalState.deliverable).toEqual([]);
  });

  it('fences operations without or with a stale consumer lease (410), and 404s unknown fetch', async () => {
    const a = makeApp();
    await request(a).post('/api/messages').send({body: 'x'});

    await request(a).post('/api/fetch').send({}).expect(404);
    await request(a)
      .post('/api/ack')
      .set('x-consumer-id', 'deadbeef')
      .send({seq: '1', deliveryId: 'd', attempt: 1})
      .expect(410);

    const consumer = await register(a, {leaseTtlMs: 50, deliveryTtlMs: 60_000});
    const d = (await request(a).post('/api/fetch').set('x-consumer-id', consumer).send({})).body.deliveries[0];
    await new Promise(resolve => setTimeout(resolve, 70));
    await request(a).post('/api/consumers/heartbeat').set('x-consumer-id', consumer).send().expect(410);
    await request(a)
      .post('/api/ack')
      .set('x-consumer-id', consumer)
      .send({seq: d.seq, deliveryId: d.deliveryId, attempt: d.attempt})
      .expect(410);
  });

  it('rejects stale delivery attempts with 409 after nack/redelivery', async () => {
    const a = makeApp();
    await publishSeq(a, 1);
    const consumer = await register(a);
    const first = (await request(a).post('/api/fetch').set('x-consumer-id', consumer).send({})).body.deliveries[0];

    await request(a)
      .post('/api/nack')
      .set('x-consumer-id', consumer)
      .send({seq: first.seq, deliveryId: first.deliveryId, attempt: first.attempt})
      .expect(200);

    // Late ack carrying the old attempt must not confirm.
    await request(a)
      .post('/api/ack')
      .set('x-consumer-id', consumer)
      .send({seq: first.seq, deliveryId: first.deliveryId, attempt: first.attempt})
      .expect(409);

    const redelivery = (await request(a).post('/api/fetch').set('x-consumer-id', consumer).send({})).body
      .deliveries[0];
    expect(redelivery.attempt).toBe(first.attempt + 1);
    await request(a)
      .post('/api/ack')
      .set('x-consumer-id', consumer)
      .send({seq: redelivery.seq, deliveryId: redelivery.deliveryId, attempt: redelivery.attempt})
      .expect(200);
  });

  it('batch ack/nack bind every item to lease + attempt and return per-item status', async () => {
    const a = makeApp();
    await publishSeq(a, 4);
    const consumer = await register(a);
    const fetched = (
      await request(a).post('/api/fetch').set('x-consumer-id', consumer).send({maxMessages: 4})
    ).body.deliveries;

    const ackBatch = await request(a)
      .post('/api/ack/batch')
      .set('x-consumer-id', consumer)
      .send({
        items: [
          {seq: fetched[0].seq, deliveryId: fetched[0].deliveryId, attempt: fetched[0].attempt},
          {seq: fetched[3].seq, deliveryId: fetched[3].deliveryId, attempt: fetched[3].attempt},
          {seq: '99', deliveryId: 'ghost', attempt: 1},
        ],
      })
      .expect(200);
    expect(ackBatch.body.watermark).toBe('1');
    expect(ackBatch.body.results.map((r: any) => `${r.seq}:${r.ok ? r.ok : r.error}`)).toEqual([
      '1:true',
      '4:true',
      '99:not_in_flight',
    ]);

    const nackBatch = await request(a)
      .post('/api/nack/batch')
      .set('x-consumer-id', consumer)
      .send({
        items: [{seq: fetched[1].seq, deliveryId: fetched[1].deliveryId, attempt: fetched[1].attempt}],
      })
      .expect(200);
    expect(nackBatch.body.results[0].ok).toBe(true);

    const state = (await request(a).get('/api/state')).body;
    expect(state.pendingAhead).toEqual(['4']);
    expect(state.gaps).toEqual([{from: '2', to: '3'}]);
    expect(state.deliverable).toContain('2');
  });

  it('restart endpoint reloads durable state: parked ack survives, gap stays recoverable', async () => {
    const a = makeApp();
    await publishSeq(a, 3);
    const consumer = await register(a);
    const fetched = (
      await request(a).post('/api/fetch').set('x-consumer-id', consumer).send({maxMessages: 3})
    ).body.deliveries;
    const d3 = fetched.find((d: any) => d.seq === '3');
    await request(a)
      .post('/api/ack')
      .set('x-consumer-id', consumer)
      .send({seq: '3', deliveryId: d3.deliveryId, attempt: d3.attempt})
      .expect(200);

    await request(a).post('/api/admin/restart').expect(200);

    // Old lease is gone; a fresh consumer recovers exactly 1 and 2.
    const state = (await request(a).get('/api/state')).body;
    expect(state.watermark).toBe('0');
    expect(state.pendingAhead).toEqual(['3']);
    expect(state.gaps).toEqual([{from: '1', to: '2'}]);
    expect(state.consumers).toEqual([]);

    const consumer2 = await register(a);
    const recovered = (
      await request(a).post('/api/fetch').set('x-consumer-id', consumer2).send({maxMessages: 10})
    ).body.deliveries.map((d: any) => d.seq);
    expect(recovered).toEqual(['1', '2']);
  });
});
