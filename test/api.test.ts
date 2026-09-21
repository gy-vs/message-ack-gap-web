import {describe, expect, it, beforeEach, afterEach} from 'vitest';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {mkdtempSync, rmSync} from 'node:fs';
import {AcknowledgmentTracker} from '../src/server/tracker';
import {DeliveryQueue} from '../src/server/queue';

let dirs: string[] = [];
function freshStateFile(): string {
  const dir = mkdtempSync(join(tmpdir(), 'delivery-state-'));
  dirs.push(dir);
  return join(dir, 'state.json');
}
afterEach(() => {
  for (const dir of dirs) rmSync(dir, {recursive: true, force: true});
  dirs = [];
});

/** Publish 1..n and fetch exactly one delivery per sequence. */
function seed(queue: DeliveryQueue, n: number, consumerId: string): Map<bigint, {deliveryId: string; attempt: number}> {
  for (let i = 1; i <= n; i++) queue.publish(`msg-${i}`);
  const tokens = new Map<bigint, {deliveryId: string; attempt: number}>();
  while (tokens.size < n) {
    for (const delivery of queue.fetch(consumerId, n)) {
      tokens.set(delivery.seq, {deliveryId: delivery.deliveryId, attempt: delivery.attempt});
    }
  }
  return tokens;
}

describe('AcknowledgmentTracker — contiguous prefix + discrete ahead set', () => {
  it('parks an out-of-order ack ahead of the watermark instead of jumping to it', () => {
    const tracker = new AcknowledgmentTracker();
    expect(tracker.confirmAndAdvance(12n).watermark).toBe(0n);
    expect(tracker.ahead.has(12n)).toBe(true);
    expect(tracker.holes()).toEqual([{from: 1n, to: 11n}]);
    tracker.confirm(11n);
    expect(tracker.advance()).toBe(0n); // 1 still missing
    for (let i = 1; i <= 10; i++) tracker.confirm(BigInt(i));
    expect(tracker.advance()).toBe(12n);
    expect(tracker.ahead.size).toBe(0);
  });

  it('treats a repeated confirmation as a no-op duplicate', () => {
    const tracker = new AcknowledgmentTracker();
    expect(tracker.confirm(5n)).toBe(true);
    expect(tracker.confirm(5n)).toBe(false);
    tracker.confirm(1n);
    tracker.advance();
    expect(tracker.confirm(1n)).toBe(false); // already in committed prefix
    expect(tracker.isConfirmed(1n)).toBe(true);
  });

  it('retract removes only parked confirmations, never the committed prefix', () => {
    const tracker = new AcknowledgmentTracker();
    tracker.confirm(1n);
    tracker.confirm(2n);
    tracker.advance();
    expect(tracker.watermark).toBe(2n);
    expect(tracker.retract(2n)).toBe(false);
    expect(tracker.retract(3n)).toBe(false);
    tracker.confirm(4n);
    expect(tracker.retract(4n)).toBe(true);
  });

  it('handles a huge numeric gap in O(1)-ish work and reports exact holes', () => {
    const tracker = new AcknowledgmentTracker();
    const huge = 1_000_000_000_000n;
    tracker.confirm(huge);
    const start = Date.now();
    expect(tracker.advance()).toBe(0n); // 1 missing => stays put, no scanning
    expect(Date.now() - start).toBeLessThan(100);
    expect(tracker.holes()).toEqual([{from: 1n, to: huge - 1n}]);
    tracker.confirm(1n);
    // With a HoleSkip, never-published numbers in between are leapt over in one jump.
    tracker.advance({exists: () => false, nextExistingAtOrAfter: () => null});
    expect(tracker.watermark).toBe(huge);
  });

  it('leaps over never-published holes but stops at a live message', () => {
    const tracker = new AcknowledgmentTracker();
    tracker.confirm(1n);
    tracker.confirm(10n);
    const live = new Set<bigint>([5n]); // 5 exists and is unconfirmed
    tracker.advance({
      exists: seq => live.has(seq),
      nextExistingAtOrAfter: seq => (5n >= seq ? 5n : null),
    });
    expect(tracker.watermark).toBe(1n); // 2..4 skipped, 5 blocks
    live.clear();
    tracker.advance({exists: () => false, nextExistingAtOrAfter: () => null});
    expect(tracker.watermark).toBe(10n); // 5..9 were never published
  });

  it('survives JSON round-trip with bigint sequences', () => {
    const tracker = new AcknowledgmentTracker(7n, [9n, 11n]);
    const restored = AcknowledgmentTracker.fromJSON(JSON.parse(JSON.stringify(tracker.toJSON())));
    expect(restored.watermark).toBe(7n);
    expect([...restored.ahead]).toEqual([9n, 11n]);
  });
});

describe('DeliveryQueue', () => {
  let queue: DeliveryQueue;
  beforeEach(() => {
    queue = new DeliveryQueue(freshStateFile());
  });

  it('out-of-order ack: seq 12 before 11 parks 12, watermark moves only after the gap closes', () => {
    const consumer = queue.registerConsumer({maxInFlight: 1000}).id;
    const tokens = seed(queue, 12, consumer);

    for (const seq of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]) {
      const token = tokens.get(BigInt(seq))!;
      queue.ack(consumer, BigInt(seq), token.deliveryId, token.attempt);
    }
    expect(queue.tracker.watermark).toBe(10n);
    expect([...queue.tracker.ahead]).toEqual([12n]);

    const token11 = tokens.get(11n)!;
    queue.ack(consumer, 11n, token11.deliveryId, token11.attempt);
    expect(queue.tracker.watermark).toBe(12n);
    expect(queue.tracker.ahead.size).toBe(0);

    // Everything in the committed prefix is permanently done.
    const refetched = queue.fetch(queue.registerConsumer({maxInFlight: 1000}).id, 100);
    expect(refetched.map(d => d.seq)).toEqual([]);
  });

  it('duplicate ack is idempotent and still reports the same watermark', () => {
    const consumer = queue.registerConsumer({maxInFlight: 1000}).id;
    const tokens = seed(queue, 3, consumer);

    const t1 = tokens.get(1n)!;
    const first = queue.ack(consumer, 1n, t1.deliveryId, t1.attempt);
    expect(first.status).toBe('ok');
    const again = queue.ack(consumer, 1n, t1.deliveryId, t1.attempt);
    expect(again.status).toBe('duplicate');
    expect(queue.tracker.watermark).toBe(1n);
    expect(queue.fetch(consumer, 10).map(d => d.seq)).not.toContain(1n);
  });

  it('nack returns the message to deliverable and a late ack on the old attempt is rejected', () => {
    const consumer = queue.registerConsumer({deliveryTtlMs: 60_000, leaseTtlMs: 60_000}).id;
    const tokens = seed(queue, 3, consumer);

    // Ack 3 early (parked), then nack 1.
    const t3 = tokens.get(3n)!;
    queue.ack(consumer, 3n, t3.deliveryId, t3.attempt);
    expect(queue.tracker.watermark).toBe(0n);

    const t1 = tokens.get(1n)!;
    queue.nack(consumer, 1n, t1.deliveryId, t1.attempt);

    // No successful confirmation survives the nack.
    expect(queue.tracker.isConfirmed(1n)).toBe(false);
    expect([...queue.tracker.ahead]).toEqual([3n]);

    // The late ack carrying the old delivery identity must not confirm anything.
    expect(() => queue.ack(consumer, 1n, t1.deliveryId, t1.attempt)).toThrowError('not_in_flight');

    // Redelivery carries a new attempt; accepting it plus 2 collapses the prefix through 3.
    const redelivery = queue.fetch(consumer, 1)[0];
    expect(redelivery.seq).toBe(1n);
    expect(redelivery.attempt).toBe(t1.attempt + 1);
    expect(() => queue.ack(consumer, 1n, t1.deliveryId, t1.attempt)).toThrowError('stale_attempt');
    queue.ack(consumer, 1n, redelivery.deliveryId, redelivery.attempt);
    const t2 = tokens.get(2n)!;
    queue.ack(consumer, 2n, t2.deliveryId, t2.attempt);
    expect(queue.tracker.watermark).toBe(3n);
    expect(queue.tracker.ahead.size).toBe(0);
  });

  it('delivery timeout requeues with a fresh attempt and the parked ack is not masked', () => {
    const t0 = 1_000;
    queue = new DeliveryQueue(freshStateFile());
    const consumer = queue.registerConsumer({deliveryTtlMs: 100, leaseTtlMs: 60_000}, t0).id;
    queue.publish('a');
    queue.publish('b');
    const first = queue.fetch(consumer, 2, t0);
    expect(first.map(d => d.seq)).toEqual([1n, 2n]);
    const d1 = first[0];
    queue.ack(consumer, 2n, first[1].deliveryId, first[1].attempt, t0);

    // After the delivery deadline, 1 is reclaimed; 2 stays parked, not committed.
    const result = queue.sweep(t0 + 101);
    expect(result.timedOut).toEqual([1n]);
    expect(queue.tracker.watermark).toBe(0n);
    expect([...queue.tracker.ahead]).toEqual([2n]);

    const redelivery = queue.fetch(consumer, 1, t0 + 101)[0];
    expect(redelivery.seq).toBe(1n);
    expect(redelivery.attempt).toBe(2);
    expect(() => queue.ack(consumer, 1n, d1.deliveryId, d1.attempt, t0 + 101)).toThrowError('stale_attempt');
    queue.ack(consumer, 1n, redelivery.deliveryId, redelivery.attempt, t0 + 101);
    expect(queue.tracker.watermark).toBe(2n);
  });

  it('lease loss fences the consumer, requeues all its deliveries, and explicit cancel does the same', () => {
    const t0 = 5_000;
    queue = new DeliveryQueue(freshStateFile());
    const consumerId = queue.registerConsumer({leaseTtlMs: 100, deliveryTtlMs: 60_000}, t0).id;
    for (let i = 1; i <= 4; i++) queue.publish(`m${i}`);
    const deliveries = queue.fetch(consumerId, 4, t0);
    const d = (seq: number) => deliveries.find(value => value.seq === BigInt(seq))!;
    queue.ack(consumerId, 2n, d(2).deliveryId, d(2).attempt, t0); // parked

    // Lease expires: 1,3,4 come back and any operation by the consumer is fenced.
    queue.sweep(t0 + 101);
    expect(() => queue.heartbeat(consumerId, undefined, t0 + 101)).toThrowError('lease_lost');
    expect(() => queue.ack(consumerId, 1n, d(1).deliveryId, d(1).attempt, t0 + 101)).toThrowError('lease_lost');
    expect(queue.tracker.watermark).toBe(0n);
    expect([...queue.tracker.ahead]).toEqual([2n]);

    // A new consumer recovers exactly the gap messages (not 2, which is parked).
    const next = queue.registerConsumer({leaseTtlMs: 1_000}, t0 + 101);
    const recovered = queue.fetch(next.id, 10, t0 + 101).map(value => value.seq);
    expect(recovered).toEqual([1n, 3n, 4n]);
    queue.releaseConsumer(next.id); // explicit cancel
    const again = queue.fetch(queue.registerConsumer({}, t0 + 200).id, 10, t0 + 200).map(value => value.seq);
    expect(again).toEqual([1n, 3n, 4n]);
  });

  it('batch ack accepts the valid subset, reports duplicates, and collapses once at the end', () => {
    const consumer = queue.registerConsumer({maxInFlight: 1000}).id;
    const tokens = seed(queue, 6, consumer);
    const item = (seq: number) => {
      const token = tokens.get(BigInt(seq))!;
      return {seq: BigInt(seq), deliveryId: token.deliveryId, attempt: token.attempt};
    };

    // Ack 1,2,4 ahead of 3; then repeat 1, and include an unknown seq 99.
    const result = queue.batchAck(consumer, [item(1), item(2), item(4), item(1), {
      seq: 99n,
      deliveryId: 'nope',
      attempt: 1,
    }]);
    expect(result.watermark).toBe(2n);
    const forSeq = (n: string) => result.results.filter(r => r.seq.toString() === n);
    expect(forSeq('1').map(r => r.status)).toEqual(['ok', 'duplicate']);
    expect(forSeq('2')[0]?.status).toBe('ok');
    expect(forSeq('4')[0]?.status).toBe('ok');
    expect(forSeq('99')[0]?.ok).toBe(false);
    expect(forSeq('99')[0]?.error).toBe('not_in_flight');
    expect([...queue.tracker.ahead]).toEqual([4n]);

    // Closing the batch with 3 then 5,6 collapses the full prefix.
    const tail = queue.batchAck(consumer, [item(3), item(5), item(6)]);
    expect(tail.watermark).toBe(6n);
  });

  it('batch nack requeues items and later batch acks redelivered attempts', () => {
    const consumer = queue.registerConsumer({leaseTtlMs: 60_000, deliveryTtlMs: 60_000}).id;
    const tokens = seed(queue, 3, consumer);
    const item = (seq: number) => {
      const token = tokens.get(BigInt(seq))!;
      return {seq: BigInt(seq), deliveryId: token.deliveryId, attempt: token.attempt};
    };
    const nacked = queue.batchNack(consumer, [item(1), item(2)]);
    expect(nacked.results.every(r => r.ok)).toBe(true);
    expect(queue.tracker.isConfirmed(1n)).toBe(false);
    const redelivered = queue.fetch(consumer, 10).sort((a, b) => (a.seq < b.seq ? -1 : a.seq > b.seq ? 1 : 0));
    expect(redelivered.map(d => d.seq)).toEqual([1n, 2n]);
    const result = queue.batchAck(
      consumer,
      redelivered.map(d => ({seq: d.seq, deliveryId: d.deliveryId, attempt: d.attempt})),
    );
    expect(result.watermark).toBe(2n);
  });

  it('restart recovery: durable prefix and parked acks survive, in-flight does not', () => {
    const stateFile = freshStateFile();
    let q = new DeliveryQueue(stateFile);
    const c1 = q.registerConsumer({maxInFlight: 1000}).id;
    const tokens = seed(q, 5, c1);
    const ack = (n: number) => {
      const t = tokens.get(BigInt(n))!;
      q.ack(c1, BigInt(n), t.deliveryId, t.attempt);
    };
    ack(1);
    ack(2);
    ack(5); // parked ahead of gaps 3,4
    expect(q.tracker.watermark).toBe(2n);
    expect([...q.tracker.ahead]).toEqual([5n]);

    // Simulate a process crash: brand-new instance over the same snapshot file.
    q = new DeliveryQueue(stateFile);
    expect(q.tracker.watermark).toBe(2n);
    expect([...q.tracker.ahead]).toEqual([5n]);

    // 3 and 4 MUST be recoverable; 1 and 2 MUST never redeliver.
    const c2 = q.registerConsumer({maxInFlight: 1000}).id;
    const recovered = q.fetch(c2, 10);
    expect(recovered.map(d => d.seq)).toEqual([3n, 4n]);

    for (const delivery of recovered) {
      q.ack(c2, delivery.seq, delivery.deliveryId, delivery.attempt);
    }
    expect(q.tracker.watermark).toBe(5n);

    // Another restart keeps the committed prefix committed.
    const q3 = new DeliveryQueue(stateFile);
    expect(q3.tracker.watermark).toBe(5n);
    expect(q3.fetch(q3.registerConsumer({maxInFlight: 1000}).id, 10)).toEqual([]);
  });

  it('restart recovery fences stale delivery attempts carried by a crashed client', () => {
    const stateFile = freshStateFile();
    let q = new DeliveryQueue(stateFile);
    const c1 = q.registerConsumer({maxInFlight: 1000}).id;
    q.publish('only');
    const delivery = q.fetch(c1, 1)[0];
    // Client crashes holding (deliveryId, attempt=1) before acking.
    q = new DeliveryQueue(stateFile);
    const c2 = q.registerConsumer({maxInFlight: 1000}).id;
    const redelivery = q.fetch(c2, 1)[0];
    expect(redelivery.seq).toBe(1n);
    expect(redelivery.attempt).toBe(2);
    expect(() => q.ack(c2, 1n, delivery.deliveryId, delivery.attempt)).toThrowError('stale_attempt');
    q.ack(c2, 1n, redelivery.deliveryId, redelivery.attempt);
    expect(q.tracker.watermark).toBe(1n);
  });

  it('sparse publishes with a huge numeric gap advance in one jump, never hiding live gaps', () => {
    const queue2 = new DeliveryQueue(freshStateFile());
    const consumer = queue2.registerConsumer({maxInFlight: 1000}).id;
    queue2.publish('first', 1n);
    const huge = 1_000_000_000n;
    queue2.publish('way-ahead', huge);

    const deliveries = new Map(queue2.fetch(consumer, 10).map(d => [d.seq, d]));
    expect([...deliveries.keys()]).toEqual([1n, huge]);
    const start = Date.now();
    queue2.ack(consumer, huge, deliveries.get(huge)!.deliveryId, deliveries.get(huge)!.attempt);
    expect(Date.now() - start).toBeLessThan(100);
    expect(queue2.tracker.watermark).toBe(0n); // live message 1 blocks the jump

    queue2.ack(consumer, 1n, deliveries.get(1n)!.deliveryId, deliveries.get(1n)!.attempt);
    expect(queue2.tracker.watermark).toBe(huge);
    expect(queue2.fetch(consumer, 10)).toEqual([]);
  });

  it('a nack below a parked far-away ack is never masked by the watermark', () => {
    const queue2 = new DeliveryQueue(freshStateFile());
    const consumer = queue2.registerConsumer({maxInFlight: 1000, leaseTtlMs: 60_000, deliveryTtlMs: 60_000}).id;
    queue2.publish('near', 1n);
    queue2.publish('far', 1_000_000n);
    const deliveries = new Map(queue2.fetch(consumer, 10).map(d => [d.seq, d]));

    const far = deliveries.get(1_000_000n)!;
    queue2.ack(consumer, far.seq, far.deliveryId, far.attempt);
    expect(queue2.tracker.watermark).toBe(0n); // live message 1 blocks the jump
    expect([...queue2.tracker.ahead]).toEqual([1_000_000n]);

    // Nack the near message: the parked far ack must not mask it, nor commit.
    const near = deliveries.get(1n)!;
    queue2.nack(consumer, near.seq, near.deliveryId, near.attempt);
    expect(queue2.tracker.isConfirmed(1n)).toBe(false);
    expect(queue2.tracker.isConfirmed(1_000_000n)).toBe(true);
    expect(queue2.tracker.watermark).toBe(0n);
    const state = queue2.snapshotState();
    expect(state.deliverable).toEqual([1n]);
    expect(state.pendingAhead).toEqual([1_000_000n]);
    expect(state.gaps.map(g => `${g.from}-${g.to}`)).toEqual(['1-1']);

    // Redeliver and accept near; the watermark leaps the hole in one jump to far.
    const redelivery = queue2.fetch(consumer, 1)[0];
    expect(redelivery.seq).toBe(1n);
    expect(redelivery.attempt).toBe(2);
    queue2.ack(consumer, redelivery.seq, redelivery.deliveryId, redelivery.attempt);
    expect(queue2.tracker.watermark).toBe(1_000_000n);
    expect(queue2.tracker.ahead.size).toBe(0);
  });

  it('after a sparse publish, sequence numbers below the gap can never be born', () => {
    const queue2 = new DeliveryQueue(freshStateFile());
    queue2.publish('far', 1_000n);
    expect(() => queue2.publish('late', 500n)).toThrowError('sequence_already_used');
    expect(queue2.publish('next').seq).toBe(1_001n);
  });

  it('exposes gap ranges instead of a single maximum on inspection', () => {
    const consumer = queue.registerConsumer({maxInFlight: 1000}).id;
    const tokens = seed(queue, 12, consumer);
    for (const seq of [1, 2, 4, 7, 12]) {
      const token = tokens.get(BigInt(seq))!;
      queue.ack(consumer, BigInt(seq), token.deliveryId, token.attempt);
    }
    const state = queue.snapshotState();
    expect(state.watermark).toBe(2n);
    expect(state.pendingAhead.map(String)).toEqual(['4', '7', '12']);
    expect(state.gaps.map(g => `${g.from}-${g.to}`)).toEqual(['3-3', '5-6', '8-11']);
    expect(state.blockingGap).toBe(3n);
  });

  it('never delivers inside the committed prefix, even after sweep churn', () => {
    const consumer = queue.registerConsumer({maxInFlight: 1000}).id;
    const tokens = seed(queue, 4, consumer);
    for (const seq of [1, 2, 3, 4]) {
      const token = tokens.get(BigInt(seq))!;
      queue.ack(consumer, BigInt(seq), token.deliveryId, token.attempt);
    }
    queue.sweep(Date.now() + 10_000_000);
    expect(queue.fetch(queue.registerConsumer({maxInFlight: 1000}).id, 100)).toEqual([]);
    expect(queue.tracker.watermark).toBe(4n);
  });
});
