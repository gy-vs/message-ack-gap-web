import {randomUUID} from 'node:crypto';
import {existsSync, mkdirSync, readFileSync, renameSync, writeFileSync} from 'node:fs';
import {dirname} from 'node:path';
import {AcknowledgmentTracker, type GapRange, type HoleSkip} from './tracker';

export interface ConsumerOptions {
  leaseTtlMs?: number;
  deliveryTtlMs?: number;
  maxInFlight?: number;
}

export interface Message {
  seq: bigint;
  body: string;
  publishedAt: number;
}

export interface Delivery {
  seq: bigint;
  body: string;
  deliveryId: string;
  attempt: number;
  consumerId: string;
  deadlineUntil: number;
  leaseUntil: number;
}

interface InFlightEntry {
  seq: bigint;
  deliveryId: string;
  consumerId: string;
  attempt: number;
  deadlineUntil: number;
}

interface ConsumerRecord {
  id: string;
  leaseUntil: number;
  leaseTtlMs: number;
  deliveryTtlMs: number;
  maxInFlight: number;
}

interface AckResult {
  seq: bigint;
  status: 'ok' | 'duplicate';
}

interface BatchItemResult {
  seq: bigint;
  ok: boolean;
  status?: 'ok' | 'duplicate';
  error?: string;
}

const MIN_TTL_MS = 20;
const MAX_TTL_MS = 5 * 60 * 1000;

function clampTtl(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(MAX_TTL_MS, Math.max(MIN_TTL_MS, Math.round(value)));
}

function clampInt(value: number | undefined, fallback: number, min: number, max: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.round(value)));
}

export class LeaseError extends Error {
  constructor(public code: string) {
    super(code);
  }
}

export interface QueueSnapshot {
  version: 1;
  messages: {seq: string; body: string; publishedAt: number}[];
  tracker: {watermark: string; ahead: string[]};
  attempts: Record<string, number>;
  nextSeq: string;
}

/**
 * Durable, concurrently-consumable message queue.
 *
 * Durable state (log, contiguous watermark, discrete ahead-set, per-sequence
 * attempt counters) is snapshotted to `stateFile` after every mutation.
 * In-flight deliveries and consumer leases are ephemeral: on restart they are
 * gone and every message above the committed prefix is deliverable again, so a
 * crashed process can never strand a gap message.
 */
export class DeliveryQueue {
  readonly messages = new Map<bigint, Message>();
  readonly tracker = new AcknowledgmentTracker();
  /** Per-sequence delivery attempt counter; survives restarts so stale attempts fence old clients. */
  readonly attempts = new Map<bigint, number>();
  private nextSeq = 1n;

  private readonly consumers = new Map<string, ConsumerRecord>();
  private readonly inFlight = new Map<bigint, InFlightEntry>();
  /** Sorted ascending; only contains uncommitted, non-parked sequences. */
  private readonly deliverable: bigint[] = [];
  private deliveryCounter = 0;

  constructor(private readonly stateFile: string | null = null) {
    this.restore();
  }

  // ---------- durability ----------

  private restore(): void {
    if (!this.stateFile || !existsSync(this.stateFile)) return;
    const raw = JSON.parse(readFileSync(this.stateFile, 'utf8')) as QueueSnapshot;
    for (const msg of raw.messages ?? []) {
      this.messages.set(BigInt(msg.seq), {seq: BigInt(msg.seq), body: msg.body, publishedAt: msg.publishedAt});
    }
    const restored = AcknowledgmentTracker.fromJSON(raw.tracker);
    this.tracker.watermark = restored.watermark;
    restored.ahead.forEach(seq => this.tracker.ahead.add(seq));
    for (const [seq, attempt] of Object.entries(raw.attempts ?? {})) {
      this.attempts.set(BigInt(seq), attempt);
    }
    this.nextSeq = raw.nextSeq ? BigInt(raw.nextSeq) : this.computeNextSeq();
    this.rebuildDeliverable();
  }

  /** Drop all runtime state (leases, in-flight) and reload the durable snapshot, like a process restart. */
  reload(): void {
    this.consumers.clear();
    this.inFlight.clear();
    this.deliverable.length = 0;
    this.deliveryCounter = 0;
    this.messages.clear();
    this.tracker.ahead.clear();
    this.tracker.watermark = 0n;
    this.attempts.clear();
    this.nextSeq = 1n;
    this.restore();
  }

  private computeNextSeq(): bigint {
    let max = 0n;
    for (const seq of this.messages.keys()) if (seq > max) max = seq;
    return max + 1n;
  }

  private rebuildDeliverable(): void {
    this.deliverable.length = 0;
    for (const seq of this.messages.keys()) {
      if (seq > this.tracker.watermark && !this.tracker.ahead.has(seq)) this.deliverable.push(seq);
    }
    this.deliverable.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  }

  private persist(): void {
    if (!this.stateFile) return;
    const snapshot: QueueSnapshot = {
      version: 1,
      messages: [...this.messages.values()].map(msg => ({
        seq: msg.seq.toString(),
        body: msg.body,
        publishedAt: msg.publishedAt,
      })),
      tracker: this.tracker.toJSON(),
      attempts: Object.fromEntries([...this.attempts].map(([seq, n]) => [seq.toString(), n])),
      nextSeq: this.nextSeq.toString(),
    };
    mkdirSync(dirname(this.stateFile), {recursive: true});
    const tmp = `${this.stateFile}.tmp-${process.pid}-${this.deliveryCounter}`;
    writeFileSync(tmp, JSON.stringify(snapshot));
    renameSync(tmp, this.stateFile);
  }

  // ---------- consumers / leases ----------

  registerConsumer(options: ConsumerOptions = {}, now = Date.now()): ConsumerRecord {
    const record: ConsumerRecord = {
      id: randomUUID(),
      leaseUntil: 0,
      leaseTtlMs: clampTtl(options.leaseTtlMs, 30_000),
      deliveryTtlMs: clampTtl(options.deliveryTtlMs, 60_000),
      maxInFlight: clampInt(options.maxInFlight, 10, 1, 1000),
    };
    record.leaseUntil = now + record.leaseTtlMs;
    this.consumers.set(record.id, record);
    return record;
  }

  heartbeat(consumerId: string, leaseTtlMs?: number, now = Date.now()): ConsumerRecord {
    const consumer = this.consumers.get(consumerId);
    if (!consumer) throw new LeaseError('lease_lost');
    if (consumer.leaseUntil <= now) {
      this.sweep(now);
      throw new LeaseError('lease_lost');
    }
    if (leaseTtlMs !== undefined) consumer.leaseTtlMs = clampTtl(leaseTtlMs, consumer.leaseTtlMs);
    consumer.leaseUntil = now + consumer.leaseTtlMs;
    return consumer;
  }

  /** Explicit cancel/release: all of the consumer's in-flight messages return to deliverable. */
  releaseConsumer(consumerId: string): {requeued: bigint[]} {
    const requeued: bigint[] = [];
    for (const [seq, entry] of this.inFlight) {
      if (entry.consumerId === consumerId) {
        this.inFlight.delete(seq);
        this.requeue(seq);
        requeued.push(seq);
      }
    }
    this.consumers.delete(consumerId);
    if (requeued.length) this.persist();
    return {requeued: requeued.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))};
  }

  private requireLiveConsumer(consumerId: string, now: number): ConsumerRecord {
    const consumer = this.consumers.get(consumerId);
    if (!consumer) throw new LeaseError('lease_lost');
    if (consumer.leaseUntil <= now) {
      this.sweep(now);
      throw new LeaseError('lease_lost');
    }
    return consumer;
  }

  /**
   * Reclaim everything whose lease or per-delivery deadline has elapsed.
   * Expired consumer leases fence the consumer AND requeue every one of its
   * in-flight messages, even if the individual delivery deadline is still open.
   */
  sweep(now = Date.now()): {expiredConsumers: string[]; timedOut: bigint[]} {
    const expiredConsumers: string[] = [];
    for (const [id, consumer] of this.consumers) {
      if (consumer.leaseUntil <= now) expiredConsumers.push(id);
    }
    const timedOut: bigint[] = [];
    let changed = expiredConsumers.length > 0;

    for (const id of expiredConsumers) {
      for (const [seq, entry] of this.inFlight) {
        if (entry.consumerId === id) {
          this.inFlight.delete(seq);
          this.requeue(seq);
          timedOut.push(seq);
        }
      }
      this.consumers.delete(id);
    }

    for (const [seq, entry] of this.inFlight) {
      if (entry.deadlineUntil <= now) {
        this.inFlight.delete(seq);
        this.requeue(seq);
        timedOut.push(seq);
        changed = true;
      }
    }

    if (changed) this.persist();
    return {
      expiredConsumers,
      timedOut: timedOut.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)),
    };
  }

  // ---------- publish / fetch ----------

  publish(body: string, explicitSeq?: bigint, now = Date.now()): Message {
    let seq: bigint;
    if (explicitSeq !== undefined) {
      if (explicitSeq < this.nextSeq) throw new LeaseError('sequence_already_used');
      seq = explicitSeq;
    } else {
      seq = this.nextSeq;
    }
    const message: Message = {seq, body: String(body ?? ''), publishedAt: now};
    this.messages.set(seq, message);
    this.nextSeq = seq + 1n;
    this.insertDeliverable(seq);
    this.persist();
    return message;
  }

  private insertDeliverable(seq: bigint): void {
    if (seq <= this.tracker.watermark || this.tracker.ahead.has(seq) || this.inFlight.has(seq)) return;
    let lo = 0;
    let hi = this.deliverable.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.deliverable[mid] < seq) lo = mid + 1;
      else hi = mid;
    }
    if (this.deliverable[lo] === seq) return;
    this.deliverable.splice(lo, 0, seq);
  }

  private requeue(seq: bigint): void {
    if (!this.messages.has(seq)) return;
    // Never resurrect a committed or parked-ahead confirmation.
    this.tracker.retract(seq);
    this.insertDeliverable(seq);
  }

  fetch(consumerId: string, maxMessages = 1, now = Date.now()): Delivery[] {
    this.sweep(now);
    const consumer = this.requireLiveConsumer(consumerId, now);
    const inflightForConsumer = [...this.inFlight.values()].filter(e => e.consumerId === consumerId).length;
    const capacity = Math.max(0, consumer.maxInFlight - inflightForConsumer);
    const wanted = Math.min(clampInt(maxMessages, 1, 1, consumer.maxInFlight), capacity);
    const deliveries: Delivery[] = [];
    while (deliveries.length < wanted) {
      const seq = this.deliverable.shift();
      if (seq === undefined) break;
      if (seq <= this.tracker.watermark) continue; // committed prefix is never re-delivered
      const attempt = (this.attempts.get(seq) ?? 0) + 1;
      this.attempts.set(seq, attempt);
      // Globally unique across restarts (the counter resets with the process);
      // correctness is additionally fenced by the persisted, per-seq attempt.
      const deliveryId = `d-${randomUUID().slice(0, 12)}-${seq.toString(36)}`;
      this.inFlight.set(seq, {
        seq,
        deliveryId,
        consumerId,
        attempt,
        deadlineUntil: now + consumer.deliveryTtlMs,
      });
      const message = this.messages.get(seq)!;
      deliveries.push({
        seq,
        body: message.body,
        deliveryId,
        attempt,
        consumerId,
        deadlineUntil: now + consumer.deliveryTtlMs,
        leaseUntil: consumer.leaseUntil,
      });
    }
    if (deliveries.length) this.persist();
    return deliveries;
  }

  // ---------- acks / nacks ----------

  private resolveEntry(
    consumerId: string,
    seq: bigint,
    deliveryId: string,
    attempt: number,
    now: number,
  ): InFlightEntry {
    this.requireLiveConsumer(consumerId, now);
    const entry = this.inFlight.get(seq);
    if (!entry) throw new LeaseError('not_in_flight');
    if (entry.consumerId !== consumerId || entry.deliveryId !== deliveryId || entry.attempt !== attempt) {
      throw new LeaseError('stale_attempt');
    }
    return entry;
  }

  ack(consumerId: string, seq: bigint, deliveryId: string, attempt: number, now = Date.now()): AckResult {
    this.sweep(now);
    this.requireLiveConsumer(consumerId, now);
    const entry = this.inFlight.get(seq);
    if (!entry) {
      // Ack after nack/timeout/rebalance: idempotent only if it committed earlier.
      if (this.tracker.isConfirmed(seq)) return {seq, status: 'duplicate'};
      throw new LeaseError('not_in_flight');
    }
    if (entry.consumerId !== consumerId || entry.deliveryId !== deliveryId || entry.attempt !== attempt) {
      throw new LeaseError('stale_attempt');
    }
    this.inFlight.delete(seq);
    this.tracker.confirm(seq);
    this.tracker.advance(this.holeSkip());
    this.trimCommittedDeliverable();
    this.persist();
    return {seq, status: 'ok'};
  }

  nack(consumerId: string, seq: bigint, deliveryId: string, attempt: number, now = Date.now()): {seq: bigint} {
    this.sweep(now);
    const entry = this.resolveEntry(consumerId, seq, deliveryId, attempt, now);
    this.inFlight.delete(entry.seq);
    // A nack must never leave a successful confirmation behind.
    this.tracker.retract(seq);
    this.requeue(seq);
    this.persist();
    return {seq};
  }

  batchAck(
    consumerId: string,
    items: {seq: bigint; deliveryId: string; attempt: number}[],
    now = Date.now(),
  ): {watermark: bigint; results: BatchItemResult[]} {
    this.sweep(now);
    // Lease is checked once for the whole batch; individual item failures do not abort the rest.
    this.requireLiveConsumer(consumerId, now);
    const results: BatchItemResult[] = [];
    for (const item of items) {
      const entry = this.inFlight.get(item.seq);
      if (!entry) {
        if (this.tracker.isConfirmed(item.seq)) {
          results.push({seq: item.seq, ok: true, status: 'duplicate'});
        } else {
          results.push({seq: item.seq, ok: false, error: 'not_in_flight'});
        }
        continue;
      }
      if (entry.consumerId !== consumerId || entry.deliveryId !== item.deliveryId || entry.attempt !== item.attempt) {
        results.push({seq: item.seq, ok: false, error: 'stale_attempt'});
        continue;
      }
      this.inFlight.delete(item.seq);
      this.tracker.confirm(item.seq);
      results.push({seq: item.seq, ok: true, status: 'ok'});
    }
    this.tracker.advance(this.holeSkip());
    this.trimCommittedDeliverable();
    this.persist();
    return {watermark: this.tracker.watermark, results};
  }

  /**
   * Live (uncommitted) messages = deliverable queue + in-flight set.
   * Parked-ahead confirmations are *not* live and are consumed by advance itself.
   */
  private holeSkip(): HoleSkip {
    return {
      exists: (seq: bigint) =>
        this.messages.has(seq) &&
        seq > this.tracker.watermark &&
        !this.tracker.ahead.has(seq),
      nextExistingAtOrAfter: (seq: bigint) => {
        let candidate: bigint | null = null;
        // Smallest deliverable >= seq (binary search).
        let lo = 0;
        let hi = this.deliverable.length;
        while (lo < hi) {
          const mid = (lo + hi) >> 1;
          if (this.deliverable[mid] < seq) lo = mid + 1;
          else hi = mid;
        }
        if (lo < this.deliverable.length) candidate = this.deliverable[lo];
        for (const entry of this.inFlight.values()) {
          if (entry.seq >= seq && (candidate === null || entry.seq < candidate)) candidate = entry.seq;
        }
        return candidate;
      },
    };
  }

  batchNack(
    consumerId: string,
    items: {seq: bigint; deliveryId: string; attempt: number}[],
    now = Date.now(),
  ): {results: BatchItemResult[]} {
    this.sweep(now);
    this.requireLiveConsumer(consumerId, now);
    const results: BatchItemResult[] = [];
    for (const item of items) {
      const entry = this.inFlight.get(item.seq);
      if (!entry) {
        results.push({seq: item.seq, ok: false, error: 'not_in_flight'});
        continue;
      }
      if (entry.consumerId !== consumerId || entry.deliveryId !== item.deliveryId || entry.attempt !== item.attempt) {
        results.push({seq: item.seq, ok: false, error: 'stale_attempt'});
        continue;
      }
      this.inFlight.delete(item.seq);
      this.tracker.retract(item.seq);
      this.requeue(item.seq);
      results.push({seq: item.seq, ok: true, status: 'ok'});
    }
    this.persist();
    return {results};
  }

  private trimCommittedDeliverable(): void {
    while (this.deliverable.length && this.deliverable[0] <= this.tracker.watermark) this.deliverable.shift();
  }

  // ---------- inspection ----------

  /**
   * Gap ranges among published, uncommitted messages that sit below the highest
   * discrete (out-of-order) confirmation. These are precisely the holes that
   * keep parked confirmations from collapsing into the prefix.
   * O(k log k) in parked/uncommitted count — never in numeric gap size.
   */
  gapRanges(limit = 32): {ranges: GapRange[]; truncated: boolean} {
    if (this.tracker.ahead.size === 0) return {ranges: [], truncated: false};
    let ceiling = this.tracker.watermark;
    for (const seq of this.tracker.ahead) if (seq > ceiling) ceiling = seq;
    const missing: bigint[] = [];
    for (const seq of this.messages.keys()) {
      if (seq > this.tracker.watermark && seq <= ceiling && !this.tracker.ahead.has(seq)) missing.push(seq);
    }
    missing.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const ranges: GapRange[] = [];
    for (const seq of missing) {
      const last = ranges[ranges.length - 1];
      if (last && seq === last.to + 1n) last.to = seq;
      else {
        if (ranges.length >= limit) return {ranges, truncated: true};
        ranges.push({from: seq, to: seq});
      }
    }
    return {ranges, truncated: false};
  }

  snapshotState(now = Date.now()) {
    this.sweep(now);
    const {ranges, truncated} = this.gapRanges();
    const parked = [...this.tracker.ahead].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    // Smallest live (uncommitted) message: deliverable head or earliest in-flight.
    let blockingGap: bigint | null = this.deliverable[0] ?? null;
    for (const entry of this.inFlight.values()) {
      if (blockingGap === null || entry.seq < blockingGap) blockingGap = entry.seq;
    }
    return {
      watermark: this.tracker.watermark,
      nextSeq: this.nextSeq,
      deliverable: [...this.deliverable],
      pendingAhead: parked,
      gaps: ranges,
      gapsTruncated: truncated,
      blockingGap,
      messages: [...this.messages.values()].sort((a, b) => (a.seq < b.seq ? -1 : a.seq > b.seq ? 1 : 0)),
      inFlight: [...this.inFlight.values()]
        .sort((a, b) => (a.seq < b.seq ? -1 : a.seq > b.seq ? 1 : 0))
        .map(entry => ({...entry, leaseUntil: this.consumers.get(entry.consumerId)?.leaseUntil ?? null})),
      consumers: [...this.consumers.values()].map(consumer => ({
        id: consumer.id,
        leaseUntil: consumer.leaseUntil,
        leaseTtlMs: consumer.leaseTtlMs,
        deliveryTtlMs: consumer.deliveryTtlMs,
        maxInFlight: consumer.maxInFlight,
        alive: consumer.leaseUntil > now,
      })),
    };
  }
}
