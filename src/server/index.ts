import express, {type Express, type Request, type Response} from 'express';
import {fileURLToPath} from 'node:url';
import {join} from 'node:path';
import {DeliveryQueue, LeaseError} from './queue';

export interface AppOptions {
  stateFile?: string | null;
  /** Deterministic clock, mainly for tests. */
  now?: () => number;
}

const ERROR_STATUS: Record<string, number> = {
  unknown_consumer: 404,
  lease_lost: 410,
  not_in_flight: 409,
  stale_attempt: 409,
  sequence_already_used: 409,
};

function seq(value: unknown): bigint {
  if (typeof value === 'number' && Number.isSafeInteger(value)) return BigInt(value);
  if (typeof value === 'string' && /^\d+$/.test(value)) return BigInt(value);
  throw new LeaseError('invalid_seq');
}

export function createApp(options: AppOptions = {}): Express {
  const stateFile = options.stateFile === undefined ? join(process.cwd(), '.delivery-state.json') : options.stateFile;
  const queue = new DeliveryQueue(stateFile);
  const now = options.now ?? (() => Date.now());

  const app = express();
  app.use(express.json({limit: '1mb'}));
  app.locals.queue = queue;

  function fail(res: Response, error: unknown): void {
    if (error instanceof LeaseError) {
      const code = error.code;
      const status = ERROR_STATUS[code] ?? 400;
      res.status(status).json({error: code});
      return;
    }
    res.status(400).json({error: 'bad_request'});
  }

  function consumerId(req: Request): string {
    const id = req.header('x-consumer-id');
    if (!id) throw new LeaseError('unknown_consumer');
    return id;
  }

  function requireAttempt(req: Request): {seq: bigint; deliveryId: string; attempt: number} {
    const body = req.body ?? {};
    if (typeof body.deliveryId !== 'string' || !body.deliveryId) throw new LeaseError('stale_attempt');
    const attempt = Number(body.attempt);
    if (!Number.isSafeInteger(attempt) || attempt <= 0) throw new LeaseError('stale_attempt');
    return {seq: seq(body.seq), deliveryId: body.deliveryId, attempt};
  }

  // ---------- publishing ----------

  app.post('/api/messages', (req, res) => {
    try {
      const body = req.body ?? {};
      const explicitSeq = body.seq === undefined || body.seq === null ? undefined : seq(body.seq);
      const message = queue.publish(String(body.body ?? ''), explicitSeq, now());
      res.status(201).json({seq: message.seq.toString(), body: message.body, publishedAt: message.publishedAt});
    } catch (error) {
      fail(res, error);
    }
  });

  // ---------- consumers / leases ----------

  app.post('/api/consumers', (req, res) => {
    const body = req.body ?? {};
    const consumer = queue.registerConsumer(
      {leaseTtlMs: body.leaseTtlMs, deliveryTtlMs: body.deliveryTtlMs, maxInFlight: body.maxInFlight},
      now(),
    );
    res.status(201).json({
      consumerId: consumer.id,
      leaseUntil: consumer.leaseUntil,
      leaseTtlMs: consumer.leaseTtlMs,
      deliveryTtlMs: consumer.deliveryTtlMs,
      maxInFlight: consumer.maxInFlight,
    });
  });

  app.post('/api/consumers/heartbeat', (req, res) => {
    try {
      const consumer = queue.heartbeat(consumerId(req), req.body?.leaseTtlMs, now());
      res.json({leaseUntil: consumer.leaseUntil, leaseTtlMs: consumer.leaseTtlMs});
    } catch (error) {
      fail(res, error);
    }
  });

  // Explicit cancel: gives the lease up; every in-flight message is re-deliverable.
  app.post('/api/consumers/cancel', (req, res) => {
    try {
      const {requeued} = queue.releaseConsumer(consumerId(req));
      res.json({requeued: requeued.map(value => value.toString())});
    } catch (error) {
      fail(res, error);
    }
  });

  // ---------- fetch / ack / nack ----------

  app.post('/api/fetch', (req, res) => {
    try {
      const maxMessages = Number(req.body?.maxMessages ?? 1);
      const deliveries = queue.fetch(consumerId(req), maxMessages, now());
      res.json({
        deliveries: deliveries.map(delivery => ({
          seq: delivery.seq.toString(),
          body: delivery.body,
          deliveryId: delivery.deliveryId,
          attempt: delivery.attempt,
          deadlineUntil: delivery.deadlineUntil,
          leaseUntil: delivery.leaseUntil,
        })),
      });
    } catch (error) {
      fail(res, error);
    }
  });

  app.post('/api/ack', (req, res) => {
    try {
      const result = queue.ack(
        consumerId(req),
        requireAttempt(req).seq,
        requireAttempt(req).deliveryId,
        requireAttempt(req).attempt,
        now(),
      );
      res.json({seq: result.seq.toString(), status: result.status, watermark: queue.tracker.watermark.toString()});
    } catch (error) {
      fail(res, error);
    }
  });

  app.post('/api/nack', (req, res) => {
    try {
      const target = requireAttempt(req);
      const result = queue.nack(consumerId(req), target.seq, target.deliveryId, target.attempt, now());
      res.json({seq: result.seq.toString(), requeued: true});
    } catch (error) {
      fail(res, error);
    }
  });

  app.post('/api/ack/batch', (req, res) => {
    try {
      const items = (Array.isArray(req.body?.items) ? req.body.items : []).map((item: unknown) => {
        const value = item as {seq: unknown; deliveryId: unknown; attempt: unknown};
        if (typeof value.deliveryId !== 'string' || !Number.isSafeInteger(Number(value.attempt))) {
          throw new LeaseError('stale_attempt');
        }
        return {seq: seq(value.seq), deliveryId: value.deliveryId, attempt: Number(value.attempt)};
      });
      const result = queue.batchAck(consumerId(req), items, now());
      res.json({
        watermark: result.watermark.toString(),
        results: result.results.map(item => ({...item, seq: item.seq.toString()})),
      });
    } catch (error) {
      fail(res, error);
    }
  });

  app.post('/api/nack/batch', (req, res) => {
    try {
      const items = (Array.isArray(req.body?.items) ? req.body.items : []).map((item: unknown) => {
        const value = item as {seq: unknown; deliveryId: unknown; attempt: unknown};
        if (typeof value.deliveryId !== 'string' || !Number.isSafeInteger(Number(value.attempt))) {
          throw new LeaseError('stale_attempt');
        }
        return {seq: seq(value.seq), deliveryId: value.deliveryId, attempt: Number(value.attempt)};
      });
      const result = queue.batchNack(consumerId(req), items, now());
      res.json({results: result.results.map(item => ({...item, seq: item.seq.toString()}))});
    } catch (error) {
      fail(res, error);
    }
  });

  // ---------- inspection / administration ----------

  app.get('/api/state', (_req, res) => {
    const state = queue.snapshotState(now());
    res.json({
      watermark: state.watermark.toString(),
      nextSeq: state.nextSeq.toString(),
      deliverable: state.deliverable.map(value => value.toString()),
      pendingAhead: state.pendingAhead.map(value => value.toString()),
      gaps: state.gaps.map(range => ({from: range.from.toString(), to: range.to.toString()})),
      gapsTruncated: state.gapsTruncated,
      blockingGap: state.blockingGap === null ? null : state.blockingGap.toString(),
      messages: state.messages.map(message => ({
        seq: message.seq.toString(),
        body: message.body,
        publishedAt: message.publishedAt,
      })),
      inFlight: state.inFlight.map(entry => ({
        ...entry,
        seq: entry.seq.toString(),
        deadlineUntil: entry.deadlineUntil,
        leaseUntil: entry.leaseUntil,
      })),
      consumers: state.consumers,
    });
  });

  // Test/dev aid: drop ephemeral state and reload from the durable snapshot.
  app.post('/api/admin/restart', (_req, res) => {
    queue.reload();
    res.json({restarted: true, watermark: queue.tracker.watermark.toString()});
  });

  return app;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.PORT ?? 4174);
  createApp().listen(port, '127.0.0.1', () => console.log(`server http://127.0.0.1:${port}`));
}
