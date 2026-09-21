export interface GapRange {
  from: string;
  to: string;
}

export interface InFlightRow {
  seq: string;
  deliveryId: string;
  consumerId: string;
  attempt: number;
  deadlineUntil: number;
  leaseUntil: number | null;
}

export interface ConsumerRow {
  id: string;
  leaseUntil: number;
  leaseTtlMs: number;
  deliveryTtlMs: number;
  maxInFlight: number;
  alive: boolean;
}

export interface MessageRow {
  seq: string;
  body: string;
  publishedAt: number;
}

export interface QueueState {
  watermark: string;
  nextSeq: string;
  deliverable: string[];
  pendingAhead: string[];
  gaps: GapRange[];
  gapsTruncated: boolean;
  blockingGap: string | null;
  messages: MessageRow[];
  inFlight: InFlightRow[];
  consumers: ConsumerRow[];
}

export interface Delivery extends InFlightRow {
  body: string;
}

async function call<T>(path: string, init?: RequestInit & {json?: unknown}): Promise<T> {
  const response = await fetch(path, {
    method: init?.method ?? 'GET',
    headers: init?.json === undefined ? init?.headers : {'content-type': 'application/json', ...init?.headers},
    body: init?.json === undefined ? init?.body : JSON.stringify(init.json),
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) throw new Error(data?.error ?? `http_${response.status}`);
  return data as T;
}

export const api = {
  state: () => call<QueueState>('/api/state'),
  publish: (body: string, seq?: string) =>
    call<{seq: string}>('/api/messages', {method: 'POST', json: {body, ...(seq ? {seq} : {})}}),
  register: (options: {leaseTtlMs: number; deliveryTtlMs: number; maxInFlight: number}) =>
    call<{consumerId: string; leaseUntil: number}>('/api/consumers', {method: 'POST', json: options}),
  heartbeat: (consumerId: string) =>
    call<{leaseUntil: number}>('/api/consumers/heartbeat', {
      method: 'POST',
      headers: {'x-consumer-id': consumerId},
      json: {},
    }),
  cancel: (consumerId: string) =>
    call<{requeued: string[]}>('/api/consumers/cancel', {
      method: 'POST',
      headers: {'x-consumer-id': consumerId},
      json: {},
    }),
  fetch: (consumerId: string, maxMessages: number) =>
    call<{deliveries: Delivery[]}>('/api/fetch', {
      method: 'POST',
      headers: {'x-consumer-id': consumerId},
      json: {maxMessages},
    }),
  ack: (consumerId: string, item: {seq: string; deliveryId: string; attempt: number}) =>
    call<{watermark: string; status: string}>('/api/ack', {
      method: 'POST',
      headers: {'x-consumer-id': consumerId},
      json: item,
    }),
  nack: (consumerId: string, item: {seq: string; deliveryId: string; attempt: number}) =>
    call<{requeued: boolean}>('/api/nack', {
      method: 'POST',
      headers: {'x-consumer-id': consumerId},
      json: item,
    }),
  batchAck: (consumerId: string, items: {seq: string; deliveryId: string; attempt: number}[]) =>
    call<{watermark: string; results: {seq: string; ok: boolean; error?: string}[]}>('/api/ack/batch', {
      method: 'POST',
      headers: {'x-consumer-id': consumerId},
      json: {items},
    }),
  batchNack: (consumerId: string, items: {seq: string; deliveryId: string; attempt: number}[]) =>
    call<{results: {seq: string; ok: boolean; error?: string}[]}>('/api/nack/batch', {
      method: 'POST',
      headers: {'x-consumer-id': consumerId},
      json: {items},
    }),
  restart: () => call<{restarted: boolean}>('/api/admin/restart', {method: 'POST', json: {}}),
};
