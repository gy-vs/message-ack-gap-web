/**
 * 消息交付状态机
 *
 * 核心不变量:
 *  - committedSeq 是「连续已确认前缀」的右端水位。seq <= committedSeq 的消息
 *    永远不会再交付,即使崩溃重启也保持该性质(随快照持久化)。
 *  - ackedAhead 是「前方离散确认集合」:seq > committedSeq 且已被成功确认,
 *    但因为前面还有缺口而暂时不能并入前缀。只有当缺口从前往后连续补齐时,
 *    水位才逐格推进;乱序确认(例如 12 先于 11)不会移动水位。
 *  - 每条在途消息记录 consumerId + attempt,ack/nack/cancel 必须同时匹配,
 *    旧 attempt 的迟到确认一律拒绝,不能顶替或掩盖当前交付。
 *  - nack / 超时 / 取消只是把消息从在途集合放回可交付集合(对同一消费者
 *    遵守可见性冷却),绝不保留任何「成功确认」痕迹。
 */

export type DeliveryErrorReason =
  | 'unknown_consumer'
  | 'lease_lost'
  | 'unknown_message'
  | 'committed'
  | 'not_in_flight'
  | 'stale_attempt'
  | 'foreign_consumer'
  | 'invalid_body';

export class DeliveryError extends Error {
  readonly status = 409;
  constructor(readonly reason: DeliveryErrorReason, message?: string) {
    super(message ?? reason);
  }
}

export type InFlight = {
  seq: number;
  consumerId: string;
  attempt: number;
  /** 本次交付的时间;attempt 超时以此为基准 */
  deliveredAt: number;
  /** 若为 nack 重投,记录之前消费者可见性冷却解除时刻 */
  visibleAt: number;
};

export type Consumer = {
  id: string;
  leaseExpiresAt: number;
};

export type EventKind =
  | 'publish'
  | 'deliver'
  | 'ack'
  | 'nack'
  | 'timeout'
  | 'cancel'
  | 'lease_lost'
  | 'watermark'
  | 'restart';

export type DeliveryEvent = {
  id: number;
  at: number;
  kind: EventKind;
  seq?: number;
  consumerId?: string;
  attempt?: number;
  detail?: string;
};

export type Snapshot = {
  version: 1;
  nextSeq: number;
  committedSeq: number;
  ackedAhead: number[];
  /** 可交付但尚未并入已确认前缀的序号(小顶堆数组) */
  availableHeap: number[];
  /** 崩溃瞬间仍在途的消息恢复后重新可交付,因此只需持久化 attempt 计数 */
  attempts: Record<string, number>;
  maxEventId: number;
};

export type StoreOptions = {
  /** 可注入的时钟,便于测试超时与租约 */
  now?: () => number;
  /** 快照持久化;不提供则为纯内存存储,restart 等价于清空 */
  persistence?: {load(): Snapshot | null; save(snapshot: Snapshot): void};
  /** 事件环形缓冲容量 */
  eventLimit?: number;
};

/**
 * 可交付序号的小顶堆。只有「从未确认」的消息会留在这里:
 * ack 成功即永久离开,nack/超时/取消才会重新入堆,因此不存在被水位掩盖的问题。
 */
class MinHeap {
  private data: number[] = [];
  constructor(init: number[] = []) {
    this.data = [...init];
    for (let i = (this.data.length >> 1) - 1; i >= 0; i--) this.siftDown(i);
  }
  get size(): number {
    return this.data.length;
  }
  has(value: number): boolean {
    return this.data.includes(value);
  }
  toJSON(): number[] {
    return this.data;
  }
  push(value: number): void {
    this.data.push(value);
    this.siftUp(this.data.length - 1);
  }
  /**
   * 取出最小的未被 blocked 判定为冷却的序号。
   * 做法:反复弹出堆顶收集被阻塞的序号,找到就绪者后把暂存项全部放回。
   * 放回后重新建堆,保证堆序不被破坏(被阻塞的序号在冷却结束前会一直留在堆里)。
   */
  popUntil(blocked: (seq: number) => boolean): number | null {
    const deferred: number[] = [];
    let result: number | null = null;
    while (this.data.length > 0) {
      const top = this.popRaw();
      if (!blocked(top)) {
        result = top;
        break;
      }
      deferred.push(top);
    }
    if (deferred.length > 0) {
      this.data.push(...deferred);
      for (let i = (this.data.length >> 1) - 1; i >= 0; i--) this.siftDown(i);
    }
    return result;
  }
  private popRaw(): number {
    const top = this.data[0];
    const last = this.data.pop()!;
    if (this.data.length > 0) {
      this.data[0] = last;
      this.siftDown(0);
    }
    return top;
  }
  private siftUp(i: number): void {
    const v = this.data[i];
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.data[parent] <= v) break;
      this.data[i] = this.data[parent];
      i = parent;
    }
    this.data[i] = v;
  }
  private siftDown(i: number): void {
    const n = this.data.length;
    const v = this.data[i];
    while (true) {
      const l = i * 2 + 1;
      if (l >= n) break;
      const r = l + 1;
      let child = l;
      if (r < n && this.data[r] < this.data[l]) child = r;
      if (v <= this.data[child]) break;
      this.data[i] = this.data[child];
      i = child;
    }
    this.data[i] = v;
  }
}

export class DeliveryStore {
  private now: () => number;
  private persistence?: {load(): Snapshot | null; save(snapshot: Snapshot): void};
  private nextSeq = 1;
  private committedSeq = 0;
  private ackedAhead = new Set<number>();
  private available: MinHeap;
  /** seq -> 累计交付次数,重启恢复时保留以便 attempt 单调递增 */
  private attempts = new Map<number, number>();
  private inFlight = new Map<number, InFlight>();
  /** "seq:consumerId" -> nack 后对该消费者的可见性冷却解除时刻(其他消费者不受限) */
  private cooldown = new Map<string, number>();
  private consumers = new Map<string, Consumer>();
  private events: DeliveryEvent[] = [];
  private maxEventId = 0;
  private readonly eventLimit: number;

  constructor(options: StoreOptions = {}) {
    this.now = options.now ?? (() => Date.now());
    this.persistence = options.persistence;
    this.eventLimit = options.eventLimit ?? 200;
    this.available = new MinHeap();
    const snapshot = this.persistence?.load() ?? null;
    if (snapshot) this.restore(snapshot);
  }

  // ---------------------------------------------------------------- 发布

  publish(count = 1): number[] {
    if (!Number.isInteger(count) || count < 1 || count > 100_000) {
      throw new DeliveryError('invalid_body', 'count must be an integer in [1, 100000]');
    }
    const seqs: number[] = [];
    for (let i = 0; i < count; i++) {
      const seq = this.nextSeq++;
      this.available.push(seq);
      seqs.push(seq);
    }
    this.log('publish', {detail: `+${count}`});
    this.persist();
    return seqs;
  }

  // ---------------------------------------------------------------- 租约

  registerConsumer(id: string, leaseMs = 30_000): Consumer {
    const consumer: Consumer = {id, leaseExpiresAt: this.now() + leaseMs};
    this.consumers.set(id, consumer);
    return consumer;
  }

  renewLease(consumerId: string, leaseMs = 30_000): Consumer {
    const consumer = this.requireActiveConsumer(consumerId);
    consumer.leaseExpiresAt = this.now() + leaseMs;
    return consumer;
  }

  private requireActiveConsumer(consumerId: string): Consumer {
    const consumer = this.consumers.get(consumerId);
    if (!consumer) throw new DeliveryError('unknown_consumer');
    if (consumer.leaseExpiresAt <= this.now()) throw new DeliveryError('lease_lost');
    return consumer;
  }

  // ---------------------------------------------------------------- 交付

  /**
   * 取走至多 maxMessages 条可交付消息。
   * 已提交前缀中的消息永远不会出现;nack 回退的消息在冷却期对原消费者不可见,
   * 但其他消费者立即可见。
   */
  deliver(consumerId: string, maxMessages = 1, attemptTimeoutMs = 60_000): InFlight[] {
    this.requireActiveConsumer(consumerId);
    if (!Number.isInteger(maxMessages) || maxMessages < 1) {
      throw new DeliveryError('invalid_body', 'maxMessages must be a positive integer');
    }
    const result: InFlight[] = [];
    for (let i = 0; i < maxMessages; i++) {
      const seq = this.available.popUntil(candidate => {
        const until = this.cooldown.get(`${candidate}:${consumerId}`);
        if (until === undefined) return false;
        if (until <= this.now()) {
          this.cooldown.delete(`${candidate}:${consumerId}`);
          return false;
        }
        return true;
      });
      if (seq === null) break;
      this.cooldown.delete(`${seq}:${consumerId}`);
      const attempt = (this.attempts.get(seq) ?? 0) + 1;
      this.attempts.set(seq, attempt);
      const flight: InFlight = {
        seq,
        consumerId,
        attempt,
        deliveredAt: this.now(),
        visibleAt: this.now() + attemptTimeoutMs,
      };
      this.inFlight.set(seq, flight);
      result.push(flight);
      this.log('deliver', {seq, consumerId, attempt});
    }
    this.persist();
    return result;
  }

  // ---------------------------------------------------------------- 确认

  /**
   * 成功确认。只有「当前 attempt + 当前持有消费者」的 ack 有效:
   *  - 已并入前缀的消息再 ack => committed(幂等成功语义由 batch 层处理)
   *  - 不属于该消费者 / attempt 不匹配 => 拒绝,迟到确认不能顶替新交付
   * 水位仅在缺口从前往后连续补齐时逐格推进。
   */
  ack(consumerId: string, seq: number, attempt: number): {advancedTo: number; duplicated: boolean} {
    this.requireActiveConsumer(consumerId);
    this.requireKnown(seq);
    // 已并入连续前缀:重复 ack 幂等成功,前缀内消息绝不会因此重投
    if (seq <= this.committedSeq) return {advancedTo: this.committedSeq, duplicated: true};
    const flight = this.inFlight.get(seq);
    if (!flight) {
      // 在前方离散集合里:重复 ack 视为幂等成功(已经确认过且未并入前缀)
      if (this.ackedAhead.has(seq)) return {advancedTo: this.committedSeq, duplicated: true};
      throw new DeliveryError('not_in_flight');
    }
    if (flight.consumerId !== consumerId) throw new DeliveryError('foreign_consumer');
    if (flight.attempt !== attempt) throw new DeliveryError('stale_attempt');

    this.inFlight.delete(seq);
    this.ackedAhead.add(seq);
    this.log('ack', {seq, consumerId, attempt});

    const before = this.committedSeq;
    while (this.ackedAhead.delete(this.committedSeq + 1)) this.committedSeq++;
    if (this.committedSeq > before) {
      this.log('watermark', {seq: this.committedSeq, detail: `committed prefix -> ${this.committedSeq}`});
    }
    this.persist();
    return {advancedTo: this.committedSeq, duplicated: false};
  }

  /**
   * 批量确认:逐元素应用相同的 attempt 绑定规则。
   * 任一元素非法则整批拒绝(原子语义),不会出现部分确认掩盖失败的情况。
   */
  ackBatch(
    consumerId: string,
    items: ReadonlyArray<{seq: number; attempt: number}>,
  ): {advancedTo: number; acked: number[]; duplicated: number[]} {
    this.requireActiveConsumer(consumerId);
    if (!Array.isArray(items) || items.length === 0) {
      throw new DeliveryError('invalid_body', 'acks must be a non-empty array');
    }
    // 预检:全部合法才执行(原子批量,不允许部分确认掩盖失败)
    for (const item of items) {
      const seq = item?.seq;
      const attempt = item?.attempt;
      if (!Number.isInteger(seq) || !Number.isInteger(attempt)) {
        throw new DeliveryError('invalid_body', 'each ack needs integer seq and attempt');
      }
      this.requireKnown(seq);
      if (seq <= this.committedSeq) continue; // 已提交前缀:幂等,跳过
      const flight = this.inFlight.get(seq);
      if (!flight) {
        if (!this.ackedAhead.has(seq)) throw new DeliveryError('not_in_flight');
        continue;
      }
      if (flight.consumerId !== consumerId) throw new DeliveryError('foreign_consumer');
      if (flight.attempt !== attempt) throw new DeliveryError('stale_attempt');
    }
    const acked: number[] = [];
    const duplicated: number[] = [];
    for (const item of items) {
      const {seq, attempt} = item;
      if (seq <= this.committedSeq || this.ackedAhead.has(seq)) {
        duplicated.push(seq);
        continue;
      }
      this.inFlight.delete(seq);
      this.ackedAhead.add(seq);
      acked.push(seq);
      this.log('ack', {seq, consumerId, attempt});
    }
    const before = this.committedSeq;
    while (this.ackedAhead.delete(this.committedSeq + 1)) this.committedSeq++;
    if (this.committedSeq > before) {
      this.log('watermark', {seq: this.committedSeq, detail: `committed prefix -> ${this.committedSeq}`});
    }
    this.persist();
    return {advancedTo: this.committedSeq, acked, duplicated};
  }

  /**
   * 否定确认:消息立即回到可交付集合。绝不写入 ackedAhead,
   * 因此之后无论水位怎么推进,该缺口消息都必定可恢复。
   */
  nack(consumerId: string, seq: number, attempt: number, requeueDelayMs = 0): void {
    this.requireActiveConsumer(consumerId);
    this.requireKnown(seq);
    if (seq <= this.committedSeq) throw new DeliveryError('committed');
    const flight = this.requireCurrentFlight(consumerId, seq, attempt);
    this.inFlight.delete(seq);
    this.available.push(seq);
    if (requeueDelayMs > 0) {
      this.cooldown.set(`${seq}:${consumerId}`, this.now() + requeueDelayMs);
    }
    this.log('nack', {seq, consumerId, attempt});
    this.persist();
  }

  /** 消费者主动放弃(尚未处理完)。与 nack 相同的回退语义。 */
  cancel(consumerId: string, seq: number, attempt: number): void {
    this.requireActiveConsumer(consumerId);
    this.requireKnown(seq);
    if (seq <= this.committedSeq) throw new DeliveryError('committed');
    const flight = this.requireCurrentFlight(consumerId, seq, attempt);
    this.inFlight.delete(seq);
    this.available.push(seq);
    this.log('cancel', {seq, consumerId, attempt});
    this.persist();
  }

  private requireCurrentFlight(consumerId: string, seq: number, attempt: number): InFlight {
    const flight = this.inFlight.get(seq);
    if (!flight) throw new DeliveryError('not_in_flight');
    if (flight.consumerId !== consumerId) throw new DeliveryError('foreign_consumer');
    if (flight.attempt !== attempt) throw new DeliveryError('stale_attempt');
    return flight;
  }

  // ---------------------------------------------------------------- 扫描

  /**
   * 全局扫描:
   *  1. attempt 超时(超过交付时给出的 attemptTimeout)的在途消息回队;
   *  2. 租约过期的消费者名下全部在途消息回队(租约丢失),消费者被注销。
   * 回队消息会保留冷却,避免立即被同一过期消费者... 消费者已注销,
   * 所以直接对所有消费者立即可见。
   */
  sweep(): {timeouts: number[]; leaseLost: string[]} {
    const now = this.now();
    const timeouts: number[] = [];
    for (const flight of [...this.inFlight.values()]) {
      if (flight.visibleAt <= now) {
        this.inFlight.delete(flight.seq);
        this.available.push(flight.seq);
        timeouts.push(flight.seq);
        this.log('timeout', {seq: flight.seq, consumerId: flight.consumerId, attempt: flight.attempt});
      }
    }
    const leaseLost: string[] = [];
    for (const consumer of [...this.consumers.values()]) {
      if (consumer.leaseExpiresAt > now) continue;
      leaseLost.push(consumer.id);
      for (const flight of [...this.inFlight.values()]) {
        if (flight.consumerId !== consumer.id) continue;
        this.inFlight.delete(flight.seq);
        this.available.push(flight.seq);
        this.cooldown.delete(`${flight.seq}:${consumer.id}`);
        this.log('lease_lost', {seq: flight.seq, consumerId: consumer.id, attempt: flight.attempt});
      }
      this.consumers.delete(consumer.id);
    }
    if (timeouts.length || leaseLost.length) this.persist();
    return {timeouts, leaseLost};
  }

  // ---------------------------------------------------------------- 恢复

  /**
   * 模拟崩溃重启。
   * 有持久化:从最后一次快照重建状态。崩溃瞬间仍在途的消息没有消费者上下文,
   *   按「seq 已发布、未确认、不在可交付堆」推断为孤儿并重新入堆,attempt 计数
   *   保留(重投使用更大 attempt)。
   * 无持久化(纯内存工作台):当前在途消息直接回队,方便演示恢复效果。
   * 无论哪种路径:已提交前缀不回退,前缀内消息不再交付;缺口消息必定重新可交付。
   */
  restart(): void {
    if (this.persistence) {
      this.persist();
      const snapshot = this.persistence.load();
      if (snapshot) {
        this.restore(snapshot);
      } else {
        this.resetRuntimeState();
      }
    } else {
      // 纯内存工作台:当前在途消息直接回队,方便演示恢复效果
      for (const flight of [...this.inFlight.values()]) this.available.push(flight.seq);
      this.resetRuntimeState();
    }
    this.log('restart', {detail: this.persistence ? 'recovered from snapshot' : 'in-memory recovery'});
  }

  private resetRuntimeState(): void {
    this.inFlight.clear();
    this.cooldown.clear();
    this.consumers.clear();
  }

  private restore(snapshot: Snapshot): void {
    this.nextSeq = snapshot.nextSeq;
    this.committedSeq = snapshot.committedSeq;
    this.ackedAhead = new Set(snapshot.ackedAhead);
    this.available = new MinHeap(snapshot.availableHeap);
    this.attempts = new Map(Object.entries(snapshot.attempts).map(([k, v]) => [Number(k), v]));
    this.maxEventId = snapshot.maxEventId;
    this.inFlight.clear();
    this.cooldown.clear();
    this.consumers.clear();
    // 崩溃时在途的消息(曾交付但未确认、不在可交付堆中)全部恢复可交付
    for (const seq of this.attempts.keys()) {
      if (seq > this.committedSeq && !this.ackedAhead.has(seq) && !this.available.has(seq)) {
        this.available.push(seq);
      }
    }
  }

  // ---------------------------------------------------------------- 观测

  get committed(): number {
    return this.committedSeq;
  }

  get lastPublishedSeq(): number {
    return this.nextSeq - 1;
  }

  /**
   * 计算水位前方的缺口(未确认区间)。把「离散确认点」与「在途点」合并为
   * 有序边界,复杂度 O((k+f) log(k+f)),与发布总量无关 —— 即使发布序号到
   * 百万级、缺口巨大,也只产出少量区间而不物化每个序号。
   *
   * 返回:
   *  - pendingRanges:未确认且未在途的缺口区间,必定可恢复(可交付或冷却中);
   *  - inFlightSeqs:当前在途的序号,前端据此标注「缺口正在处理」。
   */
  gaps(limit = 100): {
    pendingRanges: Array<{from: number; to: number; cooling: boolean}>;
    inFlightSeqs: number[];
  } {
    const high = this.lastPublishedSeq;
    // 已占据(非缺口)的点:离散确认 + 在途
    const occupied = new Set<number>(this.ackedAhead);
    for (const seq of this.inFlight.keys()) occupied.add(seq);
    const points = [...occupied].filter(s => s > this.committedSeq && s <= high).sort((a, b) => a - b);
    const pendingRanges: Array<{from: number; to: number; cooling: boolean}> = [];
    let cursor = this.committedSeq + 1;
    for (const s of points) {
      if (s > cursor) {
        pendingRanges.push({from: cursor, to: s - 1, cooling: this.rangeCooling(cursor, s - 1)});
        if (pendingRanges.length >= limit) break;
      }
      cursor = s + 1;
    }
    if (pendingRanges.length < limit && cursor <= high) {
      pendingRanges.push({from: cursor, to: high, cooling: this.rangeCooling(cursor, high)});
    }
    return {
      pendingRanges: pendingRanges.slice(0, limit),
      inFlightSeqs: [...this.inFlight.keys()].sort((a, b) => a - b),
    };
  }

  /**
   * 缺口区间内是否有序号仍处于(未到期的)nack 冷却。扫描冷却表,
   * 表大小只与当前有冷却的序号×消费者数有关,与区间长度无关。
   */
  private rangeCooling(from: number, to: number): boolean {
    const now = this.now();
    for (const [key, until] of this.cooldown) {
      if (until <= now) continue;
      const colon = key.indexOf(':');
      const seq = Number(key.slice(0, colon));
      if (seq >= from && seq <= to) return true;
    }
    return false;
  }

  state(): {
    nextSeq: number;
    committedSeq: number;
    lastPublishedSeq: number;
    ackedAhead: number[];
    availableCount: number;
    inFlight: InFlight[];
    gaps: {
      pendingRanges: Array<{from: number; to: number; cooling: boolean}>;
      inFlightSeqs: number[];
    };
    consumers: Consumer[];
    events: DeliveryEvent[];
  } {
    return {
      nextSeq: this.nextSeq,
      committedSeq: this.committedSeq,
      lastPublishedSeq: this.lastPublishedSeq,
      ackedAhead: [...this.ackedAhead].sort((a, b) => a - b),
      availableCount: this.available.size,
      inFlight: [...this.inFlight.values()].sort((a, b) => a.seq - b.seq),
      gaps: this.gaps(),
      consumers: [...this.consumers.values()].map(c => ({...c})),
      events: [...this.events],
    };
  }

  // ---------------------------------------------------------------- 内部

  private requireKnown(seq: number): void {
    if (!Number.isInteger(seq) || seq < 1 || seq >= this.nextSeq) {
      throw new DeliveryError('unknown_message');
    }
  }

  private log(kind: EventKind, fields: Partial<DeliveryEvent> = {}): void {
    const event: DeliveryEvent = {id: ++this.maxEventId, at: this.now(), kind, ...fields};
    this.events.push(event);
    if (this.events.length > this.eventLimit) this.events.shift();
  }

  private persist(): void {
    if (!this.persistence) return;
    const snapshot: Snapshot = {
      version: 1,
      nextSeq: this.nextSeq,
      committedSeq: this.committedSeq,
      ackedAhead: [...this.ackedAhead].sort((a, b) => a - b),
      availableHeap: this.available.toJSON(),
      attempts: Object.fromEntries([...this.attempts.entries()].sort((a, b) => a[0] - b[0])),
      maxEventId: this.maxEventId,
    };
    this.persistence.save(snapshot);
  }
}
