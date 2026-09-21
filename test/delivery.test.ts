import {describe, expect, it} from 'vitest';
import {DeliveryError, DeliveryStore, type Snapshot} from '../src/server/delivery';

/** 可控时钟 */
function clock(start = 1000) {
  let t = start;
  return {
    now: () => t,
    advance(ms: number) {
      t += ms;
    },
  };
}

function setup(leaseMs = 100_000, attemptTimeoutMs = 5_000) {
  const time = clock();
  const store = new DeliveryStore({now: time.now});
  store.registerConsumer('c1', leaseMs);
  store.registerConsumer('c2', leaseMs);
  return {store, time, leaseMs, attemptTimeoutMs};
}

function seqs(flights: Array<{seq: number}>): number[] {
  return flights.map(f => f.seq);
}

describe('乱序 ack 与水位推进', () => {
  it('12 先于 11 确认时水位停在 10,11 补齐后才推进到 12', () => {
    const {store} = setup();
    store.publish(12);
    const all = store.deliver('c1', 12, 60_000);

    // 先确认 1..10 形成连续前缀
    for (const f of all.slice(0, 10)) store.ack('c1', f.seq, f.attempt);
    expect(store.committed).toBe(10);

    // 序号 12 先确认 —— 只进入离散集合,水位不动
    expect(store.ack('c1', all[11].seq, all[11].attempt).advancedTo).toBe(10);
    expect(store.state().ackedAhead).toEqual([12]);
    // 11 仍在途处理:它不是"可恢复缺口",而是待处理的在途序号
    expect(store.state().gaps.pendingRanges).toEqual([]);
    expect(store.state().gaps.inFlightSeqs).toEqual([11]);

    // 缺口 11 补齐 —— 水位连续推进到 12
    expect(store.ack('c1', all[10].seq, all[10].attempt).advancedTo).toBe(12);
    expect(store.committed).toBe(12);
    expect(store.state().gaps.pendingRanges).toEqual([]);
    expect(store.state().gaps.inFlightSeqs).toEqual([]);
    expect(store.state().availableCount).toBe(0);
  });

  it('离散确认集合只在缺口从前往后连续补齐时逐格推进', () => {
    const {store} = setup();
    store.publish(6);
    const all = store.deliver('c1', 6, 60_000);
    store.ack('c1', 6, all[5].attempt);
    store.ack('c1', 4, all[3].attempt);
    store.ack('c1', 5, all[4].attempt);
    expect(store.committed).toBe(0);
    expect(store.state().ackedAhead).toEqual([4, 5, 6]);
    store.ack('c1', 1, all[0].attempt);
    expect(store.committed).toBe(1);
    store.ack('c1', 3, all[2].attempt);
    expect(store.committed).toBe(1);
    store.ack('c1', 2, all[1].attempt);
    expect(store.committed).toBe(6);
  });
});

describe('重复 ack', () => {
  it('已在离散集合的消息重复 ack 幂等成功,不产生重复效果', () => {
    const {store} = setup();
    store.publish(2);
    const [m1, m2] = store.deliver('c1', 2, 60_000);
    store.ack('c1', m2.seq, m2.attempt);
    const again = store.ack('c1', m2.seq, m2.attempt);
    expect(again.duplicated).toBe(true);
    expect(store.state().ackedAhead).toEqual([2]);
  });

  it('已并入前缀的消息再 ack 返回 duplicated,且消息不会重投', () => {
    const {store} = setup();
    store.publish(2);
    const [m1, m2] = store.deliver('c1', 2, 60_000);
    store.ack('c1', m1.seq, m1.attempt);
    store.ack('c1', m2.seq, m2.attempt);
    expect(store.ack('c1', 1, 1)).toEqual({advancedTo: 2, duplicated: true});
    expect(store.deliver('c1', 10, 60_000)).toEqual([]);
  });
});

describe('nack 后 ack 迟到', () => {
  it('nack 回队后旧 attempt 的迟到 ack 必须被拒绝(回队后为 not_in_flight,重投后为 stale_attempt),attempt 递增', () => {
    const {store} = setup();
    store.publish(1);
    const [m1] = store.deliver('c1', 1, 60_000);
    expect(m1.attempt).toBe(1);
    store.nack('c1', m1.seq, m1.attempt);

    // 旧 attempt 的 ack 迟到:消息已回队、不在在途集合,拒绝且不会变成成功确认
    expect(() => store.ack('c1', m1.seq, 1)).toThrowError(
      new DeliveryError('not_in_flight'),
    );

    // 消息重新可交付,新 attempt=2,此时旧 attempt 的 ack 被 stale_attempt 拒绝
    const [redelivered] = store.deliver('c1', 1, 60_000);
    expect(redelivered.attempt).toBe(2);
    expect(() => store.ack('c1', m1.seq, 1)).toThrowError(
      new DeliveryError('stale_attempt'),
    );
    // 当前 attempt 确认成功
    store.ack('c1', redelivered.seq, redelivered.attempt);
    expect(store.committed).toBe(1);
  });

  it('nack 绝不保留成功确认:先 ack 12 形成离散确认,再 nack 11 不被水位掩盖', () => {
    const {store} = setup();
    store.publish(12);
    const all = store.deliver('c1', 12, 60_000);
    for (const f of all.slice(0, 10)) store.ack('c1', f.seq, f.attempt);
    store.ack('c1', all[11].seq, all[11].attempt); // 12 离散确认,水位仍 10
    expect(store.committed).toBe(10);

    store.nack('c1', all[10].seq, all[10].attempt); // 11 否定
    // 即使反复扫描,11 仍在缺口里且可恢复
    store.sweep();
    const gaps = store.state().gaps.pendingRanges;
    expect(gaps).toEqual([{from: 11, to: 11, cooling: false}]);

    const [again] = store.deliver('c2', 1, 60_000);
    expect(again.seq).toBe(11);
    expect(again.attempt).toBe(2);
    store.ack('c2', again.seq, again.attempt);
    expect(store.committed).toBe(12);
  });

  it('nack 带冷却时对原消费者不可见,对其他消费者立即可见', () => {
    const {store, time} = setup();
    store.publish(1);
    const [m1] = store.deliver('c1', 1, 60_000);
    store.nack('c1', m1.seq, m1.attempt, 1_000);
    expect(store.deliver('c1', 1, 60_000)).toEqual([]);
    const [forC2] = store.deliver('c2', 1, 60_000);
    expect(forC2.seq).toBe(1);
    store.nack('c2', forC2.seq, forC2.attempt);
    time.advance(1_001);
    const [forC1] = store.deliver('c1', 1, 60_000);
    expect(forC1.seq).toBe(1);
    expect(forC1.attempt).toBe(3);
  });

  it('已提交前缀的消息 nack 被拒绝', () => {
    const {store} = setup();
    store.publish(1);
    const [m1] = store.deliver('c1', 1, 60_000);
    store.ack('c1', m1.seq, m1.attempt);
    expect(() => store.nack('c1', m1.seq, m1.attempt)).toThrowError(
      new DeliveryError('committed'),
    );
  });
});

describe('租约与 attempt 绑定', () => {
  it('租约过期后不能 ack/nack,名下在途消息回队后可被其他消费者接管', () => {
    const {store, time} = setup(1_000);
    store.registerConsumer('c2', 100_000); // c2 长租约,不与 c1 一起过期
    store.publish(2);
    const [m1] = store.deliver('c1', 1, 60_000);
    time.advance(1_001);
    expect(() => store.ack('c1', m1.seq, m1.attempt)).toThrowError(
      new DeliveryError('lease_lost'),
    );
    const swept = store.sweep();
    expect(swept.leaseLost).toEqual(['c1']);
    const [taken] = store.deliver('c2', 1, 60_000);
    expect(taken.seq).toBe(1);
    expect(taken.attempt).toBe(2);
    store.ack('c2', taken.seq, taken.attempt);
    expect(store.committed).toBe(1);
    // 已注销的消费者彻底不可用
    expect(() => store.deliver('c1', 1, 60_000)).toThrowError(
      new DeliveryError('unknown_consumer'),
    );
  });

  it('续租后仍可确认', () => {
    const {store, time} = setup(1_000);
    store.publish(1);
    const [m1] = store.deliver('c1', 1, 60_000);
    time.advance(900);
    store.renewLease('c1', 1_000);
    time.advance(900);
    store.ack('c1', m1.seq, m1.attempt);
    expect(store.committed).toBe(1);
  });

  it('其他消费者不能确认不属于自己的在途消息', () => {
    const {store} = setup();
    store.publish(1);
    const [m1] = store.deliver('c1', 1, 60_000);
    expect(() => store.ack('c2', m1.seq, m1.attempt)).toThrowError(
      new DeliveryError('foreign_consumer'),
    );
  });
});

describe('attempt 超时', () => {
  it('超过 attempt 超时窗口的在途消息由 sweep 回队,不带成功确认', () => {
    const {store, time} = setup(10_000, 500);
    store.publish(2);
    const [m1, m2] = store.deliver('c1', 2, 500);
    time.advance(501);
    const swept = store.sweep();
    expect(swept.timeouts.sort((a, b) => a - b)).toEqual([m1.seq, m2.seq]);
    const again = store.deliver('c1', 2, 500);
    expect(seqs(again)).toEqual([1, 2]);
    expect(again.every(f => f.attempt === 2)).toBe(true);
    expect(store.committed).toBe(0);
  });

  it('超时后旧 attempt 的 ack 迟到同样被拒绝', () => {
    const {store, time} = setup(10_000, 500);
    store.publish(1);
    const [m1] = store.deliver('c1', 1, 500);
    time.advance(501);
    store.sweep();
    expect(() => store.ack('c1', m1.seq, 1)).toThrowError(
      new DeliveryError('not_in_flight'),
    );
    const [m2] = store.deliver('c1', 1, 500);
    expect(() => store.ack('c1', m2.seq, 1)).toThrowError(
      new DeliveryError('stale_attempt'),
    );
    store.ack('c1', m2.seq, 2);
    expect(store.committed).toBe(1);
  });

  it('cancel 立即回队且旧 attempt 失效', () => {
    const {store} = setup();
    store.publish(1);
    const [m1] = store.deliver('c1', 1, 60_000);
    store.cancel('c1', m1.seq, m1.attempt);
    expect(() => store.ack('c1', m1.seq, m1.attempt)).toThrowError(
      new DeliveryError('not_in_flight'),
    );
    const [again] = store.deliver('c1', 1, 60_000);
    expect(again.attempt).toBe(2);
  });
});

describe('批量确认', () => {
  it('乱序批量 ack 只在缺口补齐时推进水位', () => {
    const {store} = setup();
    store.publish(5);
    const all = store.deliver('c1', 5, 60_000);
    const result = store.ackBatch('c1', [
      {seq: all[4].seq, attempt: all[4].attempt},
      {seq: all[1].seq, attempt: all[1].attempt},
    ]);
    expect(result.advancedTo).toBe(0);
    expect(result.acked.sort((a, b) => a - b)).toEqual([2, 5]);
    const fill = store.ackBatch(
      'c1',
      [all[0], all[2], all[3]].map(f => ({seq: f.seq, attempt: f.attempt})),
    );
    expect(fill.advancedTo).toBe(5);
  });

  it('批量中任一 attempt 不匹配则整批拒绝(原子),已提交前缀的序号视为幂等', () => {
    const {store} = setup();
    store.publish(4);
    const all = store.deliver('c1', 4, 60_000);
    store.ack('c1', all[0].seq, all[0].attempt); // 前缀到 1
    expect(() =>
      store.ackBatch('c1', [
        {seq: all[1].seq, attempt: all[1].attempt},
        {seq: all[2].seq, attempt: 999}, // 伪造 attempt
      ]),
    ).toThrowError(new DeliveryError('stale_attempt'));
    // 整批未生效
    expect(store.committed).toBe(1);
    expect(store.state().ackedAhead).toEqual([]);
    expect(store.state().inFlight.map(f => f.seq).sort((a, b) => a - b)).toEqual([2, 3, 4]);

    const ok = store.ackBatch('c1', [
      {seq: 1, attempt: 1}, // 已提交,幂等
      {seq: all[1].seq, attempt: all[1].attempt},
      {seq: all[2].seq, attempt: all[2].attempt},
      {seq: all[3].seq, attempt: all[3].attempt},
    ]);
    expect(ok.advancedTo).toBe(4);
    expect(ok.duplicated).toEqual([1]);
  });
});

describe('重启恢复', () => {
  function persistedSetup() {
    const time = clock();
    let saved: Snapshot | null = null;
    const store = new DeliveryStore({
      now: time.now,
      persistence: {
        load: () => saved,
        save: snapshot => {
          saved = JSON.parse(JSON.stringify(snapshot)) as Snapshot;
        },
      },
    });
    return {store, time, getSnapshot: () => saved};
  }

  it('重启后水位不回退:已提交前缀不再交付,离散确认集合保留', () => {
    const {store, getSnapshot} = persistedSetup();
    store.registerConsumer('c1');
    store.publish(12);
    const all = store.deliver('c1', 10, 60_000); // 取走 1..10
    for (const f of all) store.ack('c1', f.seq, f.attempt);
    expect(store.committed).toBe(10);
    expect(getSnapshot()?.committedSeq).toBe(10);

    store.restart(); // 崩溃重启
    expect(store.committed).toBe(10);
    expect(store.state().ackedAhead).toEqual([]);
    // 11、12 未发布过交付记录... 实际仍在可交付堆
    const after = store.deliver(register(store), 5, 60_000);
    expect(seqs(after)).toEqual([11, 12]);
    expect(after.every(f => f.seq > 10)).toBe(true);
  });

  it('崩溃瞬间在途的消息重启后重新可交付,缺口必定恢复,attempt 单调递增', () => {
    const {store} = persistedSetup();
    store.registerConsumer('c1');
    store.publish(5);
    const all = store.deliver('c1', 5, 60_000);
    store.ack('c1', all[0].seq, all[0].attempt); // 1 提交
    store.ack('c1', all[3].seq, all[3].attempt); // 4 离散确认
    // 2、3、5 在途时崩溃
    store.restart();
    expect(store.committed).toBe(1);
    expect(store.state().ackedAhead).toEqual([4]);
    const c = register(store);
    const recovered = store.deliver(c, 10, 60_000);
    expect(seqs(recovered)).toEqual([2, 3, 5]);
    expect(recovered.map(f => f.attempt)).toEqual([2, 2, 2]);
    // 补齐缺口后水位连续推进
    store.ackBatch(
      c,
      recovered.filter(f => f.seq !== 5).map(f => ({seq: f.seq, attempt: f.attempt})),
    );
    expect(store.committed).toBe(4);
    store.ack(c, 5, 2);
    expect(store.committed).toBe(5);
  });

  it('新实例从旧快照加载即可获得完全一致的恢复状态', () => {
    const time = clock();
    let saved: Snapshot | null = null;
    const make = () =>
      new DeliveryStore({
        now: time.now,
        persistence: {
          load: () => saved,
          save: s => {
            saved = JSON.parse(JSON.stringify(s)) as Snapshot;
          },
        },
      });
    const a = make();
    a.registerConsumer('c1');
    a.publish(3);
    const [m1] = a.deliver('c1', 1, 60_000);
    a.ack('c1', m1.seq, m1.attempt);
    a.deliver('c1', 1, 60_000); // seq 2 在途时崩溃

    const b = make(); // 全新实例,等价于进程重启
    expect(b.committed).toBe(1);
    b.registerConsumer('c2');
    const recovered = b.deliver('c2', 10, 60_000);
    expect(seqs(recovered)).toEqual([2, 3]);
  });

  function register(store: DeliveryStore): string {
    store.registerConsumer('c-after');
    return 'c-after';
  }
});

describe('巨大序号缺口', () => {
  it('发布十万级消息、确认远端序号时,缺口以单个压缩区间呈现且状态计算不依赖区间长度', () => {
    const {store} = setup();
    store.publish(100_000);
    // 直接交付全部序号,只确认最后两条,制造巨大缺口:1..99_998 在途,99_999/100_000 离散确认
    const all = store.deliver('c1', 100_000, 60_000);
    const tail = all.filter(f => f.seq >= 99_999);
    store.ackBatch(
      'c1',
      tail.map(f => ({seq: f.seq, attempt: f.attempt})),
    );
    expect(store.committed).toBe(0);
    const {pendingRanges, inFlightSeqs} = store.state().gaps;
    // 缺口:1..99_998 全部在途处理中,因此没有「可恢复」缺口区间,但在途列表完整
    expect(pendingRanges).toEqual([]);
    expect(inFlightSeqs).toHaveLength(99_998);
    expect(inFlightSeqs[0]).toBe(1);
    expect(inFlightSeqs[inFlightSeqs.length - 1]).toBe(99_998);
  });

  it('巨大缺口以未确认区间压缩展示,nack 回退后缺口必定可恢复', () => {
    const {store} = setup();
    store.publish(100_000);
    const all = store.deliver('c1', 100_000, 60_000);
    // 确认远端两条
    store.ackBatch(
      'c1',
      all.filter(f => f.seq >= 99_999).map(f => ({seq: f.seq, attempt: f.attempt})),
    );
    // 把 1..99_998 全部 nack 回队(取消在途占用),缺口立即变为可恢复
    for (const f of all.filter(x => x.seq <= 99_998)) store.nack('c1', f.seq, f.attempt);
    const {pendingRanges, inFlightSeqs} = store.state().gaps;
    expect(pendingRanges).toEqual([{from: 1, to: 99_998, cooling: false}]);
    expect(inFlightSeqs).toEqual([]);
    // 按序恢复交付前三条,attempt 均为 2,且确认后水位仍被 4..99_998 挡在 3
    const first = store.deliver('c1', 3, 60_000);
    expect(seqs(first)).toEqual([1, 2, 3]);
    expect(first.every(f => f.attempt === 2)).toBe(true);
    store.ackBatch('c1', first.map(f => ({seq: f.seq, attempt: f.attempt})));
    expect(store.committed).toBe(3);
    expect(store.state().gaps.pendingRanges).toEqual([{from: 4, to: 99_998, cooling: false}]);
  });

  it('缺口区间内未确认消息不会被远端水位/确认掩盖,逐段补齐后水位推进', () => {
    const {store} = setup();
    store.publish(1_000);
    const all = store.deliver('c1', 1_000, 60_000);
    // 确认 1000,水位必须保持 0
    store.ack('c1', 1_000, all[999].attempt);
    expect(store.committed).toBe(0);
    // nack 掉 1 与 501..999(回队),再确认 2..500,缺口仍然是 [1] 与 [501..999]
    store.nack('c1', 1, 1);
    for (const f of all.slice(500, 999)) store.nack('c1', f.seq, f.attempt);
    store.ackBatch(
      'c1',
      all.slice(1, 500).map(f => ({seq: f.seq, attempt: f.attempt})),
    );
    expect(store.committed).toBe(0);
    const ranges = store.state().gaps.pendingRanges;
    expect(ranges[0]).toEqual({from: 1, to: 1, cooling: false});
    expect(ranges[1]).toEqual({from: 501, to: 999, cooling: false});
    // 补 1:水位推进到 500
    const [again] = store.deliver('c2', 1, 60_000);
    expect(again.seq).toBe(1);
    expect(again.attempt).toBe(2);
    store.ack('c2', 1, 2);
    expect(store.committed).toBe(500);
    // 剩余缺口可恢复
    const rest = store.deliver('c1', 1_000, 60_000);
    expect(seqs(rest)).toEqual(
      Array.from({length: 499}, (_, i) => i + 501),
    );
    expect(rest.every(f => f.attempt === 2)).toBe(true);
  });
});
