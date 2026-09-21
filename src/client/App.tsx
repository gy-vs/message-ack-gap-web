import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {
  Activity,
  AlertTriangle,
  CheckCheck,
  ChevronRight,
  Inbox,
  Play,
  Power,
  RefreshCw,
  RotateCcw,
  Send,
  UserPlus,
  XCircle,
  Zap,
} from 'lucide-react';
import {api, type Delivery, type QueueState} from './api';

type LeaseConfig = {leaseTtlMs: number; deliveryTtlMs: number; maxInFlight: number};

const DEFAULT_LEASE: LeaseConfig = {leaseTtlMs: 20_000, deliveryTtlMs: 30_000, maxInFlight: 16};

function shortId(id: string | null | undefined): string {
  return id ? id.slice(0, 8) : '—';
}

function formatGap(gap: {from: string; to: string}): string {
  return gap.from === gap.to ? gap.from : `${gap.from} → ${gap.to}`;
}

interface Status {
  kind: 'ok' | 'err';
  text: string;
}

export default function App() {
  const [state, setState] = useState<QueueState | null>(null);
  const [status, setStatus] = useState<Status>({kind: 'ok', text: '就绪'});
  const [consumerId, setConsumerId] = useState<string | null>(() => localStorage.getItem('consumerId'));
  const [lease, setLease] = useState<LeaseConfig>(DEFAULT_LEASE);
  const [publishBody, setPublishBody] = useState('payload');
  const [publishGap, setPublishGap] = useState('1000000');
  const [busy, setBusy] = useState(false);
  const stateRef = useRef<QueueState | null>(null);
  stateRef.current = state;

  const refresh = useCallback(async (silent = false) => {
    try {
      const next = await api.state();
      setState(next);
    } catch (error) {
      if (!silent) setStatus({kind: 'err', text: error instanceof Error ? error.message : String(error)});
    }
  }, []);

  useEffect(() => {
    refresh();
    const timer = setInterval(() => refresh(true), 700);
    return () => clearInterval(timer);
  }, [refresh]);

  // Heartbeat keeps the active consumer's lease alive.
  useEffect(() => {
    if (!consumerId) return;
    const beat = () =>
      api
        .heartbeat(consumerId)
        .catch(() => undefined);
    beat();
    const timer = setInterval(beat, Math.max(1000, Math.floor(lease.leaseTtlMs / 3)));
    return () => clearInterval(timer);
  }, [consumerId, lease.leaseTtlMs]);

  const run = useCallback(
    async (label: string, fn: () => Promise<unknown>) => {
      setBusy(true);
      try {
        await fn();
        await refresh();
        setStatus({kind: 'ok', text: label});
      } catch (error) {
        setStatus({kind: 'err', text: `${label} 失败: ${error instanceof Error ? error.message : String(error)}`});
      } finally {
        setBusy(false);
      }
    },
    [refresh],
  );

  async function register() {
    const created = await api.register(lease);
    setConsumerId(created.consumerId);
    localStorage.setItem('consumerId', created.consumerId);
  }

  async function forgetConsumer() {
    if (consumerId) await api.cancel(consumerId).catch(() => undefined);
    setConsumerId(null);
    localStorage.removeItem('consumerId');
    await refresh();
  }

  const myInFlight = useMemo(
    () => (state?.inFlight ?? []).filter(d => d.consumerId === consumerId),
    [state, consumerId],
  );

  const myTokens = myInFlight.map(d => ({seq: d.seq, deliveryId: d.deliveryId, attempt: d.attempt}));

  const consumers = state?.consumers ?? [];
  const gaps = state?.gaps ?? [];
  const parked = state?.pendingAhead ?? [];
  const blocked = !!state && state.blockingGap !== null;

  return (
    <main className="shell">
      <header className="topbar">
        <Inbox size={20} />
        <strong>消息交付工作台</strong>
        <small>连续前缀水位 · 离散确认集合 · 租约绑定投递</small>
        <div className={`status ${status.kind}`}>
          {status.kind === 'ok' ? <CheckCheck size={14} /> : <AlertTriangle size={14} />}
          {status.text}
        </div>
      </header>

      <section className="workspace">
        {/* ---------- 左栏：发布 / 消费者 / 场景 ---------- */}
        <aside className="pane">
          <section className="card">
            <h2><Send size={15} /> 发布消息</h2>
            <label className="field">
              <span>消息内容</span>
              <input value={publishBody} onChange={e => setPublishBody(e.target.value)} />
            </label>
            <button
              className="primary"
              disabled={busy}
              onClick={() => run('已追加到队尾（自动序号）', () => api.publish(publishBody))}
            >
              追加发布（下一个序号）
            </button>
            <label className="field">
              <span>在指定序号发布（制造巨大缺口）</span>
              <input value={publishGap} onChange={e => setPublishGap(e.target.value.replace(/[^\d]/g, ''))} />
            </label>
            <button
              disabled={busy || !publishGap}
              onClick={() => run(`已在序号 ${publishGap} 发布`, () => api.publish(publishBody, publishGap))}
            >
              发布到序号 {publishGap || '…'}
            </button>
          </section>

          <section className="card">
            <h2><UserPlus size={15} /> 消费者租约</h2>
            <div className="grid3">
              <label className="field">
                <span>租约 TTL (ms)</span>
                <input
                  type="number"
                  value={lease.leaseTtlMs}
                  onChange={e => setLease(l => ({...l, leaseTtlMs: Number(e.target.value)}))}
                />
              </label>
              <label className="field">
                <span>投递超时 (ms)</span>
                <input
                  type="number"
                  value={lease.deliveryTtlMs}
                  onChange={e => setLease(l => ({...l, deliveryTtlMs: Number(e.target.value)}))}
                />
              </label>
              <label className="field">
                <span>最大在途</span>
                <input
                  type="number"
                  value={lease.maxInFlight}
                  onChange={e => setLease(l => ({...l, maxInFlight: Number(e.target.value)}))}
                />
              </label>
            </div>
            {consumerId ? (
              <>
                <div className="consumer-id">
                  <Activity size={14} />
                  <code>{shortId(consumerId)}</code>
                  <span className="alive">心跳保活中</span>
                </div>
                <button onClick={() => run('已拉取一批消息', () => api.fetch(consumerId, lease.maxInFlight))}>
                  <Play size={14} /> 拉取消息（在途上限 {lease.maxInFlight}）
                </button>
                <div className="row">
                  <button
                    disabled={busy || myTokens.length === 0}
                    title="确认本消费者持有的全部在途消息"
                    onClick={() =>
                      run(`批量确认 ${myTokens.length} 条`, async () => {
                        const result = await api.batchAck(consumerId, myTokens);
                        return result;
                      })
                    }
                  >
                    <CheckCheck size={14} /> 批量 ack
                  </button>
                  <button
                    disabled={busy || myTokens.length === 0}
                    onClick={() => run(`批量否定 ${myTokens.length} 条`, () => api.batchNack(consumerId, myTokens))}
                  >
                    <XCircle size={14} /> 批量 nack
                  </button>
                </div>
                <button className="ghost" onClick={forgetConsumer}>
                  <Power size={14} /> 取消租约（在途全部回到可交付）
                </button>
              </>
            ) : (
              <button className="primary" disabled={busy} onClick={() => run('已注册新消费者', register)}>
                注册消费者
              </button>
            )}
          </section>

          <section className="card">
            <h2><Zap size={15} /> 场景剧本</h2>
            <button
              disabled={busy || !consumerId}
              onClick={() => run('乱序剧本：1..10 已确认，12 已停在水位前', async () => scenarios.outOfOrder(consumerId!, lease))}
            >
              乱序 ack：12 先于 11
            </button>
            <button
              disabled={busy || !consumerId}
              onClick={() => run('缺口剧本：1 停留在途，2..11 已确认', async () => scenarios.blockedGap(consumerId!, lease))}
            >
              巨大序号缺口（确认远端）
            </button>
            <button
              disabled={busy || !consumerId}
              onClick={() => run('已恢复为全新进程状态', async () => {
                await api.restart();
                setConsumerId(null);
                localStorage.removeItem('consumerId');
              })}
            >
              <RotateCcw size={14} /> 模拟崩溃重启
            </button>
          </section>
        </aside>

        {/* ---------- 中栏：水位 + 缺口 + 序号轨道 ---------- */}
        <section className="pane center">
          {state ? (
            <>
              <div className="watermark-bar">
                <div className="metric">
                  <span className="metric-label">已提交水位 watermark</span>
                  <span className="metric-value">{state.watermark}</span>
                </div>
                <div className="metric">
                  <span className="metric-label">下一条可交付</span>
                  <span className="metric-value">
                    {state.deliverable[0] ?? state.inFlight[0]?.seq ?? '—'}
                  </span>
                </div>
                <div className="metric">
                  <span className="metric-label">前方离散确认</span>
                  <span className="metric-value">{parked.length}</span>
                </div>
                <div className={`metric ${blocked ? 'danger' : ''}`}>
                  <span className="metric-label">{blocked ? '阻塞缺口' : '缺口'}</span>
                  <span className="metric-value">{blocked ? state.blockingGap : '无'}</span>
                </div>
              </div>

              <div className="gap-panel">
                <h3>
                  <AlertTriangle size={15} />
                  缺口区间（不是单个最大值）
                </h3>
                {gaps.length === 0 ? (
                  <p className="muted">没有缺口：所有已确认序号连续地并入了水位。</p>
                ) : (
                  <ul className="gap-list">
                    {gaps.map((gap, i) => (
                      <li key={`${gap.from}-${gap.to}`}>
                        <span className="gap-range">{formatGap(gap)}</span>
                        <span className="gap-count">
                          {gap.from === gap.to ? '1 条消息' : `${(BigInt(gap.to) - BigInt(gap.from) + 1n).toString()} 条消息`}
                        </span>
                        {i === 0 && <span className="badge">水位正在等待</span>}
                      </li>
                    ))}
                    {state.gapsTruncated && <li className="muted">…缺口过多，仅显示前 32 段</li>}
                  </ul>
                )}
                {parked.length > 0 && (
                  <p className="muted small">
                    停在水位前的离散确认：
                    {parked.slice(0, 24).map((seq, i) => (
                      <code className="chip" key={seq}>{i > 0 ? ' ' : ''}{seq}</code>
                    ))}
                    {parked.length > 24 && <span> …（+{parked.length - 24}）</span>}
                  </p>
                )}
              </div>

              <SequenceTrack state={state} />
            </>
          ) : (
            <p className="muted">加载中…</p>
          )}
        </section>

        {/* ---------- 右栏：在途 / 消费者 ---------- */}
        <aside className="pane">
          <section className="card">
            <h2>
              <RefreshCw size={15} /> 在途投递（in-flight）
            </h2>
            {(state?.inFlight ?? []).length === 0 ? (
              <p className="muted">当前没有在途消息。</p>
            ) : (
              <ul className="inflight-list">
                {(state?.inFlight ?? []).map(row => (
                  <li key={`${row.seq}-${row.deliveryId}`} className={row.consumerId === consumerId ? 'mine' : ''}>
                    <div className="inflight-head">
                      <strong>#{row.seq}</strong>
                      <span className="attempt">attempt {row.attempt}</span>
                      {row.consumerId === consumerId && <span className="badge ok">本消费者</span>}
                    </div>
                    <div className="inflight-meta">
                      <code>{shortId(row.deliveryId)}</code>
                      <code>{shortId(row.consumerId)}</code>
                    </div>
                    {row.consumerId === consumerId && consumerId && (
                      <div className="row">
                        <button
                          disabled={busy}
                          onClick={() =>
                            run(`#${row.seq} 已确认`, () =>
                              api.ack(consumerId, {seq: row.seq, deliveryId: row.deliveryId, attempt: row.attempt}),
                            )
                          }
                        >
                          ack
                        </button>
                        <button
                          className="warn"
                          disabled={busy}
                          onClick={() =>
                            run(`#${row.seq} 已否定，回到可交付`, () =>
                              api.nack(consumerId, {seq: row.seq, deliveryId: row.deliveryId, attempt: row.attempt}),
                            )
                          }
                        >
                          nack
                        </button>
                      </div>
                    )}
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section className="card">
            <h2><UserPlus size={15} /> 消费者与租约</h2>
            {consumers.length === 0 ? (
              <p className="muted">无活跃消费者。</p>
            ) : (
              <ul className="consumer-list">
                {consumers.map(consumer => (
                  <li key={consumer.id} className={consumer.alive ? '' : 'dead'}>
                    <code>{shortId(consumer.id)}</code>
                    <span className={consumer.alive ? 'alive' : 'lost'}>
                      {consumer.alive ? '租约有效' : '租约已失效'}
                    </span>
                    <span className="muted small">
                      租约 {consumer.leaseTtlMs}ms · 投递 {consumer.deliveryTtlMs}ms
                    </span>
                  </li>
                ))}
              </ul>
            )}
            <p className="muted small">
              提示：注册一个 <em>很短租约</em> 的消费者并拉取消息，等待心跳停止/到期，即可观察租约丢失后在途回收。
            </p>
          </section>
        </aside>
      </section>
    </main>
  );
}

/** 消息序号轨道：已提交 / 缺口 / 在途 / 离散确认 / 可交付，分段显示。 */
function SequenceTrack({state}: {state: QueueState}) {
  const segments = useMemo(() => buildSegments(state), [state]);
  return (
    <section className="track-panel">
      <h3><ChevronRight size={15} /> 序号轨道</h3>
      <div className="legend">
        <span><i className="sw committed" /> 已提交前缀（不再投递）</span>
        <span><i className="sw gap" /> 缺口（必定可恢复）</span>
        <span><i className="sw inflight" /> 在途</span>
        <span><i className="sw parked" /> 离散确认（停在水位前）</span>
        <span><i className="sw ready" /> 可交付</span>
      </div>
      <ul className="track">
        {segments.map(segment => (
          <li key={segment.key} className={`segment ${segment.kind}`} title={segment.title}>
            {segment.label}
          </li>
        ))}
      </ul>

      <h3 className="mt">可交付队列</h3>
      {state.deliverable.length === 0 ? (
        <p className="muted">队列为空。</p>
      ) : (
        <ul className="seq-chips">
          {state.deliverable.slice(0, 64).map(seq => (
            <li key={seq}>
              <code>#{seq}</code>
            </li>
          ))}
          {state.deliverable.length > 64 && <li className="muted">…+{state.deliverable.length - 64}</li>}
        </ul>
      )}
    </section>
  );
}

type SegmentKind = 'committed' | 'gap' | 'inflight' | 'parked' | 'ready';
interface Segment {
  key: string;
  kind: SegmentKind;
  label: string;
  title: string;
}

/**
 * Build a compact, non-scanning representation of the sequence space:
 * committed prefix collapses to one block; gap *ranges* (from the server)
 * collapse regardless of their numeric width; individual messages are only
 * rendered for parked / in-flight / ready sequences. A 10^12 gap is one chip.
 */
function buildSegments(state: QueueState): Segment[] {
  const segments: Segment[] = [];
  const wm = BigInt(state.watermark);
  const inflightBySeq = new Map(state.inFlight.map(d => [d.seq, d]));
  const parkedSet = new Set(state.pendingAhead);
  const readySet = new Set(state.deliverable);

  if (wm >= 1n) {
    segments.push({
      key: 'committed',
      kind: 'committed',
      label: wm === 1n ? '#1 ✓' : `#1 → #${state.watermark} ✓`,
      title: `已提交前缀，序号 ≤ ${state.watermark} 永不重投`,
    });
  }

  // Rows for every published message above the watermark plus gap ranges, sorted numerically.
  type Row =
    | {type: 'gap'; from: bigint; to: bigint}
    | {type: 'msg'; seq: bigint};
  const rows: Row[] = state.gaps.map(g => ({type: 'gap', from: BigInt(g.from), to: BigInt(g.to)}));
  for (const message of state.messages) {
    const seq = BigInt(message.seq);
    if (seq > wm) rows.push({type: 'msg', seq});
  }
  rows.sort((a, b) => {
    const ax = a.type === 'gap' ? a.from : a.seq;
    const bx = b.type === 'gap' ? b.from : b.seq;
    return ax < bx ? -1 : ax > bx ? 1 : 0;
  });

  for (const row of rows) {
    if (row.type === 'gap') {
      const width = row.to - row.from + 1n;
      segments.push({
        key: `gap-${row.from}`,
        kind: 'gap',
        label: width === 1n ? `#${row.from} 缺口` : `#${row.from}…#${row.to} 缺口`,
        title: `缺口区间，共 ${width.toString()} 条未确认消息；补齐前水位不会越过`,
      });
      continue;
    }
    const seqStr = row.seq.toString();
    if (parkedSet.has(seqStr)) {
      segments.push({key: `p-${seqStr}`, kind: 'parked', label: `#${seqStr} ⏸`, title: '离散确认，等待前方缺口补齐'});
    } else if (inflightBySeq.has(seqStr)) {
      segments.push({key: `i-${seqStr}`, kind: 'inflight', label: `#${seqStr} …`, title: '在途投递，受 delivery attempt 与租约约束'});
    } else if (readySet.has(seqStr)) {
      segments.push({key: `r-${seqStr}`, kind: 'ready', label: `#${seqStr}`, title: '可交付'});
    }
  }

  return segments;
}

/** One-click demo scenarios driving the same HTTP API a real client would use. */
const scenarios = {
  async outOfOrder(consumerId: string, lease: LeaseConfig): Promise<void> {
    for (let i = 0; i < 12; i++) await api.publish(`msg-${i + 1}`);
    const {deliveries} = await api.fetch(consumerId, Math.max(12, lease.maxInFlight));
    const bySeq = new Map<string, Delivery>(deliveries.map(d => [d.seq, d]));
    const ack = (seq: number) => {
      const d = bySeq.get(String(seq));
      if (!d) throw new Error(`missing delivery for ${seq}`);
      return api.ack(consumerId, {seq: d.seq, deliveryId: d.deliveryId, attempt: d.attempt});
    };
    // 12 confirms first and must park behind missing 11.
    await ack(12);
    for (let seq = 1; seq <= 10; seq++) await ack(seq);
    // 11 deliberately left in-flight so the UI shows the blocking gap.
  },

  async blockedGap(consumerId: string, lease: LeaseConfig): Promise<void> {
    await api.publish('near', '1');
    await api.publish('far', '1000000');
    const {deliveries} = await api.fetch(consumerId, Math.max(2, lease.maxInFlight));
    const far = deliveries.find(d => d.seq === '1000000');
    if (!far) throw new Error('far delivery missing');
    // Confirm the far message; near message stays in-flight → watermark blocked at 0.
    await api.ack(consumerId, {seq: far.seq, deliveryId: far.deliveryId, attempt: far.attempt});
  },
};
