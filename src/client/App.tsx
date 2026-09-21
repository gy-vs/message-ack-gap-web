import {useCallback, useEffect, useMemo, useState} from 'react';
import {
  AlertTriangle,
  CheckCheck,
  Play,
  Power,
  RefreshCw,
  Send,
  TimerReset,
  XCircle,
} from 'lucide-react';

type InFlight = {
  seq: number;
  consumerId: string;
  attempt: number;
  deliveredAt: number;
  visibleAt: number;
};
type GapRange = {from: number; to: number; cooling: boolean};
type Consumer = {id: string; leaseExpiresAt: number};
type DeliveryEvent = {
  id: number;
  at: number;
  kind: string;
  seq?: number;
  consumerId?: string;
  attempt?: number;
  detail?: string;
};
type State = {
  nextSeq: number;
  committedSeq: number;
  lastPublishedSeq: number;
  ackedAhead: number[];
  availableCount: number;
  inFlight: InFlight[];
  gaps: {pendingRanges: GapRange[]; inFlightSeqs: number[]};
  consumers: Consumer[];
  events: DeliveryEvent[];
};

async function call<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`${json?.error ?? response.status}${json?.message ? `: ${json.message}` : ''}`);
  return json as T;
}

const fmt = new Intl.NumberFormat();
const rangeText = (range: GapRange): string =>
  range.from === range.to ? fmt.format(range.from) : `${fmt.format(range.from)} – ${fmt.format(range.to)}`;

const eventColor: Record<string, string> = {
  ack: '#176b55',
  nack: '#b4502a',
  timeout: '#b4802a',
  cancel: '#6b6f78',
  lease_lost: '#a13a3a',
  watermark: '#176b55',
  restart: '#a13a3a',
  deliver: '#3a5fa1',
  publish: '#5b5b66',
};

export default function App() {
  const [state, setState] = useState<State | null>(null);
  const [consumerId, setConsumerId] = useState('worker-1');
  const [leaseMs, setLeaseMs] = useState(30_000);
  const [attemptTimeoutMs, setAttemptTimeoutMs] = useState(60_000);
  const [publishCount, setPublishCount] = useState(12);
  const [batchSize, setBatchSize] = useState(5);
  const [cooldown, setCooldown] = useState(0);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(async () => {
    const value = await fetch('/api/state').then(r => r.json() as Promise<State>);
    setState(value);
  }, []);

  useEffect(() => {
    refresh();
    const timer = setInterval(() => refresh().catch(() => undefined), 1500);
    return () => clearInterval(timer);
  }, [refresh]);

  const run = useCallback(
    async (action: () => Promise<unknown>) => {
      setError('');
      setBusy(true);
      try {
        await action();
        await refresh();
      } catch (e) {
        setError((e as Error).message);
      } finally {
        setBusy(false);
      }
    },
    [refresh],
  );

  const mine = useMemo(
    () => (state?.inFlight ?? []).filter(f => f.consumerId === consumerId),
    [state, consumerId],
  );
  const now = Date.now();

  return (
    <main className="shell">
      <header className="topbar">
        <AlertTriangle size={20} />
        <strong>消息交付工作台 · 确认水位与缺口</strong>
        <small>连续已确认前缀 + 前方离散确认集合,水位只随缺口补齐推进</small>
      </header>

      <section className="workspace">
        {/* 左栏:控制台 */}
        <aside className="pane controls">
          <h2>消费者</h2>
          <label className="field">
            consumerId
            <input value={consumerId} onChange={e => setConsumerId(e.target.value)} />
          </label>
          <label className="field">
            租约 (ms)
            <input type="number" value={leaseMs} onChange={e => setLeaseMs(Number(e.target.value))} />
          </label>
          <label className="field">
            attempt 超时 (ms)
            <input
              type="number"
              value={attemptTimeoutMs}
              onChange={e => setAttemptTimeoutMs(Number(e.target.value))}
            />
          </label>
          <div className="btnrow">
            <button
              onClick={() => run(() => call('/api/consumers', {consumerId, leaseMs}))}
              disabled={busy}
            >
              <Power size={14} /> 注册 / 重获租约
            </button>
            <button
              onClick={() => run(() => call(`/api/consumers/${encodeURIComponent(consumerId)}/renew`, {leaseMs}))}
              disabled={busy}
            >
              <TimerReset size={14} /> 续租
            </button>
          </div>

          <h2>发布</h2>
          <label className="field">
            数量
            <input
              type="number"
              value={publishCount}
              onChange={e => setPublishCount(Number(e.target.value))}
            />
          </label>
          <button
            className="wide"
            onClick={() => run(() => call('/api/publish', {count: publishCount}))}
            disabled={busy}
          >
            <Send size={14} /> 发布 {fmt.format(publishCount)} 条
          </button>

          <h2>交付与确认</h2>
          <label className="field">
            批量拉取条数
            <input type="number" value={batchSize} onChange={e => setBatchSize(Number(e.target.value))} />
          </label>
          <label className="field">
            nack 冷却 (ms)
            <input type="number" value={cooldown} onChange={e => setCooldown(Number(e.target.value))} />
          </label>
          <div className="btnrow">
            <button
              className="primary"
              disabled={busy}
              onClick={() =>
                run(() => call('/api/deliver', {consumerId, maxMessages: batchSize, attemptTimeoutMs}))
              }
            >
              <Play size={14} /> 拉取
            </button>
            <button
              disabled={busy || mine.length === 0}
              onClick={() =>
                run(() =>
                  call('/api/ack/batch', {
                    consumerId,
                    acks: mine.map(f => ({seq: f.seq, attempt: f.attempt})),
                  }),
                )
              }
            >
              <CheckCheck size={14} /> 全部 ack
            </button>
          </div>
          <div className="btnrow">
            <button
              disabled={busy}
              onClick={() => run(() => call('/api/sweep'))}
              title="扫描 attempt 超时与租约丢失,在途消息回队"
            >
              <RefreshCw size={14} /> 扫描超时 / 租约
            </button>
            <button
              className="danger"
              disabled={busy}
              onClick={() => run(() => call('/api/restart'))}
              title="模拟崩溃重启:从快照恢复,在途消息重新可交付"
            >
              <Power size={14} /> 崩溃重启
            </button>
          </div>
          {error && <div className="error"><XCircle size={14} /> {error}</div>}
        </aside>

        {/* 中栏:水位与缺口 */}
        <section className="pane">
          <h2>提交水位(连续已确认前缀)</h2>
          <div className="watermark-card">
            <div className="wm-main">
              <span className="wm-label">committedSeq</span>
              <span className="wm-value">{state ? fmt.format(state.committedSeq) : '–'}</span>
            </div>
            <div className="wm-sub">
              已发布到 {state ? fmt.format(state.lastPublishedSeq) : 0} · 可交付 {state?.availableCount ?? 0} ·
              在途 {state?.inFlight.length ?? 0}
            </div>
            <div className="wm-note">
              水位不是“见过的最大 ack 序号”:即使远端序号已确认,只要前方有缺口,水位也不会越过它。
            </div>
          </div>

          <h2>缺口 <small>(未确认区间,而非单个最大值)</small></h2>
          {state && state.gaps.pendingRanges.length === 0 ? (
            <div className="ok">水位前方没有缺口{state.inFlight.length > 0 ? '(仅剩在途处理中)' : ''}</div>
          ) : (
            <ul className="gap-list">
              {state?.gaps.pendingRanges.map((range, i) => (
                <li key={i} className={range.cooling ? 'cooling' : ''}>
                  <AlertTriangle size={15} />
                  <span className="gap-seq">{rangeText(range)}</span>
                  <span className="gap-meta">
                    {range.from === range.to ? 1 : fmt.format(range.to - range.from + 1)} 条未确认
                    {range.cooling ? ' · 部分处于 nack 冷却' : ' · 可恢复交付'}
                  </span>
                </li>
              ))}
            </ul>
          )}

          <h2>前方离散确认集合 ackedAhead</h2>
          {state && state.ackedAhead.length === 0 ? (
            <div className="dim">空</div>
          ) : (
            <div className="chips">
              {state?.ackedAhead.map(seq => <span className="chip ahead" key={seq}>{fmt.format(seq)}</span>)}
            </div>
          )}

          <h2>在途消息 <small>(绑定消费者租约 + attempt)</small></h2>
          <div className="table-wrap">
            <table className="flight-table">
              <thead>
                <tr>
                  <th>seq</th>
                  <th>consumer</th>
                  <th>attempt</th>
                  <th>剩余</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {state?.inFlight.map(f => {
                  const mineRow = f.consumerId === consumerId;
                  const remaining = Math.max(0, f.visibleAt - now);
                  const lease = state.consumers.find(c => c.id === f.consumerId);
                  const leaseDead = !lease || lease.leaseExpiresAt <= now;
                  return (
                    <tr key={f.seq} className={mineRow ? 'mine' : ''}>
                      <td className="mono">{fmt.format(f.seq)}</td>
                      <td>{f.consumerId}{leaseDead && <span className="tag dead">租约丢失</span>}</td>
                      <td className="mono">{f.attempt}</td>
                      <td className="mono">{remaining}ms</td>
                      <td className="row-actions">
                        <button
                          disabled={busy || !mineRow}
                          className="okbtn"
                          onClick={() =>
                            run(() => call('/api/ack', {consumerId, seq: f.seq, attempt: f.attempt}))
                          }
                          title={mineRow ? '' : '只有当前持有消费者可确认'}
                        >
                          ack
                        </button>
                        <button
                          disabled={busy || !mineRow}
                          onClick={() =>
                            run(() =>
                              call('/api/ack', {
                                consumerId,
                                seq: f.seq,
                                attempt: f.attempt - 1,
                              }),
                            )
                          }
                          title="模拟旧 attempt 的迟到 ack(应被拒绝)"
                        >
                          迟到ack
                        </button>
                        <button
                          disabled={busy || !mineRow}
                          className="nackbtn"
                          onClick={() =>
                            run(() =>
                              call('/api/nack', {
                                consumerId,
                                seq: f.seq,
                                attempt: f.attempt,
                                requeueDelayMs: cooldown,
                              }),
                            )
                          }
                        >
                          nack
                        </button>
                        <button
                          disabled={busy || !mineRow}
                          onClick={() =>
                            run(() => call('/api/cancel', {consumerId, seq: f.seq, attempt: f.attempt}))
                          }
                        >
                          cancel
                        </button>
                      </td>
                    </tr>
                  );
                })}
                {state?.inFlight.length === 0 && (
                  <tr>
                    <td colSpan={5} className="dim">无在途消息</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        {/* 右栏:消费者与事件 */}
        <aside className="pane">
          <h2>消费者租约</h2>
          <ul className="consumer-list">
            {state?.consumers.map(c => {
              const dead = c.leaseExpiresAt <= now;
              const holding = state.inFlight.filter(f => f.consumerId === c.id).length;
              return (
                <li key={c.id} className={dead ? 'dead' : ''}>
                  <strong>{c.id}</strong>
                  <span className={dead ? 'tag dead' : 'tag alive'}>{dead ? '已过期' : '存活'}</span>
                  <span className="dim">{dead ? 0 : Math.max(0, c.leaseExpiresAt - now)}ms · 在途 {holding}</span>
                </li>
              );
            })}
            {state?.consumers.length === 0 && <li className="dim">无注册消费者</li>}
          </ul>

          <h2>事件流</h2>
          <ul className="events">
            {state?.events
              .slice()
              .reverse()
              .map(event => (
                <li key={event.id}>
                  <span className="ev-kind" style={{color: eventColor[event.kind] ?? '#444'}}>
                    {event.kind}
                  </span>
                  <span className="ev-body">
                    {event.seq !== undefined && <>seq=<b>{fmt.format(event.seq)}</b></>}
                    {event.consumerId && <> @{event.consumerId}</>}
                    {event.attempt !== undefined && <> a{event.attempt}</>}
                    {event.detail && <> {event.detail}</>}
                  </span>
                </li>
              ))}
          </ul>
        </aside>
      </section>
    </main>
  );
}
