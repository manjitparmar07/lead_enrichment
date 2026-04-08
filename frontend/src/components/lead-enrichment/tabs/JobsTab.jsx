import { useState } from 'react'
import {
  RefreshCw, Clock, CheckCircle, AlertCircle,
  ChevronDown, ChevronUp, ArrowRight,
} from 'lucide-react'
import { toast } from 'react-hot-toast'
import { BACKEND } from '../../../utils/api'
import { Card, Lbl, StatusBadge, RefreshSpin, GhostBtn } from '../../ui'

// Lucide icons used inline in JSX below
import { Briefcase, Database, Layers, List } from 'lucide-react'

export default function JobsTab({ jobs, onRefresh, onSelectJob }) {
  const [expandedJobs, setExpandedJobs] = useState({})
  const [filterStatus, setFilterStatus] = useState('')
  const [searchQ, setSearchQ] = useState('')
  const [stopping, setStopping] = useState({})
  const [rerunning, setRerunning] = useState({})
  const [deleting, setDeleting] = useState({})

  const toggleExpand = (jobId) =>
    setExpandedJobs(prev => ({ ...prev, [jobId]: !prev[jobId] }))

  const handleStop = async (jobId) => {
    setStopping(p => ({ ...p, [jobId]: true }))
    try {
      const res = await fetch(`${BACKEND}/leads/jobs/${jobId}/stop`, { method: 'POST' })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail)
      toast.success(`Job stopped · ${data.snapshots_cancelled ?? 0} snapshot(s) cancelled`)
      onRefresh()
    } catch (e) {
      toast.error(`Stop failed: ${e.message}`)
    } finally { setStopping(p => ({ ...p, [jobId]: false })) }
  }

  const handleRerun = async (jobId) => {
    setRerunning(p => ({ ...p, [jobId]: true }))
    try {
      const res = await fetch(`${BACKEND}/leads/jobs/${jobId}/rerun`, { method: 'POST' })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail)
      toast.success(`Job restarted · new snapshot: ${data.new_snapshot_id?.slice(0, 12) ?? '—'}`)
      onRefresh()
    } catch (e) {
      toast.error(`Rerun failed: ${e.message}`)
    } finally { setRerunning(p => ({ ...p, [jobId]: false })) }
  }

  const handleDelete = async (jobId) => {
    if (!confirm('Delete this job? This cannot be undone.')) return
    setDeleting(p => ({ ...p, [jobId]: true }))
    try {
      const res = await fetch(`${BACKEND}/leads/jobs/${jobId}`, { method: 'DELETE' })
      if (!res.ok) throw new Error((await res.json()).detail)
      onRefresh()
    } catch (e) {
      alert(`Delete failed: ${e.message}`)
    } finally { setDeleting(p => ({ ...p, [jobId]: false })) }
  }

  const visibleJobs = jobs.filter(j => {
    if (filterStatus && j.status !== filterStatus) return false
    if (searchQ && !j.id.toLowerCase().includes(searchQ.toLowerCase())) return false
    return true
  })

  const STATUS_CFG = {
    running:                { color: '#6366f1', bg: 'rgba(99,102,241,0.1)',  border: 'rgba(99,102,241,0.25)', label: 'Running',          spin: true  },
    pending:                { color: '#f97316', bg: 'rgba(249,115,22,0.1)', border: 'rgba(249,115,22,0.25)', label: 'Pending',          spin: false },
    completed:              { color: '#10b981', bg: 'rgba(16,185,129,0.1)', border: 'rgba(16,185,129,0.25)', label: 'Completed',        spin: false },
    completed_with_errors:  { color: '#f97316', bg: 'rgba(249,115,22,0.1)', border: 'rgba(249,115,22,0.25)', label: 'Completed (errors)',spin: false },
    failed:                 { color: '#ef4444', bg: 'rgba(239,68,68,0.1)',  border: 'rgba(239,68,68,0.25)',  label: 'Failed',           spin: false },
    stale:                  { color: '#6b7280', bg: 'rgba(107,114,128,0.08)',border: 'rgba(107,114,128,0.2)', label: 'Stale',            spin: false },
    fallback:               { color: '#8b5cf6', bg: 'rgba(139,92,246,0.1)', border: 'rgba(139,92,246,0.25)', label: 'Processing',       spin: true  },
  }

  const subStatusColor = (s) => ({
    pending:               '#6b7280',
    running:               '#6366f1',
    scraped:               '#eab308',
    completed:             '#10b981',
    completed_with_errors: '#f97316',
    failed:                '#ef4444',
  }[s] || '#6b7280')

  const subStatusIcon = (s) => {
    if (s === 'running') return <span style={{ display: 'inline-block', animation: 'spin 1s linear infinite' }}><RefreshCw size={9} /></span>
    if (s === 'scraped') return <Database size={9} />
    if (s === 'completed') return <CheckCircle size={9} />
    if (s === 'failed') return <AlertCircle size={9} />
    return <span style={{ display: 'inline-block', width: 9, height: 9, borderRadius: '50%', background: '#6b7280', opacity: 0.5 }} />
  }

  const ALL_STATUSES = ['running','pending','fallback','completed','completed_with_errors','failed','stale','cancelled']

  return (
    <div>
      {/* Toolbar */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 14, flexWrap: 'wrap' }}>
        <input
          type="text"
          value={searchQ}
          onChange={e => setSearchQ(e.target.value)}
          placeholder="Search by Job ID…"
          style={{
            flex: 1, minWidth: 180, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
            borderRadius: 7, padding: '6px 12px', fontSize: 12, color: 'var(--text-1)',
          }}
        />
        <select
          value={filterStatus}
          onChange={e => setFilterStatus(e.target.value)}
          style={{
            background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
            borderRadius: 7, padding: '6px 10px', fontSize: 12, color: 'var(--text-1)', cursor: 'pointer',
          }}
        >
          <option value="">All statuses</option>
          {ALL_STATUSES.map(s => <option key={s} value={s}>{s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g,' ')}</option>)}
        </select>
        <div style={{ fontSize: 11, color: 'var(--text-3)', flexShrink: 0 }}>
          {visibleJobs.filter(j => ['running','fallback','pending'].includes(j.status)).length > 0
            ? `${visibleJobs.filter(j => ['running','fallback','pending'].includes(j.status)).length} in progress`
            : `${visibleJobs.length} / ${jobs.length} jobs`
          }
        </div>
        <GhostBtn onClick={onRefresh}><RefreshCw size={11} /> Refresh</GhostBtn>
      </div>

      {visibleJobs.length === 0 && (
        <div style={{ textAlign: 'center', padding: '60px 20px', color: 'var(--text-3)' }}>
          <Briefcase size={34} style={{ opacity: 0.25, marginBottom: 10 }} />
          <p style={{ fontSize: 14 }}>{jobs.length === 0 ? 'No enrichment jobs yet — use Bulk Enrich to start' : 'No jobs match the current filter'}</p>
        </div>
      )}

      {visibleJobs.map(job => {
        const done = (job.processed || 0)
        const failed = (job.failed || 0)
        const total = job.total_urls || 0
        const pct = total > 0 ? Math.round(((done + failed) / total) * 100) : 0
        const cfg = STATUS_CFG[job.status] || STATUS_CFG.pending
        const isStale = job.status === 'stale'
        const isActive = ['running', 'fallback', 'pending'].includes(job.status)
        const barColor = job.status === 'completed' ? '#10b981'
          : job.status === 'failed' ? '#ef4444'
          : isActive ? '#6366f1' : '#6b7280'
        const subJobs = job.sub_jobs || []
        const isExpanded = expandedJobs[job.id]

        return (
          <div key={job.id} style={{
            padding: '16px 18px', borderRadius: 10,
            border: `1px solid ${isActive ? 'rgba(99,102,241,0.3)' : 'var(--border-1)'}`,
            background: 'var(--bg-card)', marginBottom: 10,
          }}>
            {/* Header row */}
            <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 12 }}>
              {/* Status badge */}
              <span style={{
                display: 'inline-flex', alignItems: 'center', gap: 5,
                padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                background: cfg.bg, border: `1px solid ${cfg.border}`, color: cfg.color,
              }}>
                {cfg.spin
                  ? <span style={{ display: 'inline-block', animation: 'spin 1s linear infinite' }}><RefreshCw size={10} /></span>
                  : <CheckCircle size={10} />
                }
                {cfg.label}
              </span>
              <code style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-3)', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {job.id}
              </code>
              <span style={{ fontSize: 11, color: 'var(--text-3)', flexShrink: 0 }}>
                {new Date(job.created_at).toLocaleString()}
              </span>
            </div>

            {/* Stale warning */}
            {isStale && (
              <div style={{
                padding: '8px 12px', marginBottom: 12, borderRadius: 7,
                background: 'rgba(239,68,68,0.06)', border: '1px solid rgba(239,68,68,0.2)',
                fontSize: 11, color: '#ef4444',
              }}>
                ⚠ {job.error || 'This job was waiting for a webhook that was not configured. Re-submit via Bulk Enrich.'}
              </div>
            )}

            {/* Progress stats */}
            <div style={{ display: 'flex', gap: 20, marginBottom: 10, alignItems: 'center' }}>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 20, fontWeight: 800, color: 'var(--text-1)', lineHeight: 1 }}>{total}</div>
                <div style={{ fontSize: 9, color: 'var(--text-3)', textTransform: 'uppercase', marginTop: 2 }}>Total</div>
              </div>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 20, fontWeight: 800, color: '#10b981', lineHeight: 1 }}>{done}</div>
                <div style={{ fontSize: 9, color: 'var(--text-3)', textTransform: 'uppercase', marginTop: 2 }}>Done</div>
              </div>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 20, fontWeight: 800, color: '#ef4444', lineHeight: 1 }}>{failed}</div>
                <div style={{ fontSize: 9, color: 'var(--text-3)', textTransform: 'uppercase', marginTop: 2 }}>Failed</div>
              </div>
              <div style={{ flex: 1, marginLeft: 8 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5 }}>
                  <span style={{ fontSize: 11, color: 'var(--text-3)' }}>Progress</span>
                  <span style={{ fontSize: 12, fontWeight: 700, color: barColor }}>{pct}%</span>
                </div>
                <div style={{ height: 8, borderRadius: 4, background: 'var(--bg-elevated)', overflow: 'hidden' }}>
                  <div style={{
                    height: '100%', borderRadius: 4, width: `${pct}%`,
                    background: `linear-gradient(90deg,${barColor}88,${barColor})`,
                    transition: 'width 1s ease',
                  }} />
                </div>
                {total > 0 && done < total && isActive && (
                  <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 4 }}>
                    {total - done - failed} lead(s) remaining · each takes ~30–60s
                  </div>
                )}
              </div>
            </div>

            {/* Snapshot info */}
            {job.snapshot_id && (
              <div style={{ fontSize: 10, color: 'var(--text-3)', marginBottom: 10 }}>
                Bright Data Snapshot: <code style={{ fontFamily: 'monospace' }}>{job.snapshot_id}</code>
              </div>
            )}

            {/* Sub-jobs / Chunks section */}
            {subJobs.length > 0 && (
              <div style={{ marginBottom: 10 }}>
                <button onClick={() => toggleExpand(job.id)} style={{
                  display: 'flex', alignItems: 'center', gap: 6,
                  background: 'none', border: 'none', cursor: 'pointer',
                  color: 'var(--text-3)', fontSize: 11, padding: '4px 0', marginBottom: 6,
                }}>
                  {isExpanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                  <Layers size={11} />
                  {subJobs.length} chunk{subJobs.length !== 1 ? 's' : ''} · {subJobs.filter(s => s.status === 'completed' || s.status === 'completed_with_errors').length} done
                </button>

                {isExpanded && (
                  <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))',
                    gap: 6,
                  }}>
                    {subJobs.map((sj, idx) => {
                      const sjPct = sj.total_urls > 0
                        ? Math.round(((sj.processed + sj.failed) / sj.total_urls) * 100)
                        : 0
                      const sjColor = subStatusColor(sj.status)
                      return (
                        <div key={sj.id} style={{
                          padding: '8px 10px', borderRadius: 7,
                          border: `1px solid ${sj.status === 'running' ? 'rgba(99,102,241,0.3)' : sj.status === 'scraped' ? 'rgba(234,179,8,0.35)' : 'var(--border-1)'}`,
                          background: 'var(--bg-elevated)',
                        }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 5 }}>
                            <span style={{ color: sjColor }}>{subStatusIcon(sj.status)}</span>
                            <span style={{ fontSize: 10, fontWeight: 700, color: sjColor }}>
                              Chunk {idx + 1}
                            </span>
                            <span style={{ fontSize: 9, color: 'var(--text-3)', marginLeft: 'auto' }}>
                              {sj.processed}/{sj.total_urls}
                            </span>
                          </div>
                          <div style={{ height: 4, borderRadius: 2, background: 'var(--bg-base)', overflow: 'hidden' }}>
                            <div style={{
                              height: '100%', borderRadius: 2, width: `${sjPct}%`,
                              background: `linear-gradient(90deg,${sjColor}88,${sjColor})`,
                              transition: 'width 0.8s ease',
                            }} />
                          </div>
                          {sj.status === 'scraped' && (
                            <div style={{ fontSize: 9, color: '#eab308', marginTop: 3 }}>
                              Scraped · LLM processing…
                            </div>
                          )}
                          {sj.failed > 0 && (
                            <div style={{ fontSize: 9, color: '#ef4444', marginTop: 3 }}>
                              {sj.failed} failed
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            )}

            {/* Actions */}
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              {done > 0 && (
                <GhostBtn onClick={() => onSelectJob(job.id)} style={{ fontSize: 11 }}>
                  <List size={11} /> View {done} lead(s) <ArrowRight size={11} />
                </GhostBtn>
              )}
              <div style={{ marginLeft: 'auto', display: 'flex', gap: 6, alignItems: 'center' }}>
                {/* Stop — visible while job is active */}
                {['running','pending','fallback'].includes(job.status) && (
                  <button
                    onClick={() => handleStop(job.id)}
                    disabled={stopping[job.id]}
                    title="Stop this job and cancel BrightData snapshots"
                    style={{
                      padding: '5px 13px', borderRadius: 6, fontSize: 11, fontWeight: 600,
                      cursor: stopping[job.id] ? 'not-allowed' : 'pointer',
                      border: '1px solid rgba(245,158,11,0.4)', background: 'rgba(245,158,11,0.08)',
                      color: '#f59e0b', opacity: stopping[job.id] ? 0.5 : 1,
                      display: 'flex', alignItems: 'center', gap: 5,
                    }}
                  >
                    <svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>
                    {stopping[job.id] ? 'Stopping…' : 'Stop'}
                  </button>
                )}

                {/* Rerun — visible when stopped/failed/cancelled/stale */}
                {['cancelled','failed','completed_with_errors','stale'].includes(job.status) && (
                  <button
                    onClick={() => handleRerun(job.id)}
                    disabled={rerunning[job.id]}
                    title="Rerun this job via BrightData"
                    style={{
                      padding: '5px 13px', borderRadius: 6, fontSize: 11, fontWeight: 600,
                      cursor: rerunning[job.id] ? 'not-allowed' : 'pointer',
                      border: '1px solid rgba(99,102,241,0.4)', background: 'rgba(99,102,241,0.1)',
                      color: '#a5b4fc', opacity: rerunning[job.id] ? 0.5 : 1,
                      display: 'flex', alignItems: 'center', gap: 5,
                    }}
                  >
                    <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
                      strokeLinecap="round" strokeLinejoin="round"
                      style={rerunning[job.id] ? { animation: 'spin 0.8s linear infinite' } : {}}>
                      <polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-4.5"/>
                    </svg>
                    {rerunning[job.id] ? 'Rerunning…' : 'Rerun'}
                  </button>
                )}

                <button
                  onClick={() => handleDelete(job.id)}
                  disabled={deleting[job.id]}
                  style={{
                    padding: '5px 13px', borderRadius: 6, fontSize: 11, fontWeight: 600,
                    cursor: deleting[job.id] ? 'not-allowed' : 'pointer',
                    border: '1px solid rgba(239,68,68,0.3)', background: 'rgba(239,68,68,0.06)',
                    color: '#ef4444', opacity: deleting[job.id] ? 0.5 : 1,
                  }}
                >
                  {deleting[job.id] ? 'Deleting…' : 'Delete'}
                </button>
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}
