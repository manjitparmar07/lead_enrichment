// EmailEnrichmentPage.jsx — Standalone Email Enrichment
import { useState, useEffect, useRef, useCallback } from 'react'
import toast from 'react-hot-toast'

const API = `${import.meta.env.VITE_BACKEND_URL || 'https://api-lead-enrichment-worksbuddy.lbmdemo.com'}/api`
const POLL_MS = 3000

function getOrgId() {
  try {
    const token = localStorage.getItem('wb_ai_token')
    if (!token) return 'default'
    const payload = JSON.parse(atob(token.split('.')[1]))
    return payload.org_id || payload.organization_id || 'default'
  } catch { return 'default' }
}

function authHeaders() {
  const token = localStorage.getItem('wb_ai_token') || ''
  return { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }
}

function fmtDate(iso) {
  if (!iso) return '—'
  try { return new Date(iso).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) }
  catch { return iso }
}

// ── Styles ──────────────────────────────────────────────────────────────────

const card = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border-1)',
  borderRadius: 14,
  padding: '24px 28px',
}

const btn = (variant = 'primary', disabled = false) => ({
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '8px 16px', borderRadius: 9, fontSize: 12, fontWeight: 600,
  cursor: disabled ? 'not-allowed' : 'pointer',
  opacity: disabled ? 0.5 : 1,
  transition: 'all 0.15s',
  border: variant === 'ghost' ? '1px solid var(--border-1)' : 'none',
  background: variant === 'primary' ? 'linear-gradient(135deg,#6366f1,#8b5cf6)'
    : variant === 'danger'  ? '#ef4444'
    : 'transparent',
  color: variant === 'ghost' ? 'var(--text-2)' : '#fff',
})

const tag = (color = 'blue') => {
  const map = {
    blue:   { bg: 'rgba(99,102,241,0.12)',  color: '#a5b4fc' },
    green:  { bg: 'rgba(16,185,129,0.12)',  color: '#6ee7b7' },
    amber:  { bg: 'rgba(245,158,11,0.12)',  color: '#fcd34d' },
    red:    { bg: 'rgba(239,68,68,0.12)',   color: '#fca5a5' },
    gray:   { bg: 'rgba(148,163,184,0.1)',  color: '#94a3b8' },
  }
  const t = map[color] || map.gray
  return {
    display: 'inline-flex', alignItems: 'center', padding: '2px 8px',
    borderRadius: 5, fontSize: 10, fontWeight: 700,
    background: t.bg, color: t.color,
  }
}

// ── Progress bar ─────────────────────────────────────────────────────────────

function ProgressBar({ pct }) {
  return (
    <div style={{ background: 'var(--border-1)', borderRadius: 999, height: 6, overflow: 'hidden' }}>
      <div style={{
        width: `${Math.min(pct, 100)}%`, height: '100%',
        background: 'linear-gradient(90deg,#6366f1,#8b5cf6)',
        transition: 'width 0.4s ease',
        borderRadius: 999,
      }} />
    </div>
  )
}

// ── Source badge ─────────────────────────────────────────────────────────────

function SourceBadge({ source }) {
  const colors = { hunter: 'blue', apollo: 'green', dropcontact: 'amber', pdl: 'amber', pattern_guess: 'gray' }
  return <span style={tag(colors[source] || 'gray')}>{source || '—'}</span>
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function EmailEnrichmentPage() {
  const [candidates, setCandidates] = useState({ total: 0, leads: [] })
  const [loadingCandidates, setLoadingCandidates] = useState(true)
  const [job, setJob] = useState(null)            // active job state
  const [running, setRunning] = useState(false)
  const [limit, setLimit] = useState(500)
  const [regenerating, setRegenerating] = useState({})  // { lead_id: true }
  const pollRef = useRef(null)

  // ── Load candidates ────────────────────────────────────────────────────────
  const loadCandidates = useCallback(async () => {
    setLoadingCandidates(true)
    try {
      const res = await fetch(`${API}/email-enrich/candidates?per_page=50`, { headers: authHeaders() })
      if (!res.ok) throw new Error(await res.text())
      const data = await res.json()
      setCandidates(data)
    } catch (e) {
      toast.error('Failed to load candidates: ' + e.message)
    } finally {
      setLoadingCandidates(false)
    }
  }, [])

  useEffect(() => { loadCandidates() }, [loadCandidates])

  // ── Poll job ───────────────────────────────────────────────────────────────
  const pollJob = useCallback(async (jobId) => {
    try {
      const res = await fetch(`${API}/email-enrich/jobs/${jobId}`, { headers: authHeaders() })
      if (!res.ok) return
      const data = await res.json()
      setJob(data)
      if (data.status === 'completed' || data.status === 'failed') {
        clearInterval(pollRef.current)
        setRunning(false)
        toast.success(`Done! Found ${data.found} emails out of ${data.total} leads.`)
        loadCandidates()   // refresh candidate count
      }
    } catch { /* ignore */ }
  }, [loadCandidates])

  useEffect(() => () => clearInterval(pollRef.current), [])

  // ── Start job ──────────────────────────────────────────────────────────────
  const startJob = async () => {
    setRunning(true)
    setJob(null)
    try {
      const res = await fetch(`${API}/email-enrich/start`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ lead_ids: [], limit }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      if (!data.job_id) {
        toast(data.message || 'No eligible leads found.')
        setRunning(false)
        return
      }
      toast.success(`Job started — enriching ${data.total} leads`)
      setJob({ job_id: data.job_id, status: 'running', total: data.total, processed: 0, found: 0, failed: 0, results: [] })
      pollRef.current = setInterval(() => pollJob(data.job_id), POLL_MS)
    } catch (e) {
      toast.error('Failed to start: ' + e.message)
      setRunning(false)
    }
  }

  // ── Regenerate single lead email ───────────────────────────────────────────
  const regenerateEmail = async (leadId, name) => {
    setRegenerating(prev => ({ ...prev, [leadId]: true }))
    try {
      const res = await fetch(`${API}/leads/view/email`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ leadenrich_id: leadId }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      if (data.source === 'apollo_credit_exhausted') {
        toast.error('Apollo credit balance exhausted. Please recharge your Apollo plan to continue email enrichment.', { duration: 6000 })
      } else if (data.email) {
        toast.success(`Email found: ${data.email}`)
        loadCandidates()
      } else {
        toast(`No email found for ${name || leadId}`)
      }
    } catch (e) {
      toast.error('Regenerate failed: ' + e.message)
    } finally {
      setRegenerating(prev => ({ ...prev, [leadId]: false }))
    }
  }

  const pct = job ? Math.round(((job.processed || 0) / Math.max(job.total || 1, 1)) * 100) : 0

  return (
    <div style={{ padding: '28px 32px', maxWidth: 1100, margin: '0 auto' }}>

      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, color: 'var(--text-1)', margin: 0 }}>
          Email Enrichment
        </h1>
        <p style={{ fontSize: 13, color: 'var(--text-3)', margin: '4px 0 0' }}>
          Automatically find missing work emails for imported leads using Hunter → Apollo → pattern waterfall.
        </p>
      </div>

      {/* Stats row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 20 }}>
        <StatCard
          label="Leads without email"
          value={loadingCandidates ? '…' : candidates.total.toLocaleString()}
          color="#a5b4fc"
        />
        <StatCard
          label="Emails found (this job)"
          value={job ? job.found?.toLocaleString() ?? '0' : '—'}
          color="#6ee7b7"
        />
        <StatCard
          label="Success rate"
          value={job && job.processed > 0 ? `${Math.round((job.found / job.processed) * 100)}%` : '—'}
          color="#fcd34d"
        />
      </div>

      {/* Control panel */}
      <div style={{ ...card, marginBottom: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 4 }}>
              Batch size
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-3)' }}>
              How many leads (without emails) to process in this run. Leads are ordered by score (highest first).
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <select
              value={limit}
              onChange={e => setLimit(Number(e.target.value))}
              disabled={running}
              style={{
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
                color: 'var(--text-1)', borderRadius: 8, padding: '6px 10px',
                fontSize: 12, cursor: running ? 'not-allowed' : 'pointer',
              }}
            >
              {[50, 100, 250, 500, 1000, 2000].map(n => (
                <option key={n} value={n}>{n.toLocaleString()} leads</option>
              ))}
            </select>
            <button
              style={btn('primary', running || candidates.total === 0)}
              disabled={running || candidates.total === 0}
              onClick={startJob}
            >
              {running ? (
                <>
                  <Spinner />
                  Running…
                </>
              ) : (
                <>
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <polygon points="5 3 19 12 5 21 5 3"/>
                  </svg>
                  Start Email Enrichment
                </>
              )}
            </button>
          </div>
        </div>

        {/* Progress */}
        {job && (
          <div style={{ marginTop: 20, padding: 16, background: 'var(--bg-base)', borderRadius: 10, border: '1px solid var(--border-1)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8, fontSize: 12 }}>
              <span style={{ color: 'var(--text-2)', fontWeight: 600 }}>
                {job.status === 'completed' ? 'Completed' : 'Processing…'}
              </span>
              <span style={{ color: 'var(--text-3)' }}>
                {job.processed?.toLocaleString()} / {job.total?.toLocaleString()} · {job.found} found · {job.failed} skipped
              </span>
            </div>
            <ProgressBar pct={pct} />
            <div style={{ display: 'flex', gap: 8, marginTop: 10, flexWrap: 'wrap' }}>
              <span style={tag(job.status === 'completed' ? 'green' : job.status === 'failed' ? 'red' : 'blue')}>
                {job.status?.toUpperCase()}
              </span>
              <span style={{ fontSize: 11, color: 'var(--text-3)' }}>{pct}%</span>
              {job.completed_at && (
                <span style={{ fontSize: 11, color: 'var(--text-3)', marginLeft: 'auto' }}>
                  Finished {fmtDate(job.completed_at)}
                </span>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Results table (job results preview) */}
      {job && job.results && job.results.length > 0 && (
        <div style={{ ...card, marginBottom: 20 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 14 }}>
            Emails found ({job.results.length} shown)
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border-1)' }}>
                  {['Name', 'Company', 'Email', 'Source', 'Verified'].map(h => (
                    <th key={h} style={{ padding: '6px 10px', textAlign: 'left', color: 'var(--text-3)', fontWeight: 600, whiteSpace: 'nowrap' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {job.results.map((r, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid var(--border-1)' }}>
                    <td style={{ padding: '8px 10px', color: 'var(--text-1)', fontWeight: 500 }}>{r.name || '—'}</td>
                    <td style={{ padding: '8px 10px', color: 'var(--text-2)' }}>{r.company || '—'}</td>
                    <td style={{ padding: '8px 10px' }}>
                      <a href={`mailto:${r.email}`} style={{ color: '#a5b4fc', textDecoration: 'none', fontFamily: 'monospace', fontSize: 11 }}>
                        {r.email}
                      </a>
                    </td>
                    <td style={{ padding: '8px 10px' }}><SourceBadge source={r.source} /></td>
                    <td style={{ padding: '8px 10px' }}>
                      <span style={tag(r.verified ? 'green' : 'gray')}>{r.verified ? 'YES' : 'NO'}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Candidates table */}
      <div style={card}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>
            Leads without email
            {candidates.total > 0 && (
              <span style={{ ...tag('amber'), marginLeft: 8 }}>{candidates.total.toLocaleString()}</span>
            )}
          </div>
          <button style={btn('ghost', loadingCandidates)} onClick={loadCandidates} disabled={loadingCandidates}>
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
            </svg>
            Refresh
          </button>
        </div>

        {loadingCandidates ? (
          <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
            <Spinner /> Loading…
          </div>
        ) : candidates.leads.length === 0 ? (
          <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
            {candidates.total === 0
              ? 'All leads already have emails.'
              : 'No leads with enrichable data found (need first name + company domain).'}
          </div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border-1)' }}>
                  {['Name', 'Title', 'Company', 'LinkedIn', 'Can Enrich', 'Action'].map(h => (
                    <th key={h} style={{ padding: '6px 10px', textAlign: 'left', color: 'var(--text-3)', fontWeight: 600, whiteSpace: 'nowrap' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {candidates.leads.map((l, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid var(--border-1)' }}>
                    <td style={{ padding: '8px 10px', color: 'var(--text-1)', fontWeight: 500 }}>{l.name || '—'}</td>
                    <td style={{ padding: '8px 10px', color: 'var(--text-3)' }}>{l.title || '—'}</td>
                    <td style={{ padding: '8px 10px', color: 'var(--text-2)' }}>{l.company || '—'}</td>
                    <td style={{ padding: '8px 10px' }}>
                      {l.linkedin_url ? (
                        <a href={l.linkedin_url} target="_blank" rel="noopener noreferrer"
                          style={{ color: '#a5b4fc', fontSize: 11, textDecoration: 'none' }}>
                          View
                        </a>
                      ) : '—'}
                    </td>
                    <td style={{ padding: '8px 10px' }}>
                      <span style={tag(l.has_domain ? 'green' : 'red')}>
                        {l.has_domain ? 'YES' : 'NO DOMAIN'}
                      </span>
                    </td>
                    <td style={{ padding: '8px 10px' }}>
                      <button
                        style={btn('ghost', regenerating[l.id])}
                        disabled={regenerating[l.id]}
                        onClick={() => regenerateEmail(l.id, l.name)}
                      >
                        {regenerating[l.id] ? <><Spinner /> Finding…</> : (
                          <>
                            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                              <polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
                            </svg>
                            Regenerate
                          </>
                        )}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {candidates.total > candidates.leads.length && (
              <div style={{ padding: '10px 10px 0', color: 'var(--text-3)', fontSize: 11 }}>
                Showing {candidates.leads.length} of {candidates.total.toLocaleString()} leads without email.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// ── Sub-components ────────────────────────────────────────────────────────────

function StatCard({ label, value, color }) {
  return (
    <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 12, padding: '16px 20px' }}>
      <div style={{ fontSize: 11, color: 'var(--text-3)', fontWeight: 600, marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        {label}
      </div>
      <div style={{ fontSize: 26, fontWeight: 700, color }}>
        {value}
      </div>
    </div>
  )
}

function Spinner() {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
      style={{ animation: 'spin 0.8s linear infinite', display: 'inline-block' }}>
      <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round"/>
      <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
    </svg>
  )
}
