// StoragePage.jsx — View all imported + enriched leads and unmapped field data
import { useState, useEffect, useCallback } from 'react'
import toast from 'react-hot-toast'

const API_BASE = `${import.meta.env.VITE_BACKEND_URL || 'https://api-lead-enrichment-worksbuddy.lbmdemo.com'}/api`


// ── Styles ─────────────────────────────────────────────────────────────────

const card = {
  background: 'var(--bg-card)', border: '1px solid var(--border-1)',
  borderRadius: 14, padding: '20px 24px',
}

function Badge({ label, color = '#6b7280', bg }) {
  return (
    <span style={{
      fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 5,
      background: bg || `${color}22`, color,
      letterSpacing: '0.04em', textTransform: 'uppercase',
    }}>{label}</span>
  )
}

function sourceColor(src) {
  if (!src) return '#6b7280'
  if (src.includes('bright') || src.includes('linkedin')) return '#6366f1'
  if (src.includes('apollo')) return '#3b82f6'
  if (src.includes('hunter') || src.includes('email')) return '#10b981'
  return '#f59e0b'
}

function shortId(id) {
  if (!id) return '—'
  if (id.startsWith('http')) return id.slice(-20)  // linkedin url
  return id.slice(0, 8) + '…'
}

function copyText(text) {
  navigator.clipboard?.writeText(text).then(() => toast.success('Copied'))
}

// ── Stat card ──────────────────────────────────────────────────────────────

function StatCard({ label, value, sub, color = 'var(--text-1)' }) {
  return (
    <div style={{ ...card, flex: 1, minWidth: 140 }}>
      <div style={{ fontSize: 28, fontWeight: 800, color, marginBottom: 4 }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
      </div>
      <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-2)' }}>{label}</div>
      {sub && <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

// ── Pagination ─────────────────────────────────────────────────────────────

function Pagination({ page, pages, onChange }) {
  if (pages <= 1) return null
  return (
    <div style={{ display: 'flex', gap: 6, alignItems: 'center', justifyContent: 'center', marginTop: 20 }}>
      <button onClick={() => onChange(1)} disabled={page === 1} style={pgBtn(page === 1)}>«</button>
      <button onClick={() => onChange(page - 1)} disabled={page === 1} style={pgBtn(page === 1)}>‹</button>
      {Array.from({ length: Math.min(7, pages) }, (_, i) => {
        let p
        if (pages <= 7) p = i + 1
        else if (page <= 4) p = i + 1
        else if (page >= pages - 3) p = pages - 6 + i
        else p = page - 3 + i
        return (
          <button key={p} onClick={() => onChange(p)} style={pgBtn(false, p === page)}>
            {p}
          </button>
        )
      })}
      <button onClick={() => onChange(page + 1)} disabled={page === pages} style={pgBtn(page === pages)}>›</button>
      <button onClick={() => onChange(pages)} disabled={page === pages} style={pgBtn(page === pages)}>»</button>
    </div>
  )
}

const pgBtn = (disabled, active = false) => ({
  padding: '5px 10px', borderRadius: 7, border: '1px solid var(--border-1)',
  background: active ? '#6366f1' : 'transparent',
  color: active ? '#fff' : disabled ? 'var(--text-3)' : 'var(--text-2)',
  cursor: disabled ? 'not-allowed' : 'pointer', fontSize: 12, fontWeight: 600,
  opacity: disabled ? 0.4 : 1,
})

// ── Leads tab ──────────────────────────────────────────────────────────────

function LeadsTab() {
  const [data, setData] = useState({ leads: [], total: 0, page: 1, pages: 1 })
  const [page, setPage] = useState(1)
  const [q, setQ] = useState('')
  const [source, setSource] = useState('')
  const [loading, setLoading] = useState(false)
  const [expanded, setExpanded] = useState(null)
  const [unmapped, setUnmapped] = useState({})

  const load = useCallback(async (pg = 1, query = q, src = source) => {
    setLoading(true)
    try {
      const params = new URLSearchParams({ page: pg, limit: 50, q: query, source: src })
      const res = await fetch(`${API_BASE}/storage/leads?${params}`)
      if (!res.ok) throw new Error(res.statusText)
      setData(await res.json())
    } catch (err) {
      toast.error(`Failed to load leads: ${err.message}`)
    } finally { setLoading(false) }
  }, [])

  useEffect(() => { load(page, q, source) }, [page, source])

  const handleSearch = (e) => {
    e.preventDefault()
    setPage(1); load(1, q, source)
  }

  const loadUnmapped = async (leadId) => {
    if (expanded === leadId) { setExpanded(null); return }
    setExpanded(leadId)
    if (unmapped[leadId]) return
    try {
      const res = await fetch(`${API_BASE}/storage/unmapped/${encodeURIComponent(leadId)}`)
      if (!res.ok) return
      const d = await res.json()
      setUnmapped(u => ({ ...u, [leadId]: d.records }))
    } catch { }
  }

  return (
    <div>
      {/* Filters */}
      <form onSubmit={handleSearch} style={{ display: 'flex', gap: 10, marginBottom: 20, flexWrap: 'wrap' }}>
        <input
          type="text" value={q} onChange={e => setQ(e.target.value)}
          placeholder="Search name, email, company…"
          style={{
            flex: 1, minWidth: 200, background: 'var(--bg-base)', border: '1px solid var(--border-1)',
            borderRadius: 9, padding: '8px 14px', fontSize: 13, color: 'var(--text-1)',
          }}
        />
        <select
          value={source} onChange={e => { setSource(e.target.value); setPage(1); load(1, q, e.target.value) }}
          style={{ background: 'var(--bg-base)', border: '1px solid var(--border-1)', borderRadius: 9, padding: '8px 12px', fontSize: 13, color: 'var(--text-1)', cursor: 'pointer' }}
        >
          <option value="">All sources</option>
          {['brightdata_linkedin','apollo_csv','file_import','hunter','apollo'].map(s => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <button type="submit" style={{
          padding: '8px 18px', borderRadius: 9, border: 'none', background: '#6366f1',
          color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer',
        }}>Search</button>
      </form>

      {/* Table */}
      <div style={{ border: '1px solid var(--border-1)', borderRadius: 12, overflow: 'hidden' }}>
        {/* Header */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: '130px 1fr 1fr 1fr 100px 100px 90px 70px',
          background: 'var(--bg-sidebar)', padding: '8px 16px',
          fontSize: 10, fontWeight: 700, color: 'var(--text-3)',
          letterSpacing: '0.06em', textTransform: 'uppercase',
          borderBottom: '1px solid var(--border-1)',
        }}>
          <span>Lead ID</span>
          <span>Name</span>
          <span>Email</span>
          <span>Company</span>
          <span>Source</span>
          <span>Status</span>
          <span>Score</span>
          <span>Fields</span>
        </div>

        {loading && (
          <div style={{ padding: '40px', textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>Loading…</div>
        )}

        {!loading && data.leads.length === 0 && (
          <div style={{ padding: '40px', textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
            No leads found. Import a file to get started.
          </div>
        )}

        {!loading && data.leads.map(lead => (
          <div key={lead.id}>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: '130px 1fr 1fr 1fr 100px 100px 90px 70px',
                padding: '10px 16px', borderTop: '1px solid var(--border-1)',
                alignItems: 'center', cursor: 'pointer',
                background: expanded === lead.id ? 'rgba(99,102,241,0.04)' : undefined,
              }}
              onClick={() => loadUnmapped(lead.id)}
            >
              {/* Lead ID */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span
                  style={{ fontSize: 11, fontFamily: 'monospace', color: '#a5b4fc', cursor: 'pointer' }}
                  title={lead.id}
                  onClick={e => { e.stopPropagation(); copyText(lead.id) }}
                >
                  {shortId(lead.id)}
                </span>
              </div>

              {/* Name */}
              <div>
                <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {lead.name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || '—'}
                </div>
                {lead.title && <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 1 }}>{lead.title}</div>}
              </div>

              {/* Email */}
              <div style={{ fontSize: 11, color: 'var(--text-2)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {lead.work_email || '—'}
              </div>

              {/* Company */}
              <div style={{ fontSize: 12, color: 'var(--text-2)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {lead.company || '—'}
              </div>

              {/* Source */}
              <div>
                {lead.enrichment_source
                  ? <Badge label={lead.enrichment_source.replace(/_/g,' ')} color={sourceColor(lead.enrichment_source)} />
                  : <span style={{ fontSize: 11, color: 'var(--text-3)' }}>—</span>
                }
              </div>

              {/* Status */}
              <div>
                {lead.status === 'enriched'
                  ? <Badge label="enriched" color="#10b981" />
                  : <Badge label={lead.status || 'pending'} color="#6b7280" />
                }
              </div>

              {/* Score */}
              <div style={{ fontSize: 12, fontWeight: 700, color: lead.total_score > 60 ? '#10b981' : lead.total_score > 30 ? '#f59e0b' : 'var(--text-3)' }}>
                {lead.total_score > 0 ? lead.total_score : '—'}
              </div>

              {/* Unmapped toggle */}
              <div style={{ textAlign: 'right' }}>
                <span style={{ fontSize: 10, color: '#a5b4fc', fontWeight: 600 }}>
                  {expanded === lead.id ? '▲' : '▼'} fields
                </span>
              </div>
            </div>

            {/* Expanded: unmapped data */}
            {expanded === lead.id && (
              <div style={{ background: 'rgba(99,102,241,0.04)', borderTop: '1px solid var(--border-1)', padding: '12px 24px' }}>
                {!unmapped[lead.id] && <div style={{ fontSize: 12, color: 'var(--text-3)' }}>Loading unmapped fields…</div>}
                {unmapped[lead.id] && unmapped[lead.id].length === 0 && (
                  <div style={{ fontSize: 12, color: 'var(--text-3)' }}>No unmapped fields for this lead.</div>
                )}
                {unmapped[lead.id] && unmapped[lead.id].map(rec => {
                  let parsed = {}
                  try { parsed = JSON.parse(rec.raw_data) } catch { }
                  return (
                    <div key={rec.id} style={{ marginBottom: 12 }}>
                      <div style={{ fontSize: 11, color: 'var(--text-3)', marginBottom: 6 }}>
                        Source: <strong style={{ color: 'var(--text-2)' }}>{rec.source_file || '—'}</strong>
                        &nbsp;·&nbsp;{rec.created_at?.slice(0, 10)}
                      </div>
                      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        {Object.entries(parsed).map(([k, v]) => (
                          <div key={k} style={{
                            background: 'var(--bg-card)', border: '1px solid var(--border-1)',
                            borderRadius: 8, padding: '4px 10px',
                          }}>
                            <div style={{ fontSize: 9, color: 'var(--text-3)', fontWeight: 700, textTransform: 'uppercase', marginBottom: 2 }}>{k}</div>
                            <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{String(v)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        ))}
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16 }}>
        <span style={{ fontSize: 12, color: 'var(--text-3)' }}>
          {data.total.toLocaleString()} total leads · Page {page} of {data.pages}
        </span>
        <Pagination page={page} pages={data.pages} onChange={p => setPage(p)} />
      </div>
    </div>
  )
}

// ── Unmapped tab ────────────────────────────────────────────────────────────

function UnmappedTab() {
  const [data, setData] = useState({ records: [], total: 0, page: 1, pages: 1 })
  const [page, setPage] = useState(1)
  const [sourceFile, setSourceFile] = useState('')
  const [loading, setLoading] = useState(false)
  const [expandedId, setExpandedId] = useState(null)

  const load = useCallback(async (pg = 1, sf = sourceFile) => {
    setLoading(true)
    try {
      const params = new URLSearchParams({ page: pg, limit: 50, source_file: sf })
      const res = await fetch(`${API_BASE}/storage/unmapped?${params}`)
      if (!res.ok) throw new Error(res.statusText)
      setData(await res.json())
    } catch (err) { toast.error(`Failed to load: ${err.message}`) }
    finally { setLoading(false) }
  }, [sourceFile])

  useEffect(() => { load(page) }, [page])

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', gap: 10 }}>
        <input
          type="text" value={sourceFile} onChange={e => setSourceFile(e.target.value)}
          placeholder="Filter by source file name…"
          onKeyDown={e => e.key === 'Enter' && (setPage(1), load(1, sourceFile))}
          style={{
            flex: 1, background: 'var(--bg-base)', border: '1px solid var(--border-1)',
            borderRadius: 9, padding: '8px 14px', fontSize: 13, color: 'var(--text-1)',
          }}
        />
        <button
          onClick={() => { setPage(1); load(1, sourceFile) }}
          style={{ padding: '8px 18px', borderRadius: 9, border: 'none', background: '#6366f1', color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }}
        >Search</button>
      </div>

      <div style={{ border: '1px solid var(--border-1)', borderRadius: 12, overflow: 'hidden' }}>
        <div style={{
          display: 'grid', gridTemplateColumns: '130px 130px 1fr 120px',
          background: 'var(--bg-sidebar)', padding: '8px 16px',
          fontSize: 10, fontWeight: 700, color: 'var(--text-3)',
          letterSpacing: '0.06em', textTransform: 'uppercase',
          borderBottom: '1px solid var(--border-1)',
        }}>
          <span>Lead ID</span><span>Source File</span><span>Unmapped Fields</span><span>Saved At</span>
        </div>

        {loading && <div style={{ padding: '40px', textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>Loading…</div>}

        {!loading && data.records.length === 0 && (
          <div style={{ padding: '40px', textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
            No unmapped records yet. Import a file with extra columns to see them here.
          </div>
        )}

        {!loading && data.records.map(rec => {
          let parsed = {}
          try { parsed = JSON.parse(rec.raw_data) } catch { }
          const keys = Object.keys(parsed)
          const isOpen = expandedId === rec.id

          return (
            <div key={rec.id}>
              <div
                style={{
                  display: 'grid', gridTemplateColumns: '130px 130px 1fr 120px',
                  padding: '10px 16px', borderTop: '1px solid var(--border-1)',
                  alignItems: 'center', cursor: 'pointer',
                  background: isOpen ? 'rgba(99,102,241,0.04)' : undefined,
                }}
                onClick={() => setExpandedId(isOpen ? null : rec.id)}
              >
                <span
                  style={{ fontSize: 11, fontFamily: 'monospace', color: '#a5b4fc', cursor: 'pointer' }}
                  title={rec.lead_id}
                  onClick={e => { e.stopPropagation(); copyText(rec.lead_id) }}
                >
                  {shortId(rec.lead_id)}
                </span>
                <span style={{ fontSize: 12, color: 'var(--text-2)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {rec.source_file || '—'}
                </span>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {keys.slice(0, 5).map(k => (
                    <span key={k} style={{ fontSize: 10, background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', borderRadius: 4, padding: '2px 7px', fontWeight: 600 }}>{k}</span>
                  ))}
                  {keys.length > 5 && <span style={{ fontSize: 10, color: 'var(--text-3)' }}>+{keys.length - 5} more</span>}
                </div>
                <span style={{ fontSize: 11, color: 'var(--text-3)' }}>{rec.created_at?.slice(0, 10)}</span>
              </div>

              {isOpen && (
                <div style={{ background: 'rgba(99,102,241,0.04)', borderTop: '1px solid var(--border-1)', padding: '12px 24px' }}>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {Object.entries(parsed).map(([k, v]) => (
                      <div key={k} style={{ background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 8, padding: '6px 12px', minWidth: 100 }}>
                        <div style={{ fontSize: 9, color: 'var(--text-3)', fontWeight: 700, textTransform: 'uppercase', marginBottom: 3 }}>{k}</div>
                        <div style={{ fontSize: 12, color: 'var(--text-1)', wordBreak: 'break-all' }}>{String(v)}</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )
        })}
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16 }}>
        <span style={{ fontSize: 12, color: 'var(--text-3)' }}>
          {data.total.toLocaleString()} unmapped records · Page {page} of {data.pages}
        </span>
        <Pagination page={page} pages={data.pages} onChange={p => setPage(p)} />
      </div>
    </div>
  )
}

// ── Main ───────────────────────────────────────────────────────────────────

export default function StoragePage() {
  const [tab, setTab] = useState('leads')
  const [stats, setStats] = useState(null)

  useEffect(() => {
    fetch(`${API_BASE}/storage/stats`)
      .then(r => r.json())
      .then(setStats)
      .catch(() => { })
  }, [])

  const tabs = [
    { id: 'leads', label: 'All Leads' },
    { id: 'unmapped', label: 'Unmapped Fields' },
  ]

  return (
    <div style={{ padding: '32px 36px', maxWidth: 1200, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ marginBottom: 28 }}>
        <h1 style={{ fontSize: 22, fontWeight: 800, color: 'var(--text-1)', margin: 0 }}>Storage</h1>
        <p style={{ fontSize: 13, color: 'var(--text-3)', marginTop: 6 }}>
          All imported and enriched leads with their Lead IDs, plus unmapped field data from file imports.
        </p>
      </div>

      {/* Stats row */}
      {stats && (
        <div style={{ display: 'flex', gap: 14, marginBottom: 28, flexWrap: 'wrap' }}>
          <StatCard label="Total Leads" value={stats.total_leads} color="#a5b4fc" />
          <StatCard label="Enriched" value={stats.enriched_leads} color="#10b981" sub="score > 0" />
          <StatCard label="Imported" value={stats.imported_leads} color="#6366f1" />
          <StatCard label="Unmapped Records" value={stats.unmapped_records} color="#f59e0b" sub="extra columns saved" />
        </div>
      )}

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20, background: 'var(--bg-sidebar)', borderRadius: 10, padding: 4, width: 'fit-content' }}>
        {tabs.map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            style={{
              padding: '8px 20px', borderRadius: 8, border: 'none', fontSize: 13, fontWeight: 600,
              cursor: 'pointer',
              background: tab === t.id ? 'var(--bg-card)' : 'transparent',
              color: tab === t.id ? 'var(--text-1)' : 'var(--text-3)',
              boxShadow: tab === t.id ? '0 1px 4px rgba(0,0,0,0.2)' : 'none',
              transition: 'all 0.15s',
            }}
          >{t.label}</button>
        ))}
      </div>

      <div style={card}>
        {tab === 'leads' ? <LeadsTab /> : <UnmappedTab />}
      </div>
    </div>
  )
}
