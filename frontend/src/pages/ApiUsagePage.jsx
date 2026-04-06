// ApiUsagePage.jsx — API Usage Analytics
import { useState, useEffect, useCallback } from 'react'

const API_BASE = import.meta.env.VITE_BACKEND_URL || 'http://localhost:4064'

const API_META = {
  brightdata:  { label: 'BrightData',   color: '#3b82f6', bg: 'rgba(59,130,246,0.12)'  },
  apollo:      { label: 'Apollo.io',    color: '#f59e0b', bg: 'rgba(245,158,11,0.12)'  },
  huggingface: { label: 'HuggingFace',  color: '#10b981', bg: 'rgba(16,185,129,0.12)'  },
  validemail:  { label: 'ValidEmail',   color: '#8b5cf6', bg: 'rgba(139,92,246,0.12)'  },
  dropcontact: { label: 'Dropcontact',  color: '#ef4444', bg: 'rgba(239,68,68,0.12)'   },
  pdl:         { label: 'People Data Labs', color: '#06b6d4', bg: 'rgba(6,182,212,0.12)' },
  zerobounce:  { label: 'ZeroBounce',   color: '#f97316', bg: 'rgba(249,115,22,0.12)'  },
}

function getMeta(api) {
  return API_META[api] || { label: api, color: '#94a3b8', bg: 'rgba(148,163,184,0.1)' }
}

function fmt(n) {
  if (n >= 1000) return (n / 1000).toFixed(1) + 'k'
  return String(n)
}

// ── Spinner ────────────────────────────────────────────────────────────────
function Spinner() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
      style={{ animation: 'spin 0.8s linear infinite', flexShrink: 0 }}>
      <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round"/>
    </svg>
  )
}

// ── Total card ─────────────────────────────────────────────────────────────
function TotalCard({ api, total, byType }) {
  const meta = getMeta(api)
  const types = Object.entries(byType || {})
  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border-1)',
      borderRadius: 12, padding: '18px 20px', display: 'flex', flexDirection: 'column', gap: 12,
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <div style={{
          width: 34, height: 34, borderRadius: 9, background: meta.bg,
          display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
        }}>
          <ApiIcon api={api} color={meta.color} />
        </div>
        <div>
          <div style={{ fontSize: 12, color: 'var(--text-3)', fontWeight: 600 }}>{meta.label}</div>
          <div style={{ fontSize: 24, fontWeight: 700, color: total === 0 ? 'var(--text-3)' : meta.color, lineHeight: 1.2 }}>
            {total === 0 ? '0' : fmt(total)}
          </div>
        </div>
        <div style={{
          marginLeft: 'auto', fontSize: 10, color: 'var(--text-3)',
          background: 'var(--bg-base)', border: '1px solid var(--border-1)',
          padding: '2px 8px', borderRadius: 20, fontWeight: 600,
        }}>calls</div>
      </div>

      {/* Breakdown by call_type or empty state */}
      {types.length > 0 ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          {types.map(([ct, cnt]) => {
            const pct = Math.round((cnt / total) * 100)
            return (
              <div key={ct}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                  <span style={{ fontSize: 11, color: 'var(--text-3)', fontFamily: 'monospace' }}>{ct}</span>
                  <span style={{ fontSize: 11, color: 'var(--text-2)', fontWeight: 600 }}>{fmt(cnt)}</span>
                </div>
                <div style={{ height: 4, borderRadius: 99, background: 'var(--bg-base)' }}>
                  <div style={{
                    height: '100%', borderRadius: 99, width: `${pct}%`,
                    background: meta.color, opacity: 0.7, transition: 'width 0.5s',
                  }} />
                </div>
              </div>
            )
          })}
        </div>
      ) : (
        <div style={{
          fontSize: 11, color: 'var(--text-3)', textAlign: 'center',
          padding: '6px 0', borderTop: '1px solid var(--border-1)',
          fontStyle: 'italic',
        }}>No calls yet</div>
      )}
    </div>
  )
}

// ── API icon ───────────────────────────────────────────────────────────────
function ApiIcon({ api, color }) {
  const s = { width: 16, height: 16, stroke: color, fill: 'none', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round' }
  if (api === 'brightdata') return (
    <svg viewBox="0 0 24 24" style={s}>
      <circle cx="12" cy="12" r="4"/>
      <path d="M12 2v2M12 20v2M2 12h2M20 12h2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M17.66 6.34l-1.41 1.41M4.93 19.07l1.41-1.41"/>
    </svg>
  )
  if (api === 'apollo') return (
    <svg viewBox="0 0 24 24" style={s}>
      <path d="M12 2L2 7l10 5 10-5-10-5z"/>
      <path d="M2 17l10 5 10-5M2 12l10 5 10-5"/>
    </svg>
  )
  if (api === 'huggingface') return (
    <svg viewBox="0 0 24 24" style={s}>
      <circle cx="12" cy="12" r="10"/>
      <path d="M8 14s1.5 2 4 2 4-2 4-2"/>
      <line x1="9" y1="9" x2="9.01" y2="9"/>
      <line x1="15" y1="9" x2="15.01" y2="9"/>
    </svg>
  )
  if (api === 'validemail') return (
    <svg viewBox="0 0 24 24" style={s}>
      <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/>
      <polyline points="22,6 12,13 2,6"/>
    </svg>
  )
  return (
    <svg viewBox="0 0 24 24" style={s}>
      <rect x="2" y="3" width="20" height="14" rx="2" ry="2"/>
      <line x1="8" y1="21" x2="16" y2="21"/>
      <line x1="12" y1="17" x2="12" y2="21"/>
    </svg>
  )
}

// ── Day badge colour ───────────────────────────────────────────────────────
function DayBadge({ days }) {
  const selected = 'rgba(99,102,241,0.15)'
  const border   = 'rgba(99,102,241,0.4)'
  return (
    <span style={{
      fontSize: 11, fontWeight: 700, padding: '3px 10px', borderRadius: 20,
      background: selected, border: `1px solid ${border}`, color: '#a5b4fc',
    }}>{days}d</span>
  )
}

// ── Main page ──────────────────────────────────────────────────────────────
export default function ApiUsagePage() {
  const [days, setDays]       = useState(30)
  const [summary, setSummary] = useState(null)
  const [detail, setDetail]   = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState(null)

  const load = useCallback(async (d) => {
    setLoading(true)
    setError(null)
    try {
      const token = localStorage.getItem('wb_ai_token')
      const headers = token ? { Authorization: `Bearer ${token}` } : {}
      const [s, det] = await Promise.all([
        fetch(`${API_BASE}/api/leads/analytics/api-usage?days=${d}`, { headers }).then(r => r.json()),
        fetch(`${API_BASE}/api/leads/analytics/api-usage/detail?days=${d}`, { headers }).then(r => r.json()),
      ])
      setSummary(s)
      setDetail(Array.isArray(det) ? det : [])
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load(days) }, [days, load])

  // Always show these APIs — even if 0 calls
  const KNOWN_APIS = ['brightdata', 'apollo', 'huggingface', 'validemail']

  const totals = summary?.totals || {}
  const byType = summary?.by_type || {}

  // Merge known APIs with whatever came from the server
  const serverApis = Object.keys(totals)
  const apis = [...new Set([...KNOWN_APIS, ...serverApis])]

  const totalCalls = Object.values(totals).reduce((a, b) => a + b, 0)

  // Group detail rows by date for daily chart
  const byDay = {}
  detail.forEach(r => {
    byDay[r.day] = (byDay[r.day] || 0) + r.count
  })
  const days_list = Object.keys(byDay).sort().reverse()
  const maxDay = Math.max(...Object.values(byDay), 1)

  return (
    <div style={{ padding: '24px 28px', maxWidth: 1100, margin: '0 auto' }}>
      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <div style={{
          width: 36, height: 36, borderRadius: 10,
          background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
        }}>
          <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/>
            <line x1="6" y1="20" x2="6" y2="14"/><line x1="2" y1="20" x2="22" y2="20"/>
          </svg>
        </div>
        <div>
          <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: 'var(--text-1)' }}>API Usage</h1>
          <p style={{ margin: 0, fontSize: 12, color: 'var(--text-3)' }}>
            BrightData · Apollo · HuggingFace · ValidEmail — call counts
          </p>
        </div>

        {/* Day selector */}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
          {[7, 14, 30, 90].map(d => (
            <button
              key={d}
              onClick={() => setDays(d)}
              style={{
                fontSize: 11, fontWeight: 700, padding: '5px 12px', borderRadius: 20, cursor: 'pointer',
                background: days === d ? 'rgba(99,102,241,0.15)' : 'transparent',
                border: `1px solid ${days === d ? 'rgba(99,102,241,0.4)' : 'var(--border-1)'}`,
                color: days === d ? '#a5b4fc' : 'var(--text-3)',
                transition: 'all 0.15s',
              }}
            >{d}d</button>
          ))}
          <button
            onClick={() => load(days)}
            style={{
              fontSize: 11, padding: '5px 10px', borderRadius: 20, cursor: 'pointer',
              background: 'transparent', border: '1px solid var(--border-1)', color: 'var(--text-3)',
              display: 'flex', alignItems: 'center', gap: 5,
            }}
          >
            {loading ? <Spinner /> : (
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                <path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>
              </svg>
            )}
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div style={{
          background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)',
          borderRadius: 10, padding: '10px 14px', marginBottom: 20,
          fontSize: 12, color: '#f87171',
        }}>{error}</div>
      )}

      {/* Grand total banner */}
      {summary && (
        <div style={{
          background: 'var(--bg-card)', border: '1px solid var(--border-1)',
          borderRadius: 12, padding: '14px 20px', marginBottom: 20,
          display: 'flex', alignItems: 'center', gap: 16,
        }}>
          <div>
            <div style={{ fontSize: 11, color: 'var(--text-3)', fontWeight: 600, marginBottom: 2 }}>
              Total API calls in last {days} days
            </div>
            <div style={{ fontSize: 28, fontWeight: 800, color: 'var(--text-1)' }}>
              {loading ? '…' : totalCalls.toLocaleString()}
            </div>
          </div>
          <div style={{ flex: 1 }} />
          {apis.length > 0 && (
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {apis.map(api => {
                const meta = getMeta(api)
                const pct = Math.round((totals[api] / totalCalls) * 100)
                return (
                  <div key={api} style={{
                    display: 'flex', alignItems: 'center', gap: 6,
                    background: meta.bg, border: `1px solid ${meta.color}30`,
                    borderRadius: 20, padding: '4px 10px',
                  }}>
                    <div style={{ width: 6, height: 6, borderRadius: '50%', background: meta.color }} />
                    <span style={{ fontSize: 11, color: meta.color, fontWeight: 600 }}>{meta.label}</span>
                    <span style={{ fontSize: 11, color: 'var(--text-3)' }}>{pct}%</span>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      )}

      {/* Per-API cards */}
      {loading && !summary ? (
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, color: 'var(--text-3)', fontSize: 13, padding: '40px 0' }}>
          <Spinner /> Loading API usage…
        </div>
      ) : (
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(240px, 1fr))',
          gap: 14, marginBottom: 28,
        }}>
          {apis.map(api => (
            <TotalCard key={api} api={api} total={totals[api] || 0} byType={byType[api] || {}} />
          ))}
        </div>
      )}

      {/* Daily activity bar chart */}
      {days_list.length > 0 && (
        <div style={{
          background: 'var(--bg-card)', border: '1px solid var(--border-1)',
          borderRadius: 12, padding: '18px 20px', marginBottom: 24,
        }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-2)', marginBottom: 16 }}>
            Daily Activity
          </div>
          <div style={{ display: 'flex', alignItems: 'flex-end', gap: 6, height: 80, overflowX: 'auto' }}>
            {days_list.slice(0, 30).reverse().map(day => {
              const count = byDay[day]
              const h = Math.max(4, Math.round((count / maxDay) * 72))
              return (
                <div key={day} title={`${day}: ${count} calls`}
                  style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4, flex: '0 0 auto', minWidth: 24 }}>
                  <div style={{ fontSize: 9, color: 'var(--text-3)' }}>{count}</div>
                  <div style={{
                    width: 20, height: h, borderRadius: '4px 4px 2px 2px',
                    background: 'linear-gradient(to top, #6366f1, #8b5cf6)',
                    cursor: 'default', transition: 'opacity 0.2s',
                  }} />
                  <div style={{ fontSize: 8, color: 'var(--text-3)', whiteSpace: 'nowrap' }}>
                    {day.slice(5)}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Detail table */}
      {detail.length > 0 && (
        <div style={{
          background: 'var(--bg-card)', border: '1px solid var(--border-1)',
          borderRadius: 12, overflow: 'hidden',
        }}>
          <div style={{
            padding: '14px 20px', borderBottom: '1px solid var(--border-1)',
            display: 'flex', alignItems: 'center', gap: 10,
          }}>
            <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-2)' }}>
              Daily Breakdown
            </span>
            <span style={{
              fontSize: 10, color: 'var(--text-3)', background: 'var(--bg-base)',
              border: '1px solid var(--border-1)', borderRadius: 20, padding: '1px 8px',
            }}>{detail.length} rows</span>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ background: 'var(--bg-base)' }}>
                  {['Date', 'API', 'Call Type', 'Count'].map(h => (
                    <th key={h} style={{
                      padding: '9px 16px', textAlign: 'left', fontSize: 11,
                      fontWeight: 700, color: 'var(--text-3)', borderBottom: '1px solid var(--border-1)',
                      whiteSpace: 'nowrap',
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[...detail].sort((a, b) => b.day.localeCompare(a.day) || a.api.localeCompare(b.api)).map((row, i) => {
                  const meta = getMeta(row.api)
                  return (
                    <tr key={i} style={{
                      borderBottom: '1px solid var(--border-1)',
                      background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)',
                    }}>
                      <td style={{ padding: '9px 16px', color: 'var(--text-3)', fontFamily: 'monospace', fontSize: 11 }}>
                        {row.day}
                      </td>
                      <td style={{ padding: '9px 16px' }}>
                        <span style={{
                          fontSize: 11, fontWeight: 600, padding: '2px 9px', borderRadius: 20,
                          background: meta.bg, color: meta.color, border: `1px solid ${meta.color}30`,
                        }}>{meta.label}</span>
                      </td>
                      <td style={{ padding: '9px 16px', color: 'var(--text-2)', fontFamily: 'monospace', fontSize: 11 }}>
                        {row.call_type}
                      </td>
                      <td style={{ padding: '9px 16px', fontWeight: 700, color: 'var(--text-1)' }}>
                        {row.count.toLocaleString()}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
