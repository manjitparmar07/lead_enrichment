import { useState } from 'react'
import {
  UserSearch, Mail, Phone, ChevronDown, ChevronUp,
  Download, RefreshCw, X, Code2, Trash2,
} from 'lucide-react'
import { toast } from 'react-hot-toast'
import { BACKEND, jsonHdr } from '../../../utils/api'
import { TIER } from '../../../utils/leadConfig.jsx'
import { fmtCity, fmtCountry, parseTags } from '../../../utils/leadFormatters'
import { GhostBtn, Avatar, TierBadge, TagBadge, CompanyLogo, ScoreCircle, selStyle } from '../../ui'
import JsonModal from '../JsonModal'
import LeadEnrichView from '../views/LeadEnrichView'

export default function ResultsTab({ leads, total, loading, filters, onFiltersChange, searchInput, onSearchInput, onRefresh, onLeadDeleted, jobs, page, pageSize, onPageChange, onPageSizeChange }) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize))
  const [expanded, setExpanded] = useState(null)
  const [jsonModal, setJsonModal] = useState(null)
  const [jsonLoading, setJsonLoading] = useState(null)

  const openJson = async (lead) => {
    setJsonLoading(lead.id)
    try {
      const r = await fetch(`${BACKEND}/leads/${lead.id}`, { headers: jsonHdr() })
      const data = await r.json()
      setJsonModal(data)
    } catch {
      toast.error('Failed to load full JSON')
    } finally {
      setJsonLoading(null)
    }
  }

  const handleDelete = async (id) => {
    if (!confirm('Delete this lead?')) return
    try {
      await fetch(`${BACKEND}/leads/${id}`, { method: 'DELETE', headers: jsonHdr() })
      toast.success('Lead deleted'); onLeadDeleted()
    } catch { toast.error('Delete failed') }
  }

  const handleExport = () => {
    const p = new URLSearchParams()
    if (filters.tier) p.set('tier', filters.tier)
    if (filters.min_score) p.set('min_score', filters.min_score)
    window.open(`${BACKEND}/leads/export/csv?${p}`, '_blank')
  }

  return (
    <div>
      {/* Search */}
      <div style={{ marginBottom: 10 }}>
        <input
          type="text"
          value={searchInput ?? ''}
          onChange={e => onSearchInput(e.target.value)}
          placeholder="Search by name or company…"
          style={{
            width: '100%', boxSizing: 'border-box',
            background: 'var(--bg-base)', border: '1px solid var(--border-1)',
            color: 'var(--text-1)', borderRadius: 8, padding: '7px 12px',
            fontSize: 12, outline: 'none',
          }}
        />
      </div>

      {/* Filters */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 14, flexWrap: 'wrap', alignItems: 'center' }}>
        <select value={filters.tier} onChange={e => onFiltersChange(f => ({...f, tier: e.target.value}))} style={selStyle}>
          <option value="">All tiers</option>
          <option value="hot">🔥 Hot (80+)</option>
          <option value="warm">⭐ Warm (50–79)</option>
          <option value="cool">👍 Cool (25–49)</option>
          <option value="cold">❄️ Cold (&lt;25)</option>
        </select>
        <input type="number" min="0" max="100" value={filters.min_score}
          onChange={e => onFiltersChange(f => ({...f, min_score: e.target.value}))}
          placeholder="Min score" style={{ ...selStyle, width: 100 }} />
        <select value={filters.job_id} onChange={e => onFiltersChange(f => ({...f, job_id: e.target.value}))} style={selStyle}>
          <option value="">All jobs</option>
          {jobs.map(j => <option key={j.id} value={j.id}>{j.id.slice(0,8)}… ({j.total_urls})</option>)}
        </select>
        <select value={filters.sort_by || 'score'} onChange={e => onFiltersChange(f => ({...f, sort_by: e.target.value}))} style={selStyle}>
          <option value="score">Sort: Score</option>
          <option value="name">Sort: Name</option>
          <option value="company">Sort: Company</option>
          <option value="enriched_at">Sort: Enriched At</option>
          <option value="created_at">Sort: Created At</option>
        </select>
        <button onClick={() => onFiltersChange(f => ({...f, sort_dir: f.sort_dir === 'ASC' ? 'DESC' : 'ASC'}))}
          style={{ ...selStyle, cursor: 'pointer', minWidth: 36, textAlign: 'center' }}
          title="Toggle sort direction">
          {filters.sort_dir === 'ASC' ? '↑' : '↓'}
        </button>
        {(filters.tier || filters.min_score || filters.job_id) && (
          <GhostBtn onClick={() => onFiltersChange(f => ({ ...f, tier:'', min_score:'', job_id:'', q:'' }))}>
            <X size={11} /> Clear
          </GhostBtn>
        )}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <GhostBtn onClick={onRefresh}><RefreshCw size={11} /> Refresh</GhostBtn>
          <GhostBtn onClick={handleExport} style={{ color: '#10b981', borderColor: 'rgba(16,185,129,0.3)' }}>
            <Download size={11} /> Export CSV
          </GhostBtn>
        </div>
      </div>

      {/* Tier stats */}
      {total > 0 && (
        <div style={{ display: 'flex', gap: 8, marginBottom: 14, flexWrap: 'wrap', alignItems: 'center' }}>
          {['hot','warm','cool','cold'].map(tier => {
            const count = leads.filter(l => l.score_tier === tier).length
            const cfg = TIER[tier]
            return (
              <button key={tier} onClick={() => onFiltersChange(f => ({...f, tier: f.tier===tier ? '' : tier}))} style={{
                padding: '6px 12px', borderRadius: 8, cursor: 'pointer', border: `1px solid ${filters.tier===tier ? cfg.border:'var(--border-1)'}`,
                background: filters.tier===tier ? cfg.bg:'var(--bg-elevated)',
                color: cfg.color, fontSize: 11, fontWeight: 600, display: 'flex', gap: 5, alignItems: 'center',
              }}>
                {cfg.icon} {cfg.label} <span style={{ opacity: 0.7 }}>{count}</span>
              </button>
            )
          })}
          <div style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-3)' }}>
            {total} total · Page {page} of {totalPages}
          </div>
        </div>
      )}

      {loading && <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-3)', fontSize: 13 }}>Loading leads…</div>}
      {!loading && leads.length === 0 && (
        <div style={{ textAlign: 'center', padding: '60px 20px', color: 'var(--text-3)' }}>
          <UserSearch size={36} style={{ opacity: 0.25, marginBottom: 10 }} />
          <p style={{ fontSize: 14 }}>No leads yet · use Single or Bulk Enrich to start</p>
        </div>
      )}

      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {leads.map(lead => (
          <div key={lead.id} style={{
            borderRadius: 10, border: `1px solid ${expanded===lead.id ? 'rgba(99,102,241,0.3)':'var(--border-1)'}`,
            background: 'var(--bg-card)', overflow: 'hidden', transition: 'border-color 0.15s',
          }}>
            {/* Row header */}
            <div onClick={() => setExpanded(id => id===lead.id ? null : lead.id)}
              style={{ padding: '11px 14px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 10 }}>
              <Avatar name={lead.name} tier={lead.score_tier} src={lead.avatar_url} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 2 }}>
                  <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>{lead.name || 'Unknown'}</span>
                  <TierBadge tier={lead.score_tier} />
                  {parseTags(lead.tags).slice(0,2).map(t => <TagBadge key={t} text={t} />)}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  {lead.company_logo && <CompanyLogo src={lead.company_logo} name={lead.company} size={16} />}
                  <div style={{ fontSize: 11, color: 'var(--text-3)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {[lead.title, lead.company, fmtCity(lead.city) || fmtCountry(lead.country)].filter(Boolean).join(' · ')}
                  </div>
                </div>
                {lead.enriched_at && (
                  <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2 }}>
                    Scraped {new Date(lead.enriched_at).toLocaleString()}
                  </div>
                )}
              </div>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexShrink: 0 }}>
                {lead.work_email && <span style={{ fontSize: 10, color: '#10b981', display: 'flex', gap: 3, alignItems: 'center' }}><Mail size={10} /> Email</span>}
                {lead.direct_phone && <span style={{ fontSize: 10, color: '#3b82f6', display: 'flex', gap: 3, alignItems: 'center' }}><Phone size={10} /> Phone</span>}
                <ScoreCircle score={lead.total_score} tier={lead.score_tier} />
                {lead.data_completeness > 0 && (
                  <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{lead.data_completeness}%</span>
                )}
                <GhostBtn onClick={e => { e.stopPropagation(); openJson(lead) }} style={{ padding: '3px 8px', fontSize: 10, opacity: jsonLoading === lead.id ? 0.6 : 1 }}>
                  <Code2 size={10} /> {jsonLoading === lead.id ? '…' : 'JSON'}
                </GhostBtn>
                <button onClick={e => { e.stopPropagation(); handleDelete(lead.id) }}
                  style={{ background:'none', border:'none', cursor:'pointer', color:'var(--text-3)', padding: 3 }}>
                  <Trash2 size={12} />
                </button>
                {expanded===lead.id ? <ChevronUp size={13} style={{ color:'var(--text-3)' }} /> : <ChevronDown size={13} style={{ color:'var(--text-3)' }} />}
              </div>
            </div>
            {expanded === lead.id && (
              <div style={{ borderTop: '1px solid var(--border-1)' }}>
                <LeadEnrichView lead={lead} />
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Pagination */}
      {total > 0 && (
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          gap: 10, marginTop: 18, paddingTop: 14,
          borderTop: '1px solid var(--border-1)',
        }}>
          <select
            value={pageSize}
            onChange={e => onPageSizeChange(Number(e.target.value))}
            style={{ ...selStyle, width: 110 }}
          >
            <option value={10}>10 / page</option>
            <option value={20}>20 / page</option>
            <option value={50}>50 / page</option>
            <option value={100}>100 / page</option>
          </select>
          <button
            disabled={page <= 1}
            onClick={() => onPageChange(page - 1)}
            style={{
              padding: '5px 14px', borderRadius: 7, fontSize: 12, fontWeight: 600,
              border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
              color: page <= 1 ? 'var(--text-3)' : 'var(--text-1)',
              cursor: page <= 1 ? 'not-allowed' : 'pointer', opacity: page <= 1 ? 0.5 : 1,
            }}
          >← Prev</button>
          <div style={{ display: 'flex', gap: 4 }}>
            {Array.from({ length: totalPages }, (_, i) => i + 1)
              .filter(p => p === 1 || p === totalPages || Math.abs(p - page) <= 2)
              .reduce((acc, p, idx, arr) => {
                if (idx > 0 && p - arr[idx - 1] > 1) acc.push('…')
                acc.push(p)
                return acc
              }, [])
              .map((p, idx) => p === '…'
                ? <span key={`ellipsis-${idx}`} style={{ padding: '5px 4px', fontSize: 12, color: 'var(--text-3)' }}>…</span>
                : (
                  <button key={p} onClick={() => onPageChange(p)} style={{
                    width: 30, height: 28, borderRadius: 6, fontSize: 12, fontWeight: p === page ? 700 : 500,
                    border: `1px solid ${p === page ? 'rgba(99,102,241,0.5)' : 'var(--border-1)'}`,
                    background: p === page ? 'rgba(99,102,241,0.12)' : 'var(--bg-elevated)',
                    color: p === page ? '#6366f1' : 'var(--text-2)',
                    cursor: 'pointer',
                  }}>{p}</button>
                )
              )
            }
          </div>
          <button
            disabled={page >= totalPages}
            onClick={() => onPageChange(page + 1)}
            style={{
              padding: '5px 14px', borderRadius: 7, fontSize: 12, fontWeight: 600,
              border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
              color: page >= totalPages ? 'var(--text-3)' : 'var(--text-1)',
              cursor: page >= totalPages ? 'not-allowed' : 'pointer', opacity: page >= totalPages ? 0.5 : 1,
            }}
          >Next →</button>
        </div>
      )}

      {jsonModal && <JsonModal data={jsonModal} onClose={() => setJsonModal(null)} />}
    </div>
  )
}
