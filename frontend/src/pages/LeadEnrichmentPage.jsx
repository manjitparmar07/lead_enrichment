// LeadEnrichmentPage.jsx — Worksbuddy Lead Enrichment (Bright Data powered)
// 7-category deep enrichment: Identity · Professional · Company · Intent · Scoring · Outreach · CRM

import { useState, useEffect, useCallback, useRef } from 'react'
import { Upload, List, Briefcase, Zap, RefreshCw } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { BACKEND, jsonHdr } from '../utils/api'
import BulkTab from '../components/lead-enrichment/tabs/BulkTab'
import ResultsTab from '../components/lead-enrichment/tabs/ResultsTab'
import JobsTab from '../components/lead-enrichment/tabs/JobsTab'

// ─────────────────────────────────────────────────────────────────────────────
export default function LeadEnrichmentPage() {
  const [activeTab, setActiveTab] = useState('bulk')
  const [leads, setLeads] = useState([])
  const [leadsTotal, setLeadsTotal] = useState(0)
  const [jobs, setJobs] = useState([])
  const [leadsLoading, setLeadsLoading] = useState(false)
  const [filters, setFilters] = useState({ tier: '', min_score: '', job_id: '', sort_by: 'score', sort_dir: 'DESC', q: '' })
  const [searchInput, setSearchInput] = useState('')  // immediate input display
  const searchTimer = useRef(null)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)

  const handleFiltersChange = useCallback((updater) => {
    setFilters(prev => {
      const next = typeof updater === 'function' ? updater(prev) : updater
      return next
    })
    setPage(1)
  }, [])

  const handleSearchInput = useCallback((value) => {
    setSearchInput(value)
    clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => {
      setFilters(f => ({ ...f, q: value }))
      setPage(1)
    }, 400)
  }, [])

  const fetchLeads = useCallback(async () => {
    setLeadsLoading(true)
    try {
      const p = new URLSearchParams({ limit: pageSize, offset: (page - 1) * pageSize })
      if (filters.tier) p.set('tier', filters.tier)
      if (filters.min_score) p.set('min_score', filters.min_score)
      if (filters.job_id) p.set('job_id', filters.job_id)
      if (filters.sort_by) p.set('sort_by', filters.sort_by)
      if (filters.sort_dir) p.set('sort_dir', filters.sort_dir)
      if (filters.q) p.set('q', filters.q)
      const r = await fetch(`${BACKEND}/leads?${p}`, { headers: jsonHdr() })
      const d = await r.json()
      setLeads(d.leads || [])
      setLeadsTotal(d.total || 0)
    } catch { toast.error('Failed to load leads') }
    finally { setLeadsLoading(false) }
  }, [filters, page, pageSize])

  const fetchJobs = useCallback(async () => {
    try {
      const r = await fetch(`${BACKEND}/leads/jobs`, { headers: jsonHdr() })
      const d = await r.json()
      setJobs(d.jobs || [])
    } catch {}
  }, [])

  useEffect(() => {
    if (activeTab === 'results') { fetchLeads(); fetchJobs() }
    if (activeTab === 'jobs') fetchJobs()
  }, [activeTab, fetchLeads, fetchJobs])

  // Real-time job progress via Ably (with 5s polling fallback)
  useEffect(() => {
    if (activeTab !== 'jobs') return
    const running = jobs.some(j => ['running', 'pending'].includes(j.status))
    if (!running) return

    let ablyChannels = []
    let pollTimer = null

    const setupAbly = async () => {
      try {
        const token = localStorage.getItem('wb_ai_token')
        if (!token) return false

        // Decode org_id from JWT payload
        const parts = token.split('.')
        if (parts.length < 2) return false
        const padded = parts[1] + '=='.slice(0, (4 - parts[1].length % 4) % 4)
        const payload = JSON.parse(atob(padded))
        const orgId = payload.organization_id || 'default'

        const { Realtime } = await import('ably')
        const ably = new Realtime({
          authUrl:    `${BACKEND}/ably/token`,
          authParams: { org_id: orgId, user_id: payload.user_id || orgId },
          authMethod: 'GET',
        })

        // Subscribe to each running job's channel
        const runningJobs = jobs.filter(j => ['running', 'pending'].includes(j.status))
        for (const job of runningJobs) {
          const ch = ably.channels.get(`tenant:${orgId}:job:${job.id}`)
          ch.subscribe('lead:done', () => {
            // Increment local processed count immediately (optimistic)
            setJobs(prev => prev.map(j =>
              j.id === job.id ? { ...j, processed: (j.processed || 0) + 1 } : j
            ))
          })
          ch.subscribe('job:done', (msg) => {
            const { processed, failed } = msg.data || {}
            const status = failed === 0 ? 'completed' : processed > 0 ? 'completed_with_errors' : 'failed'
            setJobs(prev => prev.map(j =>
              j.id === job.id ? { ...j, status, processed: processed ?? j.processed, failed: failed ?? j.failed } : j
            ))
            // Also refresh leads list to show new results
            setTimeout(fetchLeads, 500)
          })
          ablyChannels.push({ ch, ably })
        }
        return true
      } catch {
        return false
      }
    }

    setupAbly().then(ok => {
      // Fallback polling — 5s if Ably OK, 3s if not
      pollTimer = setInterval(fetchJobs, ok ? 5000 : 3000)
    })

    return () => {
      clearInterval(pollTimer)
      ablyChannels.forEach(({ ch, ably }) => {
        try { ch.unsubscribe() } catch (_) {}
        try { ably.close() } catch (_) {}
      })
    }
  }, [activeTab, jobs, fetchJobs, fetchLeads])

  const TABS = [
    { id: 'bulk',    label: 'Bulk Enrich',   icon: <Upload     size={14} /> },
    { id: 'results', label: 'Results',        icon: <List       size={14} />, badge: leadsTotal || null },
    { id: 'jobs',    label: 'Jobs',           icon: <Briefcase  size={14} />, badge: jobs.filter(j => j.status === 'running').length || null },
  ]

  return (
    <div style={{ height: '100%', overflow: 'auto', background: 'var(--bg-base)', padding: '24px 28px' }}>
      {/* Header */}
      <div style={{ marginBottom: 22, display: 'flex', alignItems: 'center', gap: 12 }}>
        <div style={{
          width: 36, height: 36, borderRadius: 10,
          background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
        }}>
          <Zap size={18} color="#fff" />
        </div>
        <div>
          <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: 'var(--text-1)' }}>Lead Enrichment</h1>
          <p style={{ margin: 0, fontSize: 11, color: 'var(--text-3)' }}>
            Bright Data · Email Waterfall · 7-Category Deep Intelligence · AI Outreach
          </p>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 2, marginBottom: 20, borderBottom: '1px solid var(--border-1)' }}>
        {TABS.map(t => (
          <button key={t.id} onClick={() => setActiveTab(t.id)} style={{
            display: 'flex', alignItems: 'center', gap: 6,
            padding: '8px 14px', border: 'none',
            borderBottom: activeTab === t.id ? '2px solid #6366f1' : '2px solid transparent',
            background: activeTab === t.id ? 'rgba(99,102,241,0.07)' : 'transparent',
            color: activeTab === t.id ? '#a5b4fc' : 'var(--text-3)',
            cursor: 'pointer', fontSize: 13, fontWeight: activeTab === t.id ? 600 : 400,
            borderRadius: '8px 8px 0 0', transition: 'all 0.15s',
          }}>
            {t.icon} {t.label}
            {t.badge ? (
              <span style={{
                fontSize: 10, fontWeight: 700, minWidth: 18, textAlign: 'center',
                background: activeTab === t.id ? '#6366f1' : 'var(--bg-elevated)',
                color: activeTab === t.id ? '#fff' : 'var(--text-2)',
                borderRadius: 10, padding: '1px 6px',
              }}>{t.badge}</span>
            ) : null}
          </button>
        ))}
      </div>

      {activeTab === 'bulk'    && <BulkTab   onDone={() => { setActiveTab('jobs');    fetchJobs() }} />}
      {activeTab === 'results' && (
        <ResultsTab
          leads={leads} total={leadsTotal} loading={leadsLoading}
          filters={filters} onFiltersChange={handleFiltersChange}
          searchInput={searchInput} onSearchInput={handleSearchInput}
          onRefresh={fetchLeads} onLeadDeleted={fetchLeads} jobs={jobs}
          page={page} pageSize={pageSize}
          onPageChange={setPage} onPageSizeChange={(s) => { setPageSize(s); setPage(1) }}
        />
      )}
      {activeTab === 'jobs' && <JobsTab jobs={jobs} onRefresh={fetchJobs}
        onSelectJob={jid => { setFilters(f => ({...f, job_id: jid})); setPage(1); setActiveTab('results') }} />}
    </div>
  )
}
