// LeadEnrichmentPage.jsx — Worksbuddy Lead Enrichment (Bright Data powered)
// 7-category deep enrichment: Identity · Professional · Company · Intent · Scoring · Outreach · CRM

import { useState, useEffect, useCallback, useRef } from 'react'
import {
  UserSearch, Upload, List, Briefcase, Zap, RefreshCw, Download, Sparkles, Loader,
  Trash2, Mail, Phone, Linkedin, Twitter, Globe, MapPin, Clock,
  Building2, TrendingUp, Target, Send, Database, ChevronDown,
  ChevronUp, Copy, Check, AlertCircle, CheckCircle, Flame,
  Snowflake, Star, ThumbsUp, ArrowRight, X, Code2, Users,
  GraduationCap, Award, Languages, Layers, BarChart2, Megaphone,
  Newspaper, ShoppingCart, MessageSquare, CalendarDays, User,
} from 'lucide-react'
import { toast } from 'react-hot-toast'

const BACKEND = `${import.meta.env.VITE_BACKEND_URL || 'https://api-lead-enrichment-worksbuddy.lbmdemo.com'}/api`

const getToken = () => localStorage.getItem('wb_ai_token') || ''
const jsonHdr  = () => ({ 'Content-Type': 'application/json' })

function getOrgId() {
  try {
    const t = getToken()
    if (!t) return 'default'
    const payload = JSON.parse(atob(t.split('.')[1].replace(/-/g,'+').replace(/_/g,'/')))
    return payload.organization_id || payload.org_id || 'default'
  } catch { return 'default' }
}

const TIER = {
  hot:  { color: '#ef4444', bg: 'rgba(239,68,68,0.10)',  border: 'rgba(239,68,68,0.28)',  icon: <Flame    size={11} />, label: 'Hot'  },
  warm: { color: '#f97316', bg: 'rgba(249,115,22,0.10)', border: 'rgba(249,115,22,0.28)', icon: <Star     size={11} />, label: 'Warm' },
  cool: { color: '#3b82f6', bg: 'rgba(59,130,246,0.10)', border: 'rgba(59,130,246,0.28)', icon: <ThumbsUp size={11} />, label: 'Cool' },
  cold: { color: '#6b7280', bg: 'rgba(107,114,128,0.08)',border: 'rgba(107,114,128,0.22)',icon: <Snowflake size={11} />, label: 'Cold' },
}

// ISO 3166-1 alpha-2 → full country name (frontend fallback for existing DB records)
const COUNTRY_CODES = {
  AF:'Afghanistan',AL:'Albania',DZ:'Algeria',AR:'Argentina',AU:'Australia',
  AT:'Austria',BE:'Belgium',BR:'Brazil',CA:'Canada',CL:'Chile',CN:'China',
  CO:'Colombia',CZ:'Czech Republic',DK:'Denmark',EG:'Egypt',FI:'Finland',
  FR:'France',DE:'Germany',GH:'Ghana',GR:'Greece',HK:'Hong Kong',HU:'Hungary',
  IN:'India',ID:'Indonesia',IE:'Ireland',IL:'Israel',IT:'Italy',JP:'Japan',
  KE:'Kenya',KR:'South Korea',MY:'Malaysia',MX:'Mexico',MA:'Morocco',
  NL:'Netherlands',NZ:'New Zealand',NG:'Nigeria',NO:'Norway',PK:'Pakistan',
  PE:'Peru',PH:'Philippines',PL:'Poland',PT:'Portugal',RO:'Romania',RU:'Russia',
  SA:'Saudi Arabia',ZA:'South Africa',ES:'Spain',SE:'Sweden',CH:'Switzerland',
  TW:'Taiwan',TH:'Thailand',TR:'Turkey',UA:'Ukraine',AE:'United Arab Emirates',
  GB:'United Kingdom',US:'United States',VN:'Vietnam',
}

/** Map ISO country code to full name; pass-through if already a name */
const fmtCountry = (v) => {
  if (!v) return ''
  const upper = v.trim().toUpperCase()
  if (upper.length <= 3 && COUNTRY_CODES[upper]) return COUNTRY_CODES[upper]
  return v.trim()
}

/** Return only the city part from "City, State, Country" strings */
const fmtCity = (v) => (v || '').split(',')[0].trim()

// ─────────────────────────────────────────────────────────────────────────────
// JSON Modal
// ─────────────────────────────────────────────────────────────────────────────
function JsonModal({ data, onClose }) {
  const formatted = JSON.stringify(data, null, 2)
  const [copied, setCopied] = useState(false)
  const copy = () => {
    navigator.clipboard.writeText(formatted)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
    toast.success('JSON copied to clipboard')
  }
  useEffect(() => {
    const esc = (e) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', esc)
    return () => window.removeEventListener('keydown', esc)
  }, [onClose])

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 1000,
      background: 'rgba(0,0,0,0.65)', backdropFilter: 'blur(4px)',
      display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 20,
    }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{
        width: '100%', maxWidth: 900, maxHeight: '88vh',
        borderRadius: 14, border: '1px solid var(--border-1)',
        background: 'var(--bg-base)', display: 'flex', flexDirection: 'column',
        overflow: 'hidden', boxShadow: '0 25px 60px rgba(0,0,0,0.4)',
      }}>
        {/* Header */}
        <div style={{
          padding: '14px 18px', borderBottom: '1px solid var(--border-1)',
          display: 'flex', alignItems: 'center', gap: 10, flexShrink: 0,
        }}>
          <Code2 size={16} color="#6366f1" />
          <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-1)', flex: 1 }}>
            Full JSON — {data?.name || 'Lead'}
            <span style={{ fontSize: 10, fontWeight: 400, color: 'var(--text-3)', marginLeft: 8 }}>
              raw_profile + full_data + all fields
            </span>
          </span>
          <button onClick={copy} style={{
            display: 'flex', alignItems: 'center', gap: 5, padding: '6px 12px',
            borderRadius: 7, border: '1px solid var(--border-1)', background: 'transparent',
            color: copied ? '#10b981' : 'var(--text-2)', cursor: 'pointer', fontSize: 12, fontWeight: 500,
          }}>
            {copied ? <Check size={12} /> : <Copy size={12} />}
            {copied ? 'Copied!' : 'Copy JSON'}
          </button>
          <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-3)', padding: 4 }}>
            <X size={16} />
          </button>
        </div>
        {/* Body */}
        <div style={{ flex: 1, overflow: 'auto', padding: '14px 18px', background: '#ffffff' }}>
          <pre style={{
            margin: 0, fontSize: 12, lineHeight: 1.6,
            fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
            color: '#111827', whiteSpace: 'pre-wrap', wordBreak: 'break-word',
          }}>
            <JsonHighlight json={formatted} />
          </pre>
        </div>
      </div>
    </div>
  )
}

function JsonHighlight({ json }) {
  const html = json
    .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g, (match) => {
      let cls = '#0369a1' // number — blue
      if (/^"/.test(match)) {
        if (/:$/.test(match)) cls = '#111827' // key — near black
        else cls = '#15803d' // string value — dark green
      } else if (/true|false/.test(match)) cls = '#b45309' // amber
      else if (/null/.test(match)) cls = '#9ca3af' // gray
      return `<span style="color:${cls}">${match}</span>`
    })
  return <span dangerouslySetInnerHTML={{ __html: html }} />
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Page
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

// ─────────────────────────────────────────────────────────────────────────────
// SSE Stage definitions
// ─────────────────────────────────────────────────────────────────────────────
const STREAM_STAGES = [
  { id: 'profile',  label: 'BD Profile Fetch',       icon: <User      size={13} />, desc: 'Name · Title · Company · Avatar' },
  { id: 'company',  label: 'Company Waterfall',       icon: <Building2 size={13} />, desc: 'Logo · Website · Industry · Size' },
  { id: 'contact',  label: 'Contact Waterfall',       icon: <Mail      size={13} />, desc: 'Work Email · Phone (Hunter/Apollo)' },
  { id: 'website',  label: 'Website Intelligence',    icon: <Globe     size={13} />, desc: 'Value Prop · Products · Tech Stack' },
  { id: 'scoring',  label: 'LLM Scoring + Outreach',  icon: <Target    size={13} />, desc: 'ICP Score · Intent · Cold Email' },
  { id: 'complete', label: 'Save & Complete',         icon: <Database  size={13} />, desc: 'Lead persisted to DB' },
]

const STAGE_COLORS = { waiting: '#6b7280', loading: '#6366f1', done: '#10b981', error: '#ef4444' }

function StageDot({ status }) {
  if (status === 'loading') return (
    <div style={{ width: 16, height: 16, borderRadius: '50%', border: '2px solid #6366f1', borderTopColor: 'transparent', animation: 'spin 0.7s linear infinite', flexShrink: 0 }} />
  )
  if (status === 'done')    return <CheckCircle size={16} color="#10b981" style={{ flexShrink: 0 }} />
  if (status === 'error')   return <AlertCircle  size={16} color="#ef4444" style={{ flexShrink: 0 }} />
  return <div style={{ width: 16, height: 16, borderRadius: '50%', border: '2px solid #374151', flexShrink: 0 }} />
}

// ─────────────────────────────────────────────────────────────────────────────
// Single Enrich Tab — SSE streaming version
// ─────────────────────────────────────────────────────────────────────────────
function SingleTab({ onDone }) {
  const [url, setUrl] = useState('')
  const [streaming, setStreaming] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [jsonModal, setJsonModal] = useState(null)
  const [jsonLoading, setJsonLoading] = useState(false)
  const readerRef = useRef(null)

  const isCompanyUrl = url.toLowerCase().includes('linkedin.com/company/')
  const isPersonUrl  = url.toLowerCase().includes('linkedin.com/in/')

  const initStages = () => Object.fromEntries(
    STREAM_STAGES.map(s => [s.id, { status: 'waiting', data: null }])
  )
  const [stages, setStages] = useState(initStages)

  const dispatch = (event) => {
    setStages(prev => ({
      ...prev,
      [event.stage]: { status: event.status, data: event.data || event.lead || null },
    }))
    if (event.stage === 'complete' && event.status === 'done') {
      setResult(event.lead)
      toast.success('Lead enriched — full profile ready')
      onDone()
    }
    if (event.status === 'error') {
      toast.error(`Stage ${event.stage} failed: ${event.error || 'unknown error'}`)
    }
  }

  const handleEnrich = async () => {
    if (!url.trim()) return toast.error('Enter a LinkedIn URL')
    const u = url.toLowerCase()
    if (!u.includes('linkedin.com')) return toast.error('Must be a LinkedIn URL')
    if (!u.includes('/in/') && !u.includes('/company/')) return toast.error('Must be a person (/in/) or company (/company/) URL')

    setStreaming(true); setError(null); setResult(null)
    setStages(initStages())

    try {
      const response = await fetch(`${BACKEND}/leads/enrich/stream`, {
        method: 'POST', headers: jsonHdr(),
        body: JSON.stringify({ linkedin_url: url.trim(), generate_outreach: true }),
      })
      if (!response.ok) {
        const d = await response.json().catch(() => ({}))
        throw new Error(d.detail || `HTTP ${response.status}`)
      }

      const reader = response.body.getReader()
      readerRef.current = reader
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop()
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          try {
            const event = JSON.parse(line.slice(6))
            dispatch(event)
            if (event.stage === 'complete') { setStreaming(false); return }
          } catch (_) {}
        }
      }
    } catch (e) {
      if (e.name !== 'AbortError') { setError(e.message); toast.error(e.message) }
    } finally {
      setStreaming(false)
    }
  }

  const handleCancel = () => {
    try { readerRef.current?.cancel() } catch (_) {}
    setStreaming(false)
    setStages(initStages())
  }

  const activeStageIdx = STREAM_STAGES.findIndex(s => stages[s.id]?.status === 'loading')
  const doneCount = STREAM_STAGES.filter(s => stages[s.id]?.status === 'done').length
  const hasStarted = STREAM_STAGES.some(s => stages[s.id]?.status !== 'waiting')

  return (
    <div style={{ maxWidth: 1100 }}>
      {/* Input bar */}
      <Card style={{ marginBottom: 18 }}>
        <Lbl>
          LinkedIn URL
          {isCompanyUrl && <span style={{ marginLeft: 8, fontSize: 10, color: '#6366f1', fontWeight: 600, background: 'rgba(99,102,241,0.1)', padding: '2px 7px', borderRadius: 4 }}>Company Page</span>}
          {isPersonUrl  && <span style={{ marginLeft: 8, fontSize: 10, color: '#10b981', fontWeight: 600, background: 'rgba(16,185,129,0.1)', padding: '2px 7px', borderRadius: 4 }}>Person Profile</span>}
        </Lbl>
        <div style={{ display: 'flex', gap: 10, marginTop: 8 }}>
          <input value={url} onChange={e => setUrl(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && !streaming && handleEnrich()}
            placeholder="https://www.linkedin.com/in/username  or  linkedin.com/company/name"
            disabled={streaming}
            style={{ ...inputStyle, opacity: streaming ? 0.6 : 1 }} />
          {streaming
            ? <Btn onClick={handleCancel} style={{ background: 'rgba(239,68,68,0.12)', color: '#ef4444', border: '1px solid rgba(239,68,68,0.3)' }}>
                <X size={14} /> Cancel
              </Btn>
            : <Btn onClick={handleEnrich} disabled={!url.trim()}>
                <Zap size={14} />
                {isCompanyUrl ? 'Enrich Company' : 'Enrich Lead'}
              </Btn>
          }
        </div>
        <p style={{ margin: '7px 0 0', fontSize: 11, color: 'var(--text-3)' }}>
          {isCompanyUrl
            ? 'Bright Data Company → Apollo → Hunter → Website Intel · live streaming ~30–60s'
            : 'Bright Data Person → Company waterfall → Email finder → AI enrichment · live streaming ~30–60s'
          }
        </p>
      </Card>

      {error && <ErrorBox msg={error} />}

      {/* Streaming progress + live sections */}
      {hasStarted && (
        <div style={{ display: 'flex', gap: 18, alignItems: 'flex-start' }}>

          {/* ── Left: Stage sidebar ───────────────────────────────────────── */}
          <div style={{
            width: 220, flexShrink: 0,
            borderRadius: 12, border: '1px solid var(--border-1)',
            background: 'var(--bg-main)', padding: '14px 0', position: 'sticky', top: 16,
          }}>
            <div style={{ padding: '0 14px 10px', fontSize: 11, fontWeight: 600, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Enrichment Stages
            </div>
            {/* Progress bar */}
            <div style={{ margin: '0 14px 12px', height: 3, borderRadius: 2, background: 'var(--bg-elevated)' }}>
              <div style={{
                height: '100%', borderRadius: 2,
                width: `${Math.round((doneCount / STREAM_STAGES.length) * 100)}%`,
                background: 'linear-gradient(90deg,#6366f1,#10b981)',
                transition: 'width 0.4s ease',
              }} />
            </div>
            {STREAM_STAGES.map((s, i) => {
              const st = stages[s.id] || { status: 'waiting' }
              const isActive = st.status === 'loading'
              return (
                <div key={s.id} style={{
                  display: 'flex', alignItems: 'flex-start', gap: 9,
                  padding: '8px 14px',
                  background: isActive ? 'rgba(99,102,241,0.07)' : 'transparent',
                  borderLeft: isActive ? '2px solid #6366f1' : '2px solid transparent',
                  transition: 'all 0.2s',
                }}>
                  <StageDot status={st.status} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 12, fontWeight: isActive ? 600 : 500, color: isActive ? '#a5b4fc' : st.status === 'done' ? 'var(--text-1)' : 'var(--text-3)', lineHeight: 1.3 }}>
                      {s.label}
                    </div>
                    <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2, lineHeight: 1.3 }}>
                      {st.status === 'loading' ? 'Processing…' : st.status === 'done' ? s.desc : st.status === 'error' ? 'Failed' : s.desc}
                    </div>
                  </div>
                </div>
              )
            })}
            {streaming && (
              <div style={{ padding: '10px 14px 0', fontSize: 11, color: '#a5b4fc', display: 'flex', alignItems: 'center', gap: 5 }}>
                <div style={{ width: 6, height: 6, borderRadius: '50%', background: '#6366f1', animation: 'pulse 1.2s ease-in-out infinite' }} />
                Live enriching…
              </div>
            )}
            {!streaming && doneCount === STREAM_STAGES.length && (
              <div style={{ padding: '10px 14px 0', fontSize: 11, color: '#10b981', display: 'flex', alignItems: 'center', gap: 5 }}>
                <CheckCircle size={12} /> Complete
              </div>
            )}
          </div>

          {/* ── Right: Live filling sections ──────────────────────────────── */}
          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 12 }}>

            {/* Stage 1 — Profile card */}
            <StreamSection title="Profile" stageId="profile" stages={stages} icon={<User size={13} />}
              skeleton={<SkeletonProfile />}>
              {(data) => (
                <div style={{ display: 'flex', gap: 14, alignItems: 'flex-start' }}>
                  {/* Avatar — reveal immediately */}
                  <FieldReveal index={0} style={{ flexShrink: 0 }}>
                    <div style={{ width: 54, height: 54, borderRadius: 10, overflow: 'hidden', flexShrink: 0,
                      background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                      {data.avatar_url
                        ? <img src={data.avatar_url} alt="" referrerPolicy="no-referrer"
                            style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
                            onError={e => { e.target.style.display = 'none' }} />
                        : <User size={22} color="#fff" />
                      }
                    </div>
                  </FieldReveal>
                  <div style={{ flex: 1 }}>
                    {/* Name */}
                    <FieldReveal index={1}>
                      <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-1)', marginBottom: 2 }}>{data.name}</div>
                    </FieldReveal>
                    {/* Title */}
                    <FieldReveal index={2}>
                      <div style={{ fontSize: 12, color: 'var(--text-2)', marginBottom: 6 }}>{data.title}</div>
                    </FieldReveal>
                    {/* Badges */}
                    <FieldReveal index={3}>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 6 }}>
                        {data.company  && <span style={tagStyle('#6366f1')}><Building2 size={10} /> {data.company}</span>}
                        {data.location && <span style={tagStyle('#8b5cf6')}><MapPin size={10} /> {data.location}</span>}
                        {data.followers > 0 && <span style={tagStyle('#64748b')}><Users size={10} /> {data.followers.toLocaleString()} followers</span>}
                        {data.connections > 0 && <span style={tagStyle('#64748b')}>{data.connections}+ connections</span>}
                      </div>
                    </FieldReveal>
                    {/* About */}
                    {data.about && (
                      <FieldReveal index={4}>
                        <div style={{ fontSize: 11, color: 'var(--text-3)', lineHeight: 1.6, borderLeft: '2px solid var(--border-1)', paddingLeft: 8 }}>{data.about}</div>
                      </FieldReveal>
                    )}
                  </div>
                </div>
              )}
            </StreamSection>

            {/* Stage 2 — Company */}
            <StreamSection title="Company" stageId="company" stages={stages} icon={<Building2 size={13} />}
              skeleton={<SkeletonCompany />}>
              {(data) => (
                <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'flex-start' }}>
                  {/* Logo */}
                  {data.company_logo && (
                    <FieldReveal index={0} style={{ flexShrink: 0 }}>
                      <img src={data.company_logo} alt="" style={{ width: 44, height: 44, borderRadius: 8, objectFit: 'contain', border: '1px solid var(--border-1)', background: '#fff', padding: 4 }} onError={e => { e.target.style.display = 'none' }} />
                    </FieldReveal>
                  )}
                  <div style={{ flex: 1 }}>
                    {/* Website */}
                    {data.company_website && (
                      <FieldReveal index={1}>
                        <a href={data.company_website} target="_blank" rel="noopener noreferrer" style={{ fontSize: 13, color: '#a5b4fc', textDecoration: 'none', display: 'inline-flex', alignItems: 'center', gap: 4, marginBottom: 8 }}>
                          <Globe size={12} /> {data.company_website.replace(/^https?:\/\/(www\.)?/, '')}
                        </a>
                      </FieldReveal>
                    )}
                    {/* Core badges: industry, size, location */}
                    <FieldReveal index={2}>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 7, marginBottom: 6 }}>
                        {data.industry       && <span style={tagStyle('#6366f1')}>{data.industry}</span>}
                        {data.employee_count > 0 && <span style={tagStyle('#8b5cf6')}><Users size={10} /> {data.employee_count.toLocaleString()} employees</span>}
                        {data.hq_location    && <span style={tagStyle('#64748b')}><MapPin size={10} /> {data.hq_location}</span>}
                      </div>
                    </FieldReveal>
                    {/* Secondary badges: founded, funding */}
                    {(data.founded_year || data.funding_stage) && (
                      <FieldReveal index={3}>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 7, marginBottom: 6 }}>
                          {data.founded_year  && <span style={tagStyle('#64748b')}>Est. {data.founded_year}</span>}
                          {data.funding_stage && <span style={tagStyle('#f59e0b')}>{data.funding_stage}</span>}
                        </div>
                      </FieldReveal>
                    )}
                    {/* Tech stack */}
                    {data.tech_stack?.length > 0 && (
                      <FieldReveal index={4}>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                          {data.tech_stack.slice(0,8).map((t, i) => <span key={i} style={tagStyle('#0891b2')}><Code2 size={9} /> {t}</span>)}
                        </div>
                      </FieldReveal>
                    )}
                  </div>
                </div>
              )}
            </StreamSection>

            {/* Stage 3 — Contact */}
            <StreamSection title="Contact" stageId="contact" stages={stages} icon={<Mail size={13} />}
              skeleton={<SkeletonContact />}>
              {(data) => (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {/* Email */}
                  <FieldReveal index={0}>
                    {data.email
                      ? <div style={contactRow}>
                          <Mail size={13} color="#10b981" />
                          <span style={{ color: 'var(--text-1)', fontSize: 13, fontWeight: 500 }}>{data.email}</span>
                          <span style={{ fontSize: 10, color: data.email_confidence === 'high' ? '#10b981' : '#f59e0b', background: data.email_confidence === 'high' ? 'rgba(16,185,129,0.1)' : 'rgba(245,158,11,0.1)', padding: '1px 7px', borderRadius: 4, fontWeight: 600 }}>
                            {data.email_source || 'found'} · {data.email_confidence || ''}
                          </span>
                        </div>
                      : <div style={{ ...contactRow, opacity: 0.5 }}><Mail size={13} /> <span style={{ fontSize: 12, color: 'var(--text-3)' }}>Email not found</span></div>
                    }
                  </FieldReveal>
                  {/* Phone */}
                  <FieldReveal index={1}>
                    {data.phone
                      ? <div style={contactRow}>
                          <Phone size={13} color="#6366f1" />
                          <span style={{ color: 'var(--text-1)', fontSize: 13, fontWeight: 500 }}>{data.phone}</span>
                        </div>
                      : <div style={{ ...contactRow, opacity: 0.5 }}><Phone size={13} /> <span style={{ fontSize: 12, color: 'var(--text-3)' }}>Phone not found</span></div>
                    }
                  </FieldReveal>
                </div>
              )}
            </StreamSection>

            {/* Stage 4 — Website Intelligence */}
            <StreamSection title="Website Intelligence" stageId="website" stages={stages} icon={<Globe size={13} />}
              skeleton={<SkeletonWebsite />}>
              {(data) => (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  {/* Value proposition */}
                  {data.value_proposition && (
                    <FieldReveal index={0}>
                      <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)', fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6, fontStyle: 'italic' }}>
                        "{data.value_proposition}"
                      </div>
                    </FieldReveal>
                  )}
                  {/* Category badges */}
                  <FieldReveal index={1}>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 7 }}>
                      {data.business_model     && <span style={tagStyle('#6366f1')}>{data.business_model}</span>}
                      {data.product_category   && <span style={tagStyle('#8b5cf6')}>{data.product_category}</span>}
                      {data.market_positioning && <span style={tagStyle('#0891b2')}>{data.market_positioning}</span>}
                      {data.pricing_signals    && <span style={tagStyle('#059669')}>{data.pricing_signals}</span>}
                      {data.hiring_signals     && <span style={tagStyle('#d97706')}>{data.hiring_signals} hiring</span>}
                    </div>
                  </FieldReveal>
                  {/* Product offerings */}
                  {data.product_offerings?.length > 0 && (
                    <FieldReveal index={2}>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--text-3)', marginBottom: 5, fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase' }}>Product Offerings</div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                          {data.product_offerings.slice(0,6).map((p, i) => <span key={i} style={tagStyle('#6366f1')}>{p}</span>)}
                        </div>
                      </div>
                    </FieldReveal>
                  )}
                  {/* Target customers */}
                  {data.target_customers?.length > 0 && (
                    <FieldReveal index={3}>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--text-3)', marginBottom: 5, fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase' }}>Target Customers</div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                          {data.target_customers.slice(0,5).map((c, i) => <span key={i} style={tagStyle('#0891b2')}>{c}</span>)}
                        </div>
                      </div>
                    </FieldReveal>
                  )}
                  {/* Problem solved */}
                  {data.problem_solved && (
                    <FieldReveal index={4}>
                      <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.6 }}>
                        <span style={{ color: 'var(--text-3)', fontWeight: 700 }}>Problem solved: </span>{data.problem_solved}
                      </div>
                    </FieldReveal>
                  )}
                  {/* Pages scraped footer */}
                  {data.pages_scraped?.length > 0 && (
                    <FieldReveal index={5}>
                      <div style={{ fontSize: 10, color: 'var(--text-3)', paddingTop: 4, borderTop: '1px solid var(--border-1)' }}>
                        Pages scraped: {data.pages_scraped.join(' · ')}
                      </div>
                    </FieldReveal>
                  )}
                </div>
              )}
            </StreamSection>

            {/* Stage 5 — Scoring + Outreach */}
            <StreamSection title="Scoring + Outreach" stageId="scoring" stages={stages} icon={<Target size={13} />}
              skeleton={<SkeletonScoring />}>
              {(data) => (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  {/* Score row */}
                  <FieldReveal index={0}>
                    <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'center' }}>
                      <ScoreCircle score={data.total_score} tier={data.score_tier} />
                      <TierBadge tier={data.score_tier} />
                      {data.icp_match_tier && <span style={{ fontSize: 11, color: 'var(--text-2)' }}>{data.icp_match_tier}</span>}
                    </div>
                  </FieldReveal>
                  {/* Score breakdown */}
                  <FieldReveal index={1}>
                    <div style={{ display: 'flex', gap: 1 }}>
                      {[
                        { label: 'ICP Fit',  val: data.icp_fit_score, max: 40, color: '#6366f1' },
                        { label: 'Intent',   val: data.intent_score,  max: 30, color: '#8b5cf6' },
                        { label: 'Timing',   val: data.timing_score,  max: 20, color: '#0891b2' },
                      ].map(({ label, val, max, color }) => (
                        <div key={label} style={{ flex: 1, padding: '8px 10px', background: 'var(--bg-elevated)', borderRadius: 6, textAlign: 'center', border: '1px solid var(--border-1)', margin: '0 3px' }}>
                          <div style={{ fontSize: 20, fontWeight: 700, color, lineHeight: 1 }}>{val || 0}</div>
                          <div style={{ fontSize: 9, color: 'var(--text-3)', marginTop: 3, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label} / {max}</div>
                        </div>
                      ))}
                    </div>
                  </FieldReveal>
                  {/* Score explanation */}
                  {data.score_explanation && (
                    <FieldReveal index={2}>
                      <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.6, padding: '8px 10px', background: 'var(--bg-elevated)', borderRadius: 6, border: '1px solid var(--border-1)' }}>
                        {data.score_explanation}
                      </div>
                    </FieldReveal>
                  )}
                  {/* Best channel */}
                  {data.best_channel && (
                    <FieldReveal index={3}>
                      <div style={{ fontSize: 11, color: 'var(--text-3)', display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
                        <span style={tagStyle('#6366f1')}>{data.best_channel}</span>
                        {data.outreach_angle && <span style={{ color: 'var(--text-2)' }}>Angle: {data.outreach_angle}</span>}
                      </div>
                    </FieldReveal>
                  )}
                  {/* Cold email */}
                  {data.cold_email && (
                    <FieldReveal index={4}>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--text-3)', fontWeight: 700, marginBottom: 5, display: 'flex', gap: 6, alignItems: 'center', letterSpacing: '0.05em', textTransform: 'uppercase' }}>
                          <Send size={10} /> Cold Email
                          {data.email_subject && <span style={{ color: 'var(--text-2)', fontWeight: 400, textTransform: 'none', fontSize: 11, fontStyle: 'italic' }}>"{data.email_subject}"</span>}
                        </div>
                        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.7, padding: '10px 13px', borderRadius: 8, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', whiteSpace: 'pre-wrap' }}>
                          {data.cold_email}
                        </div>
                      </div>
                    </FieldReveal>
                  )}
                  {/* LinkedIn note */}
                  {data.linkedin_note && (
                    <FieldReveal index={5}>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--text-3)', fontWeight: 700, marginBottom: 5, display: 'flex', gap: 6, alignItems: 'center', letterSpacing: '0.05em', textTransform: 'uppercase' }}>
                          <Linkedin size={10} /> LinkedIn Note
                        </div>
                        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.7, padding: '10px 13px', borderRadius: 8, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
                          {data.linkedin_note}
                        </div>
                      </div>
                    </FieldReveal>
                  )}
                </div>
              )}
            </StreamSection>

            {/* Stage 6 — Complete: show full report */}
            {result && (
              <>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 4 }}>
                  <CheckCircle size={15} color="#10b981" />
                  <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>Enrichment complete — {result.name}</span>
                  <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
                    <GhostBtn onClick={async () => {
                      setJsonLoading(true)
                      try {
                        const r = await fetch(`${BACKEND}/leads/${result.id}`, { headers: jsonHdr() })
                        setJsonModal(await r.json())
                      } catch { toast.error('Failed to load JSON') }
                      finally { setJsonLoading(false) }
                    }}>
                      <Code2 size={12} /> {jsonLoading ? 'Loading…' : 'View JSON'}
                    </GhostBtn>
                  </div>
                </div>
                <LeadFullReport lead={result} />
                {jsonModal && <JsonModal data={jsonModal} onClose={() => setJsonModal(null)} />}
              </>
            )}
          </div>
        </div>
      )}

      {/* Inject spin + pulse keyframes */}
      <style>{`
        @keyframes spin        { to { transform: rotate(360deg); } }
        @keyframes pulse       { 0%,100% { opacity:1; } 50% { opacity:0.25; } }
        @keyframes fieldReveal { from { opacity:0; transform:translateY(5px); } to { opacity:1; transform:translateY(0); } }
      `}</style>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// FieldReveal — staggered field-by-field appear animation
// ─────────────────────────────────────────────────────────────────────────────
function FieldReveal({ index = 0, children, style }) {
  return (
    <div style={{ animation: 'fieldReveal 0.32s ease forwards', animationDelay: `${index * 85}ms`, opacity: 0, ...style }}>
      {children}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamSection — shows shaped skeleton while loading, field-by-field when done
// ─────────────────────────────────────────────────────────────────────────────
function StreamSection({ title, stageId, stages, icon, skeleton, children }) {
  const st = stages[stageId] || { status: 'waiting' }
  if (st.status === 'waiting') return null

  return (
    <div style={{
      borderRadius: 10, border: `1px solid ${st.status === 'error' ? 'rgba(239,68,68,0.3)' : 'var(--border-1)'}`,
      background: 'var(--bg-main)', overflow: 'hidden',
    }}>
      <div style={{
        padding: '9px 14px', borderBottom: '1px solid var(--border-1)',
        display: 'flex', alignItems: 'center', gap: 7,
        background: st.status === 'loading' ? 'rgba(99,102,241,0.04)' : 'transparent',
      }}>
        <span style={{ color: st.status === 'done' ? '#10b981' : st.status === 'error' ? '#ef4444' : '#6366f1' }}>{icon}</span>
        <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)' }}>{title}</span>
        {st.status === 'loading' && <span style={{ fontSize: 10, color: '#a5b4fc', marginLeft: 2, display: 'flex', alignItems: 'center', gap: 4 }}>
          <div style={{ width: 5, height: 5, borderRadius: '50%', background: '#6366f1', animation: 'pulse 0.9s ease-in-out infinite' }} /> Fetching…
        </span>}
        {st.status === 'error' && <span style={{ fontSize: 10, color: '#ef4444', marginLeft: 2 }}>Failed</span>}
      </div>
      <div style={{ padding: '12px 14px' }}>
        {st.status === 'loading' && (skeleton || <SkeletonLines />)}
        {st.status === 'done'    && st.data && children(st.data)}
        {st.status === 'error'   && <span style={{ fontSize: 12, color: '#ef4444' }}>Stage failed — enrichment continues</span>}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-stage shaped skeletons
// ─────────────────────────────────────────────────────────────────────────────
const SK = ({ w = '100%', h = 10, r = 5, delay = 0, style: s }) => (
  <div style={{ width: w, height: h, borderRadius: r, background: 'var(--bg-elevated)', animation: `pulse 1.5s ease-in-out ${delay}s infinite`, ...s }} />
)

function SkeletonProfile() {
  return (
    <div style={{ display: 'flex', gap: 14 }}>
      <SK w={54} h={54} r={10} style={{ flexShrink: 0 }} />
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8, paddingTop: 4 }}>
        <SK w="55%" h={14} r={4} delay={0} />
        <SK w="70%" h={10} r={4} delay={0.1} />
        <div style={{ display: 'flex', gap: 7 }}>
          <SK w={90} h={20} r={10} delay={0.15} />
          <SK w={110} h={20} r={10} delay={0.2} />
          <SK w={85} h={20} r={10} delay={0.25} />
        </div>
        <SK w="90%" h={9} r={4} delay={0.3} />
        <SK w="75%" h={9} r={4} delay={0.35} />
      </div>
    </div>
  )
}

function SkeletonCompany() {
  return (
    <div style={{ display: 'flex', gap: 14 }}>
      <SK w={44} h={44} r={8} style={{ flexShrink: 0 }} />
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8 }}>
        <SK w="50%" h={11} r={4} delay={0} />
        <div style={{ display: 'flex', gap: 7 }}>
          <SK w={80} h={20} r={10} delay={0.1} />
          <SK w={120} h={20} r={10} delay={0.15} />
          <SK w={100} h={20} r={10} delay={0.2} />
        </div>
        <div style={{ display: 'flex', gap: 7 }}>
          <SK w={70} h={20} r={10} delay={0.25} />
          <SK w={90} h={20} r={10} delay={0.3} />
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          {[55,65,50,70,60].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.35 + i * 0.05} />)}
        </div>
      </div>
    </div>
  )
}

function SkeletonContact() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 10px', borderRadius: 7, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
        <SK w={14} h={14} r={7} />
        <SK w="50%" h={10} r={4} delay={0.05} />
        <SK w={60} h={18} r={9} delay={0.1} style={{ marginLeft: 'auto' }} />
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 10px', borderRadius: 7, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
        <SK w={14} h={14} r={7} delay={0.1} />
        <SK w="35%" h={10} r={4} delay={0.15} />
      </div>
    </div>
  )
}

function SkeletonWebsite() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ padding: '10px 12px', borderRadius: 8, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', display: 'flex', flexDirection: 'column', gap: 6 }}>
        <SK w="90%" h={9} r={4} delay={0} />
        <SK w="75%" h={9} r={4} delay={0.1} />
      </div>
      <div style={{ display: 'flex', gap: 7 }}>
        {[70,90,85,80].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.15 + i * 0.05} />)}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <SK w={110} h={9} r={4} delay={0.35} />
        <div style={{ display: 'flex', gap: 6 }}>
          {[80,100,70,90].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.4 + i * 0.05} />)}
        </div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <SK w={110} h={9} r={4} delay={0.6} />
        <div style={{ display: 'flex', gap: 6 }}>
          {[90,75,85].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.65 + i * 0.05} />)}
        </div>
      </div>
      <SK w="80%" h={9} r={4} delay={0.8} />
    </div>
  )
}

function SkeletonScoring() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
        <SK w={48} h={48} r={24} delay={0} />
        <SK w={70} h={22} r={11} delay={0.1} />
        <SK w={120} h={11} r={4} delay={0.15} />
      </div>
      <div style={{ display: 'flex', gap: 6 }}>
        {[1,2,3].map(i => (
          <div key={i} style={{ flex: 1, padding: '8px 10px', borderRadius: 6, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6 }}>
            <SK w={28} h={22} r={4} delay={i * 0.1} />
            <SK w="70%" h={8} r={4} delay={i * 0.1 + 0.05} />
          </div>
        ))}
      </div>
      <SK w="100%" h={36} r={6} delay={0.35} />
      <div style={{ display: 'flex', gap: 7 }}>
        <SK w={80} h={22} r={11} delay={0.4} />
        <SK w="55%" h={11} r={4} delay={0.45} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
        <SK w={70} h={9} r={4} delay={0.5} />
        <SK w="100%" h={70} r={6} delay={0.55} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
        <SK w={80} h={9} r={4} delay={0.7} />
        <SK w="100%" h={40} r={6} delay={0.75} />
      </div>
    </div>
  )
}

function SkeletonLines() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {[80, 55, 65].map((w, i) => (
        <SK key={i} w={`${w}%`} h={10} r={5} delay={i * 0.12} />
      ))}
    </div>
  )
}

const tagStyle = (color) => ({
  display: 'inline-flex', alignItems: 'center', gap: 4,
  fontSize: 10, fontWeight: 500, padding: '2px 8px', borderRadius: 5,
  background: `${color}18`, color, border: `1px solid ${color}30`,
})

const contactRow = {
  display: 'flex', alignItems: 'center', gap: 7,
  padding: '6px 10px', borderRadius: 7,
  background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
}

// ─────────────────────────────────────────────────────────────────────────────
// Bulk Tab
// ─────────────────────────────────────────────────────────────────────────────
function BulkTab({ onDone }) {
  const [mode, setMode] = useState('text')
  const [urlsText, setUrlsText] = useState('')
  const [webhookUrl, setWebhookUrl] = useState('')
  const [loading, setLoading] = useState(false)
  const [job, setJob] = useState(null)

  // Accept both person (/in/) and company (/company/) URLs
  const urls = urlsText.split(/[\n,]+/).map(u => u.trim()).filter(u => {
    const l = u.toLowerCase()
    return u && l.includes('linkedin.com') && (l.includes('/in/') || l.includes('/company/'))
  })
  const personCount  = urls.filter(u => u.toLowerCase().includes('/in/')).length
  const companyCount = urls.filter(u => u.toLowerCase().includes('/company/')).length

  const handleFile = (e) => {
    const file = e.target.files?.[0]; if (!file) return
    const r = new FileReader()
    r.onload = ev => {
      const text = ev.target.result
      const people    = text.match(/https?:\/\/[^\s,"]+linkedin\.com\/in\/[^\s,"]+/gi) || []
      const companies = text.match(/https?:\/\/[^\s,"]+linkedin\.com\/company\/[^\s,"]+/gi) || []
      const found = [...people, ...companies]
      setUrlsText(found.join('\n'))
      toast.success(`Found ${found.length} URLs (${people.length} people, ${companies.length} companies)`)
    }
    r.readAsText(file)
  }

  const handleBulk = async () => {
    if (!urls.length) return toast.error('No valid LinkedIn URLs found')
    setLoading(true); setJob(null)
    try {
      const body = { token: getToken(), linkedin_urls: urls, forward_to_lio: false }
      if (webhookUrl.trim()) body.webhook_url = webhookUrl.trim()
      const r = await fetch(`${BACKEND}/leads/enrich/bulk`, {
        method: 'POST', headers: jsonHdr(), body: JSON.stringify(body),
      })
      const d = await r.json()
      if (!r.ok) throw new Error(d.detail || 'Bulk trigger failed')
      setJob(d.job)
      toast.success(`Batch job started for ${urls.length} URLs`)
      onDone()
    } catch (e) { toast.error(e.message) }
    finally { setLoading(false) }
  }

  return (
    <div style={{ maxWidth: 800 }}>
      <Card style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <Lbl>
            LinkedIn URLs
            {urls.length > 0 && (
              <span style={{ marginLeft: 8, fontSize: 11, fontWeight: 400, color: 'var(--text-3)' }}>
                {urls.length} total
                {personCount > 0 && <span style={{ color: '#10b981', marginLeft: 5 }}>· {personCount} people</span>}
                {companyCount > 0 && <span style={{ color: '#6366f1', marginLeft: 5 }}>· {companyCount} companies</span>}
              </span>
            )}
          </Lbl>
          <div style={{ display: 'flex', gap: 6 }}>
            {['text','csv'].map(m => (
              <button key={m} onClick={() => setMode(m)} style={{
                padding: '4px 11px', borderRadius: 6, fontSize: 11, fontWeight: 500, cursor: 'pointer',
                border: `1px solid ${mode===m ? '#6366f1':'var(--border-1)'}`,
                background: mode===m ? 'rgba(99,102,241,0.1)':'transparent',
                color: mode===m ? '#a5b4fc':'var(--text-3)',
              }}>{m === 'text' ? 'Paste URLs' : 'Upload CSV'}</button>
            ))}
          </div>
        </div>
        {mode === 'text' ? (
          <textarea value={urlsText} onChange={e => setUrlsText(e.target.value)} rows={8}
            placeholder={"https://www.linkedin.com/in/alice\nhttps://www.linkedin.com/in/bob\nhttps://www.linkedin.com/company/acme-corp"}
            style={{ ...inputStyle, width: '100%', resize: 'vertical', fontFamily: 'monospace', fontSize: 12, boxSizing: 'border-box' }} />
        ) : (
          <label style={{
            display: 'flex', flexDirection: 'column', alignItems: 'center',
            border: '2px dashed var(--border-1)', borderRadius: 10, padding: '28px 20px',
            cursor: 'pointer', gap: 8, color: 'var(--text-3)',
          }}>
            <Upload size={20} />
            <span style={{ fontSize: 13 }}>Click to upload CSV</span>
            <input type="file" accept=".csv,.txt" onChange={handleFile} style={{ display: 'none' }} />
          </label>
        )}
        {urls.length > 0 && (
          <div style={{ marginTop: 10, padding: '7px 12px', borderRadius: 7, background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)', fontSize: 12, color: 'var(--text-2)', display: 'flex', gap: 6, alignItems: 'center' }}>
            <CheckCircle size={12} color="#a5b4fc" /> <strong style={{ color: '#a5b4fc' }}>{urls.length}</strong> valid URLs ready
          </div>
        )}
      </Card>
      <Card style={{ marginBottom: 16 }}>
        <Lbl>Webhook URL (optional)</Lbl>
        <input value={webhookUrl} onChange={e => setWebhookUrl(e.target.value)}
          placeholder="https://your-app.com/api/leads/webhook/brightdata"
          style={{ ...inputStyle, width: '100%', marginTop: 8, boxSizing: 'border-box' }} />
      </Card>
      <Btn onClick={handleBulk} loading={loading} disabled={!urls.length}>
        {loading ? <RefreshSpin /> : <Upload size={14} />}
        {loading ? 'Starting…' : `Enrich ${urls.length || 0} Leads`}
      </Btn>
      {job && (
        <div style={{ marginTop: 16, padding: '14px 18px', borderRadius: 10, background: 'rgba(16,185,129,0.06)', border: '1px solid rgba(16,185,129,0.25)' }}>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 8 }}>
            <CheckCircle size={14} color="#10b981" />
            <span style={{ fontSize: 13, fontWeight: 600, color: '#10b981' }}>Batch job created</span>
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-2)' }}>
            Job ID: <code style={{ fontFamily: 'monospace', background: 'var(--bg-elevated)', padding: '1px 5px', borderRadius: 4 }}>{job.id}</code>
            &nbsp;·&nbsp; <StatusBadge status={job.status} /> &nbsp;·&nbsp; {job.total_urls} URLs
          </div>
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Results Tab
// ─────────────────────────────────────────────────────────────────────────────
function ResultsTab({ leads, total, loading, filters, onFiltersChange, searchInput, onSearchInput, onRefresh, onLeadDeleted, jobs, page, pageSize, onPageChange, onPageSizeChange }) {
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

// ─────────────────────────────────────────────────────────────────────────────
// Jobs Tab
// ─────────────────────────────────────────────────────────────────────────────
function JobsTab({ jobs, onRefresh, onSelectJob }) {
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

// ─────────────────────────────────────────────────────────────────────────────
// LeadEnrichView — 4 enrichment containers (LinkedIn, Email, Outreach, Company)
// ─────────────────────────────────────────────────────────────────────────────
const ENRICH_TABS = [
  { id: 'linkedin',           label: 'LinkedIn Enrich',    icon: '🔗', color: '#6366f1' },
  { id: 'email',              label: 'Email Enrich',       icon: '✉️', color: '#10b981' },
  { id: 'outreach',           label: 'Outreach Enrich',    icon: '📤', color: '#f59e0b' },
  { id: 'company',            label: 'Company Enrich',     icon: '🏢', color: '#3b82f6' },
  { id: 'brightdata_profile', label: 'BD Profile (Raw)',   icon: '👤', color: '#8b5cf6', rawData: true },
  { id: 'brightdata_company', label: 'BD Company (Raw)',   icon: '🏗️', color: '#06b6d4', rawData: true },
  { id: 'apollo_raw',         label: 'Apollo Raw',         icon: '🚀', color: '#f97316', rawData: true },
  { id: 'website_scrap',      label: 'Website Scrap',      icon: '🌐', color: '#84cc16', rawData: true },
]

// IDs of tabs that load from the single /raw-data endpoint
const RAW_DATA_TABS = new Set(['brightdata_profile', 'brightdata_company', 'apollo_raw', 'website_scrap'])

function EnrichJsonView({ data }) {
  if (!data) return null
  return (
    <pre style={{
      margin: 0, padding: '12px 14px', fontSize: 11, lineHeight: 1.6,
      background: '#ffffff', color: '#000000',
      borderRadius: 8, overflowX: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-word',
    }}>
      {JSON.stringify(data, null, 2)}
    </pre>
  )
}


function _RegenJsonView({ data, leadenrich_id, endpoint, viewTab, onRegenerated, accentColor, directFetch }) {
  const [regenerating, setRegenerating] = useState(false)

  const handleRegenerate = async () => {
    setRegenerating(true)
    try {
      // directFetch=true: skip the regen step, just re-fetch the view tab directly (used for email)
      if (!directFetch) {
        const r = await fetch(`${BACKEND}/leads/${leadenrich_id}/${endpoint}`, { method: 'POST', headers: jsonHdr() })
        if (!r.ok) {
          const d = await r.json().catch(() => ({}))
          throw new Error(d?.detail || `HTTP ${r.status}`)
        }
      }
      const isGetTab = viewTab === 'linkedin'
      const v = await fetch(
        isGetTab
          ? `${BACKEND}/leads/view/${viewTab}?leadenrich_id=${encodeURIComponent(leadenrich_id)}`
          : `${BACKEND}/leads/view/${viewTab}`,
        isGetTab
          ? { headers: jsonHdr() }
          : { method: 'POST', headers: jsonHdr(), body: JSON.stringify({ leadenrich_id }) }
      )
      const fresh = await v.json()
      if (!v.ok) throw new Error(fresh?.detail || `HTTP ${v.status}`)
      // Credit exhausted — show warning, don't update cache
      if (fresh?.source === 'apollo_credit_exhausted') {
        toast.error('Apollo credit balance exhausted. Please recharge your Apollo plan to continue email enrichment.', { duration: 6000 })
        return
      }
      onRegenerated(fresh)
      toast.success('Regenerated')
    } catch (e) {
      toast.error('Regenerate failed: ' + e.message)
    } finally {
      setRegenerating(false)
    }
  }

  if (!data) return null
  return (
    <div>
      <div style={{ marginBottom: 10, display: 'flex', justifyContent: 'flex-end' }}>
        <button onClick={handleRegenerate} disabled={regenerating} style={{
          display: 'flex', alignItems: 'center', gap: 5, padding: '5px 12px',
          borderRadius: 7, border: `1px solid ${accentColor}55`,
          background: regenerating ? `${accentColor}10` : `${accentColor}18`,
          color: accentColor, fontSize: 11, fontWeight: 600,
          cursor: regenerating ? 'not-allowed' : 'pointer',
          opacity: regenerating ? 0.7 : 1, transition: 'opacity 0.15s',
        }}>
          <RefreshCw size={11} style={{ animation: regenerating ? 'spin 1s linear infinite' : 'none' }} />
          {regenerating ? 'Regenerating…' : 'Regenerate'}
        </button>
      </div>
      <EnrichJsonView data={data} />
    </div>
  )
}

function LinkedInEnrichView({ data, leadenrich_id, onRegenerated }) {
  return (
    <_RegenJsonView
      data={data}
      leadenrich_id={leadenrich_id}
      endpoint="crm-brief"
      viewTab="linkedin"
      onRegenerated={onRegenerated}
      accentColor="#6366f1"
    />
  )
}

function OutreachEnrichView({ data, leadenrich_id, onRegenerated }) {
  return (
    <_RegenJsonView
      data={data}
      leadenrich_id={leadenrich_id}
      endpoint="outreach"
      viewTab="outreach"
      onRegenerated={onRegenerated}
      accentColor="#f59e0b"
    />
  )
}

function CompanyEnrichView({ data, leadenrich_id, onRegenerated }) {
  return (
    <_RegenJsonView
      data={data}
      leadenrich_id={leadenrich_id}
      endpoint="company"
      viewTab="company"
      onRegenerated={onRegenerated}
      accentColor="#3b82f6"
    />
  )
}

function LeadEnrichView({ lead }) {
  const [activeTab, setActiveTab] = useState('linkedin')
  const [cache, setCache]     = useState({})   // { tabId: data }
  const [loading, setLoading] = useState({})   // { tabId: bool }
  const [errors, setErrors]   = useState({})   // { tabId: msg }

  const leadenrich_id = lead?.id

  const _fetchViewTab = async (tabId) => {
    // Raw-data tabs all load from a single endpoint
    if (RAW_DATA_TABS.has(tabId)) {
      const r = await fetch(`${BACKEND}/leads/${leadenrich_id}/raw-data`, { headers: jsonHdr() })
      const data = await r.json()
      if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
      // Each raw tab shows only its own field
      const fieldMap = {
        brightdata_profile: 'brightdata_profile',
        brightdata_company: 'brightdata_company',
        apollo_raw:         'apollo_raw',
        website_scrap:      'website_scrap',
      }
      return data[fieldMap[tabId]] ?? null
    }
    // linkedin uses GET + query param; email/outreach/company use POST + JSON body
    const isGet = tabId === 'linkedin'
    const url = isGet
      ? `${BACKEND}/leads/view/${tabId}?leadenrich_id=${encodeURIComponent(leadenrich_id)}`
      : `${BACKEND}/leads/view/${tabId}`
    const opts = isGet
      ? { headers: jsonHdr() }
      : { method: 'POST', headers: jsonHdr(), body: JSON.stringify({ leadenrich_id }) }
    const r = await fetch(url, opts)
    const data = await r.json()
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  }

  const fetchTab = async (tabId) => {
    if (cache[tabId] || loading[tabId]) return
    setLoading(p => ({ ...p, [tabId]: true }))
    setErrors(p => ({ ...p, [tabId]: null }))
    try {
      const data = await _fetchViewTab(tabId)
      setCache(p => ({ ...p, [tabId]: data }))
    } catch (e) {
      setErrors(p => ({ ...p, [tabId]: e.message }))
    } finally {
      setLoading(p => ({ ...p, [tabId]: false }))
    }
  }

  const refreshTab = async (tabId) => {
    setLoading(p => ({ ...p, [tabId]: true }))
    setErrors(p => ({ ...p, [tabId]: null }))
    try {
      const data = await _fetchViewTab(tabId)
      setCache(p => ({ ...p, [tabId]: data }))
    } catch (e) {
      setErrors(p => ({ ...p, [tabId]: e.message }))
    } finally {
      setLoading(p => ({ ...p, [tabId]: false }))
    }
  }

  // Auto-load LinkedIn tab on mount
  useEffect(() => {
    if (leadenrich_id) fetchTab('linkedin')
  }, [leadenrich_id])

  const handleTabClick = (tabId) => {
    setActiveTab(tabId)
    fetchTab(tabId)
  }

  return (
    <div style={{ padding: '12px 14px' }}>
      {/* leadenrich_id badge */}
      <div style={{ marginBottom: 10, display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ fontSize: 10, color: 'var(--text-3)' }}>leadenrich_id:</span>
        <code style={{
          fontSize: 10, padding: '2px 7px', borderRadius: 5,
          background: 'rgba(99,102,241,0.12)', color: '#a5b4fc', fontFamily: 'monospace',
        }}>{leadenrich_id}</code>
      </div>

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 12, borderBottom: '1px solid var(--border-1)', paddingBottom: 0 }}>
        {ENRICH_TABS.map(tab => {
          const isActive = activeTab === tab.id
          const isLoading = loading[tab.id]
          const isDone = !!cache[tab.id]
          return (
            <button key={tab.id} onClick={() => handleTabClick(tab.id)} style={{
              display: 'flex', alignItems: 'center', gap: 5, padding: '7px 12px',
              border: 'none', borderBottom: isActive ? `2px solid ${tab.color}` : '2px solid transparent',
              background: 'transparent', cursor: 'pointer', fontSize: 11, fontWeight: isActive ? 600 : 400,
              color: isActive ? tab.color : 'var(--text-3)', whiteSpace: 'nowrap',
              borderRadius: '6px 6px 0 0', transition: 'color 0.15s',
            }}>
              <span>{tab.icon}</span>
              {tab.label}
              {isLoading && <span style={{ fontSize: 9, opacity: 0.7 }}>⟳</span>}
              {!isLoading && isDone && <span style={{ fontSize: 8, color: '#10b981' }}>●</span>}
            </button>
          )
        })}
      </div>

      {/* Content */}
      {ENRICH_TABS.map(tab => {
        if (activeTab !== tab.id) return null
        const isLoading = loading[tab.id]
        const err = errors[tab.id]
        const data = cache[tab.id]
        return (
          <div key={tab.id}>
            {isLoading && (
              <div style={{ padding: '30px 0', textAlign: 'center', color: 'var(--text-3)', fontSize: 12 }}>
                Loading {tab.label}…
              </div>
            )}
            {!isLoading && err && (
              <div style={{ padding: '12px 14px', borderRadius: 8, background: 'rgba(239,68,68,0.08)',
                color: '#f87171', fontSize: 12 }}>
                {err}
              </div>
            )}
            {!isLoading && !err && data && tab.id === 'linkedin' && (
              <LinkedInEnrichView
                data={data}
                leadenrich_id={leadenrich_id}
                onRegenerated={fresh => setCache(p => ({ ...p, linkedin: fresh }))}
              />
            )}
            {!isLoading && !err && data && tab.id === 'outreach' && (
              <OutreachEnrichView
                data={data}
                leadenrich_id={leadenrich_id}
                onRegenerated={fresh => setCache(p => ({ ...p, outreach: fresh }))}
              />
            )}
            {!isLoading && !err && data && tab.id === 'company' && (
              <CompanyEnrichView
                data={data}
                leadenrich_id={leadenrich_id}
                onRegenerated={fresh => setCache(p => ({ ...p, company: fresh }))}
              />
            )}
            {!isLoading && !err && data && tab.id === 'email' && (
              <_RegenJsonView
                data={data}
                leadenrich_id={leadenrich_id}
                viewTab="email"
                directFetch={true}
                onRegenerated={fresh => setCache(p => ({ ...p, email: fresh }))}
                accentColor="#10b981"
              />
            )}
            {!isLoading && !err && data && RAW_DATA_TABS.has(tab.id) && (
              <EnrichJsonView data={data} />
            )}
            {!isLoading && !err && data && !RAW_DATA_TABS.has(tab.id) && tab.id !== 'linkedin' && tab.id !== 'outreach' && tab.id !== 'company' && tab.id !== 'email' && (
              <EnrichJsonView data={data} />
            )}
            {!isLoading && !err && !data && (
              <div style={{ padding: '30px 0', textAlign: 'center', color: 'var(--text-3)', fontSize: 12 }}>
                No data yet
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// LeadFullReport — 8-stage waterfall layout
// ─────────────────────────────────────────────────────────────────────────────
function LeadFullReport({ lead, compact }) {
  const [section, setSection] = useState('identity')

  const full = (() => {
    try {
      if (lead.full_data && typeof lead.full_data === 'object') return lead.full_data
      return JSON.parse(lead.full_data || '{}')
    } catch { return {} }
  })()

  const SECTIONS = [
    { id: 'identity',    label: '1 · Identity',    icon: <User          size={12} /> },
    { id: 'professional',label: '2 · Professional', icon: <GraduationCap size={12} /> },
    { id: 'company',     label: '3 · Company',      icon: <Building2     size={12} /> },
    { id: 'website',     label: '4 · Website Intel',icon: <Globe         size={12} />, badge: lead.product_category ? '●' : null },
    { id: 'intent',      label: '5 · Intent',       icon: <TrendingUp    size={12} /> },
    { id: 'scoring',     label: '6 · Scoring',      icon: <Target        size={12} /> },
    { id: 'ai_insights',  label: '7 · AI Insights',   icon: <Zap           size={12} />, badge: lead.auto_tags ? '✦' : null },
    { id: 'co_intel',    label: '8 · Company Intel', icon: <Building2     size={12} />, badge: lead.company_score > 0 ? '✦' : null },
    { id: 'outreach',    label: '9 · Outreach',      icon: <Send          size={12} /> },
    { id: 'crm',         label: '10 · CRM',          icon: <Database      size={12} /> },
    { id: 'lio',         label: '✦ LIO',              icon: <Zap           size={12} />, badge: '✦' },
  ]

  return (
    <div style={{ padding: compact ? '12px 14px' : 0 }}>
      {/* Person + Company identity header */}
      {!compact && (
        <div style={{
          display: 'flex', alignItems: 'center', gap: 14, marginBottom: 16,
          padding: '14px 16px', borderRadius: 10,
          background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
        }}>
          <Avatar name={lead.name} tier={lead.score_tier} src={lead.avatar_url} size={54} />
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 3 }}>
              <span style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-1)' }}>{lead.name || '—'}</span>
              <TierBadge tier={lead.score_tier} />
              <ScoreCircle score={lead.total_score} tier={lead.score_tier} />
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-2)', marginBottom: 2 }}>
              {[lead.title, lead.company].filter(Boolean).join(' at ')}
            </div>
            <div style={{ fontSize: 11, color: 'var(--text-3)' }}>
              {[fmtCity(lead.city), fmtCountry(lead.country)].filter(Boolean).join(', ')}
              {lead.work_email && <span style={{ marginLeft: 10, color: '#10b981' }}>✉ {lead.work_email}</span>}
            </div>
          </div>
          {/* Company block */}
          {lead.company && (
            <div style={{
              display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6,
              padding: '10px 14px', borderRadius: 9,
              background: 'var(--bg-base)', border: '1px solid var(--border-1)', flexShrink: 0,
            }}>
              <CompanyLogo src={lead.company_logo} name={lead.company} size={36} />
              <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-1)', textAlign: 'center', maxWidth: 120,
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {lead.company}
              </div>
              {(lead.company_website || lead.company_email) && (
                <div style={{ fontSize: 10, color: 'var(--text-3)', textAlign: 'center' }}>
                  {lead.company_website && (
                    <a href={lead.company_website.startsWith('http') ? lead.company_website : `https://${lead.company_website}`}
                      target="_blank" rel="noreferrer"
                      style={{ color: '#6366f1', textDecoration: 'none', display: 'block' }}>
                      {lead.company_website.replace(/^https?:\/\//, '')}
                    </a>
                  )}
                  {lead.company_email && <span style={{ color: '#10b981' }}>{lead.company_email}</span>}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Section tabs */}
      <div style={{ display: 'flex', gap: 2, marginBottom: 14, overflowX: 'auto', paddingBottom: 2,
        borderBottom: '1px solid var(--border-1)', flexWrap: 'nowrap' }}>
        {SECTIONS.map(s => (
          <button key={s.id} onClick={() => setSection(s.id)} style={{
            display: 'flex', alignItems: 'center', gap: 5, padding: '6px 11px',
            border: 'none', borderBottom: section===s.id ? '2px solid #6366f1' : '2px solid transparent',
            background: 'transparent', color: section===s.id ? '#a5b4fc':'var(--text-3)',
            cursor: 'pointer', fontSize: 11, fontWeight: section===s.id ? 600:400,
            whiteSpace: 'nowrap', borderRadius: '6px 6px 0 0',
          }}>
            {s.icon} {s.label}
            {s.badge && <span style={{ fontSize: 8, color: '#10b981', marginLeft: 2 }}>{s.badge}</span>}
          </button>
        ))}
      </div>

      {section === 'identity'     && <IdentitySection       lead={lead} full={full} />}
      {section === 'professional' && <ProfessionalSection   lead={lead} full={full} />}
      {section === 'company'      && <CompanySection        lead={lead} full={full} />}
      {section === 'website'      && <WebsiteIntelSection   lead={lead} full={full} />}
      {section === 'intent'       && <IntentSection         lead={lead} full={full} />}
      {section === 'scoring'      && <ScoringSection        lead={lead} full={full} />}
      {section === 'ai_insights'  && <AIInsightsSection     lead={lead} full={full} />}
      {section === 'co_intel'     && <CompanyIntelSection   lead={lead} full={full} />}
      {section === 'outreach'     && <OutreachSection       lead={lead} full={full} />}
      {section === 'crm'          && <CrmSection            lead={lead} full={full} />}
      {section === 'lio'          && <AiEnrichmentSection   lead={lead} />}
    </div>
  )
}

// ── Section components ────────────────────────────────────────────────────────

// ── AI Enrichment Section ─────────────────────────────────────────────────────
function _tryParse(raw) {
  if (!raw) return null
  if (typeof raw === 'object') return raw
  try { return JSON.parse(raw) } catch { return null }
}

function NluCompanyIntel({ raw }) {
  const d = _tryParse(raw)
  if (!d) return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
  const stageBadgeColor = { startup: '#f59e0b', growth: '#10b981', enterprise: '#6366f1' }[d.company_stage] || '#6b7280'
  const techColor = { low: '#ef4444', medium: '#f59e0b', high: '#10b981' }[d.tech_maturity] || '#6b7280'
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
        {d.company_stage && <span style={{ padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700, background: stageBadgeColor + '22', border: `1px solid ${stageBadgeColor}44`, color: stageBadgeColor, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{d.company_stage}</span>}
        {d.tech_maturity && <span style={{ fontSize: 11, color: techColor, fontWeight: 600 }}>Tech: {d.tech_maturity}</span>}
      </div>
      {[['Offering', d.primary_offering], ['Business Model', d.business_model], ['Target Customer', d.target_customer]].map(([l, v]) => v ? (
        <div key={l}><div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 2 }}>{l}</div>
        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.55 }}>{v}</div></div>
      ) : null)}
      {Array.isArray(d.likely_pain_points) && d.likely_pain_points.length > 0 && (
        <div><div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 5 }}>Likely Pain Points</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
          {d.likely_pain_points.map((p, i) => <span key={i} style={{ padding: '3px 9px', borderRadius: 6, fontSize: 11, background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.25)', color: '#fca5a5' }}>{p}</span>)}
        </div></div>
      )}
    </div>
  )
}

function NluTags({ raw }) {
  const tags = _tryParse(raw)
  if (Array.isArray(tags)) return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: '4px 0' }}>
      {tags.map((t, i) => <span key={i} style={{ padding: '4px 11px', borderRadius: 20, fontSize: 11, fontWeight: 600, background: 'rgba(99,102,241,0.12)', border: '1px solid rgba(99,102,241,0.3)', color: '#a5b4fc' }}>{t}</span>)}
    </div>
  )
  return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
}

function NluSignals({ raw }) {
  const d = _tryParse(raw)
  if (!d || typeof d !== 'object') return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
  const fields = [
    { k: 'posts_about', label: 'Posts About', icon: '📝' },
    { k: 'engages_with', label: 'Engages With', icon: '👍' },
    { k: 'communication_style', label: 'Communication', icon: '💬' },
    { k: 'decision_pattern', label: 'Decision Pattern', icon: '🧠' },
    { k: 'pain_point_hint', label: 'Pain Point', icon: '⚡' },
    { k: 'warm_signal', label: 'Warm Signal', icon: '🔥' },
  ]
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {fields.map(({ k, label, icon }) => d[k] && d[k] !== 'null' ? (
        <div key={k} style={{ display: 'flex', gap: 10, padding: '8px 10px', borderRadius: 7, background: 'var(--bg-card)', border: '1px solid var(--border-1)' }}>
          <span style={{ fontSize: 14, flexShrink: 0 }}>{icon}</span>
          <div><div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 2 }}>{label}</div>
          <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.55 }}>{d[k]}</div></div>
        </div>
      ) : null)}
    </div>
  )
}

function NluBuyingSignals({ raw }) {
  const d = _tryParse(raw)
  if (!d || typeof d !== 'object') return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
  const intentColor = { low: '#6b7280', medium: '#f59e0b', high: '#10b981' }[d.intent_level] || '#6b7280'
  const actionColor = { reach_now: '#10b981', nurture: '#f59e0b', ignore: '#6b7280' }[d.recommended_action] || '#6b7280'
  const score = parseInt(d.timing_score) || 0
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
        <div style={{ padding: '6px 14px', borderRadius: 20, fontWeight: 700, fontSize: 12, background: intentColor + '22', border: `1px solid ${intentColor}44`, color: intentColor, textTransform: 'uppercase' }}>
          Intent: {d.intent_level}
        </div>
        <div style={{ padding: '6px 14px', borderRadius: 20, fontWeight: 700, fontSize: 12, background: actionColor + '22', border: `1px solid ${actionColor}44`, color: actionColor }}>
          {(d.recommended_action || '').replace(/_/g, ' ')}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <div style={{ width: 80, height: 6, borderRadius: 3, background: 'var(--bg-card)' }}>
            <div style={{ width: `${score}%`, height: '100%', borderRadius: 3, background: score > 60 ? '#10b981' : score > 30 ? '#f59e0b' : '#6b7280', transition: 'width 0.5s' }} />
          </div>
          <span style={{ fontSize: 11, color: 'var(--text-3)' }}>Timing {score}/100</span>
        </div>
      </div>
      {Array.isArray(d.trigger_events) && d.trigger_events.length > 0 && (
        <div><div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 5 }}>Trigger Events</div>
        {d.trigger_events.map((t, i) => <div key={i} style={{ fontSize: 12, color: 'var(--text-1)', padding: '5px 0', borderBottom: i < d.trigger_events.length-1 ? '1px solid var(--border-1)' : 'none', lineHeight: 1.5 }}>• {t}</div>)}
        </div>
      )}
    </div>
  )
}

function NluPitchIntel({ raw }) {
  const d = _tryParse(raw)
  if (!d || typeof d !== 'object') return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {d.core_pain && <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)' }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#f87171', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>⚡ Core Pain</div>
        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6 }}>{d.core_pain}</div>
      </div>}
      {d.value_prop && <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(16,185,129,0.08)', border: '1px solid rgba(16,185,129,0.2)' }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#6ee7b7', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>✓ Value Prop</div>
        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6 }}>{d.value_prop}</div>
      </div>}
      {d.personalization_hook && <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)' }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>🎯 Opening Hook</div>
        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6, fontStyle: 'italic' }}>"{d.personalization_hook}"</div>
      </div>}
      <div style={{ display: 'flex', gap: 8 }}>
        {d.angle && <span style={{ padding: '3px 10px', borderRadius: 6, fontSize: 11, fontWeight: 600, background: 'rgba(139,92,246,0.15)', border: '1px solid rgba(139,92,246,0.3)', color: '#c4b5fd' }}>Angle: {d.angle}</span>}
        {d.cta_strategy && <span style={{ padding: '3px 10px', borderRadius: 6, fontSize: 11, fontWeight: 600, background: 'rgba(245,158,11,0.15)', border: '1px solid rgba(245,158,11,0.3)', color: '#fcd34d' }}>CTA: {d.cta_strategy}</span>}
      </div>
      {Array.isArray(d.do_not_pitch) && d.do_not_pitch.length > 0 && (
        <div><div style={{ fontSize: 10, fontWeight: 700, color: '#f87171', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>🚫 Do NOT Say</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
          {d.do_not_pitch.map((p, i) => <span key={i} style={{ padding: '2px 8px', borderRadius: 5, fontSize: 11, background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.2)', color: '#fca5a5' }}>{p}</span>)}
        </div></div>
      )}
    </div>
  )
}

function NluOutreach({ raw }) {
  const d = _tryParse(raw)
  if (!d) return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
  const email = d.email || {}
  const li    = d.linkedin || {}
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {(email.subject || email.body) && (
        <div style={{ borderRadius: 9, border: '1px solid var(--border-1)', overflow: 'hidden' }}>
          <div style={{ padding: '7px 12px', background: 'var(--bg-card)', borderBottom: '1px solid var(--border-1)', display: 'flex', alignItems: 'center', gap: 6 }}>
            <Mail size={11} color="#6366f1" />
            <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-2)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Cold Email</span>
            {email.subject && <span style={{ marginLeft: 'auto', fontSize: 11, color: '#a5b4fc', fontWeight: 600 }}>Subject: {email.subject}</span>}
          </div>
          {email.body && <div style={{ padding: '12px', fontSize: 12, lineHeight: 1.7, color: 'var(--text-1)', whiteSpace: 'pre-wrap' }}>{email.body}</div>}
        </div>
      )}
      {li.connection_note && (
        <div style={{ borderRadius: 9, border: '1px solid var(--border-1)', overflow: 'hidden' }}>
          <div style={{ padding: '7px 12px', background: 'var(--bg-card)', borderBottom: '1px solid var(--border-1)', display: 'flex', alignItems: 'center', gap: 6 }}>
            <Linkedin size={11} color="#0a66c2" />
            <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-2)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>LinkedIn Note</span>
            <span style={{ marginLeft: 'auto', fontSize: 10, color: 'var(--text-3)' }}>{li.connection_note.length}/40w</span>
          </div>
          <div style={{ padding: '12px', fontSize: 12, lineHeight: 1.7, color: 'var(--text-1)' }}>{li.connection_note}</div>
        </div>
      )}
      {li.follow_up && (
        <div style={{ borderRadius: 9, border: '1px solid var(--border-1)', overflow: 'hidden' }}>
          <div style={{ padding: '7px 12px', background: 'var(--bg-card)', borderBottom: '1px solid var(--border-1)', display: 'flex', alignItems: 'center', gap: 6 }}>
            <MessageSquare size={11} color="#10b981" />
            <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-2)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Follow-up (after connect)</span>
          </div>
          <div style={{ padding: '12px', fontSize: 12, lineHeight: 1.7, color: 'var(--text-1)' }}>{li.follow_up}</div>
        </div>
      )}
    </div>
  )
}

function NluLeadScore({ raw }) {
  const d = _tryParse(raw)
  if (!d || typeof d !== 'object') return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
  const score = parseInt(d.lead_score) || 0
  const scoreColor = score >= 70 ? '#10b981' : score >= 40 ? '#f59e0b' : '#ef4444'
  const priorityColor = { high: '#10b981', medium: '#f59e0b', low: '#6b7280' }[d.priority] || '#6b7280'
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        <div style={{ width: 64, height: 64, borderRadius: '50%', border: `4px solid ${scoreColor}`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
          <span style={{ fontSize: 20, fontWeight: 800, color: scoreColor, lineHeight: 1 }}>{score}</span>
        </div>
        <div>
          <div style={{ fontSize: 22, fontWeight: 800, color: scoreColor }}>{score}/100</div>
          {d.priority && <span style={{ padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700, background: priorityColor + '22', border: `1px solid ${priorityColor}44`, color: priorityColor, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{d.priority} priority</span>}
        </div>
      </div>
      {d.reason && <div><div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Why this score</div>
      <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.65 }}>{d.reason}</div></div>}
      {d.next_best_action && <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)' }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>→ Next Best Action</div>
        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.55, fontWeight: 600 }}>{d.next_best_action}</div>
      </div>}
    </div>
  )
}


// ── AI Enrichment Module Definitions ─────────────────────────────────────────
const AI_MODULES = [
  { id: 'identity',             name: 'Identity',             route: 'identity',             color: '#06b6d4', icon: '👤', group: 'core' },
  { id: 'contact',              name: 'Contact',              route: 'contact',              color: '#10b981', icon: '📧', group: 'core' },
  { id: 'scores',               name: 'Scores',               route: 'scores',               color: '#f59e0b', icon: '📊', group: 'core' },
  { id: 'icp_match',            name: 'ICP Match',            route: 'icp-match',            color: '#8b5cf6', icon: '🎯', group: 'intel' },
  { id: 'behavioural_signals',  name: 'Behavioural Signals',  route: 'behavioural-signals',  color: '#6366f1', icon: '🧠', group: 'intel' },
  { id: 'pitch_intelligence',   name: 'Pitch Intelligence',   route: 'pitch-intelligence',   color: '#ec4899', icon: '💡', group: 'intel' },
  { id: 'activity',             name: 'Activity',             route: 'activity',             color: '#14b8a6', icon: '⚡', group: 'intel' },
  { id: 'tags',                 name: 'Tags',                 route: 'tags',                 color: '#a78bfa', icon: '🏷️', group: 'output' },
  { id: 'outreach',             name: 'Outreach',             route: 'outreach',             color: '#ef4444', icon: '✉️', group: 'output' },
  { id: 'persona_analysis',     name: 'Persona Analysis',     route: 'persona-analysis',     color: '#f97316', icon: '🔍', group: 'output' },
]

// ── AI NLU Renderers ──────────────────────────────────────────────────────────

function AiNluIdentity({ d }) {
  if (!d) return null
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {d.title && <span style={{ padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700, background: 'rgba(6,182,212,0.12)', border: '1px solid rgba(6,182,212,0.3)', color: '#67e8f9' }}>{d.title}</span>}
        {d.company && <span style={{ padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 600, background: 'var(--bg-card)', border: '1px solid var(--border-1)', color: 'var(--text-2)' }}>{d.company}</span>}
      </div>
      {[['Location', d.location], ['Timezone', d.timezone], ['LinkedIn', d.linkedin_url]].map(([l, v]) => v ? (
        <div key={l} style={{ display: 'flex', gap: 8 }}>
          <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', minWidth: 70 }}>{l}</span>
          <span style={{ fontSize: 12, color: 'var(--text-1)' }}>{v}</span>
        </div>
      ) : null)}
      {d.followers != null && <div style={{ fontSize: 11, color: 'var(--text-3)' }}>{d.followers?.toLocaleString()} followers</div>}
    </div>
  )
}

function AiNluContact({ d }) {
  if (!d) return null
  const confColor = (d.email_confidence || 0) > 0.7 ? '#10b981' : (d.email_confidence || 0) > 0.4 ? '#f59e0b' : '#6b7280'
  const riskColor = { low: '#10b981', medium: '#f59e0b', high: '#ef4444' }[d.bounce_risk] || '#6b7280'
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {d.work_email && (
        <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(16,185,129,0.08)', border: '1px solid rgba(16,185,129,0.2)' }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#6ee7b7', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Work Email</div>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-1)', fontFamily: 'monospace' }}>{d.work_email}</div>
          <div style={{ display: 'flex', gap: 8, marginTop: 5 }}>
            <span style={{ fontSize: 11, color: confColor }}>Confidence: {Math.round((d.email_confidence || 0) * 100)}%</span>
            <span style={{ fontSize: 11, color: riskColor }}>Bounce: {d.bounce_risk}</span>
          </div>
        </div>
      )}
      {d.phone && <div style={{ fontSize: 12, color: 'var(--text-2)' }}>📞 {d.phone}</div>}
      {Array.isArray(d.waterfall_steps) && d.waterfall_steps.length > 0 && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>How we inferred it</div>
          {d.waterfall_steps.map((s, i) => <div key={i} style={{ fontSize: 11, color: 'var(--text-2)', paddingLeft: 8, lineHeight: 1.7 }}>→ {s}</div>)}
        </div>
      )}
    </div>
  )
}

function AiNluScores({ d }) {
  if (!d) return null
  const icp = d.icp_score || 0
  const intent = d.intent_score || 0
  const icpColor = icp >= 70 ? '#10b981' : icp >= 40 ? '#f59e0b' : '#ef4444'
  const intentColor = intent >= 70 ? '#10b981' : intent >= 40 ? '#f59e0b' : '#ef4444'
  const breakdown = d.icp_breakdown || {}
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', gap: 16 }}>
        {[['ICP Score', icp, icpColor], ['Intent Score', intent, intentColor]].map(([label, val, color]) => (
          <div key={label} style={{ flex: 1, textAlign: 'center', padding: '12px 8px', borderRadius: 10, background: color + '12', border: `1px solid ${color}33` }}>
            <div style={{ fontSize: 28, fontWeight: 800, color, lineHeight: 1 }}>{val}</div>
            <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 3, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{label}</div>
          </div>
        ))}
      </div>
      {Object.keys(breakdown).length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>ICP Breakdown</div>
          {Object.entries(breakdown).map(([k, v]) => (
            <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: 11, color: 'var(--text-3)', minWidth: 120, textTransform: 'capitalize' }}>{k.replace(/_/g, ' ')}</span>
              <div style={{ flex: 1, height: 5, borderRadius: 3, background: 'var(--bg-card)' }}>
                <div style={{ width: `${v || 0}%`, height: '100%', borderRadius: 3, background: '#6366f1', transition: 'width 0.4s' }} />
              </div>
              <span style={{ fontSize: 11, color: '#a5b4fc', minWidth: 28, textAlign: 'right' }}>{v}</span>
            </div>
          ))}
        </div>
      )}
      {d.best_send_window && <div style={{ padding: '8px 12px', borderRadius: 7, background: 'rgba(245,158,11,0.08)', border: '1px solid rgba(245,158,11,0.2)', fontSize: 12, color: '#fcd34d' }}>⏰ {d.best_send_window}</div>}
    </div>
  )
}

function AiNluIcpMatch({ d }) {
  if (!d) return null
  const verdictColor = { 'Strong Match': '#10b981', 'Moderate Match': '#f59e0b', 'Weak Match': '#ef4444', 'No Match': '#6b7280' }[d.icp_verdict] || '#6b7280'
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {d.icp_verdict && <span style={{ alignSelf: 'flex-start', padding: '4px 14px', borderRadius: 20, fontSize: 12, fontWeight: 700, background: verdictColor + '22', border: `1px solid ${verdictColor}44`, color: verdictColor }}>{d.icp_verdict}</span>}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
        {d.is_decision_maker && <span style={{ padding: '3px 10px', borderRadius: 6, fontSize: 11, fontWeight: 600, background: 'rgba(16,185,129,0.12)', border: '1px solid rgba(16,185,129,0.3)', color: '#6ee7b7' }}>Decision Maker</span>}
        {d.geo_fit && <span style={{ padding: '3px 10px', borderRadius: 6, fontSize: 11, fontWeight: 600, background: 'rgba(99,102,241,0.12)', border: '1px solid rgba(99,102,241,0.3)', color: '#a5b4fc' }}>Geo Fit</span>}
        {d.industry && <span style={{ padding: '3px 10px', borderRadius: 6, fontSize: 11, background: 'var(--bg-card)', border: '1px solid var(--border-1)', color: 'var(--text-2)' }}>{d.industry}</span>}
        {d.company_size && <span style={{ padding: '3px 10px', borderRadius: 6, fontSize: 11, background: 'var(--bg-card)', border: '1px solid var(--border-1)', color: 'var(--text-2)' }}>{d.company_size} employees</span>}
      </div>
      {[['Education', d.education_level], ['Awards', d.awards_count != null ? `${d.awards_count} awards` : null]].map(([l, v]) => v ? (
        <div key={l} style={{ display: 'flex', gap: 8 }}>
          <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', minWidth: 70 }}>{l}</span>
          <span style={{ fontSize: 12, color: 'var(--text-1)' }}>{v}</span>
        </div>
      ) : null)}
    </div>
  )
}

function AiNluBehavioural({ d }) {
  if (!d) return null
  const TopicPills = ({ items, accent }) => {
    const list = Array.isArray(items) ? items.filter(Boolean) : []
    if (!list.length) return null
    return (
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
        {list.map((t, i) => (
          <span key={i} style={{
            padding: '3px 9px', borderRadius: 6, fontSize: 11,
            background: accent + '14', border: `1px solid ${accent}35`, color: accent,
          }}>{t}</span>
        ))}
      </div>
    )
  }
  const Block = ({ icon, label, labelColor, value, children }) => {
    if (!value && !children) return null
    return (
      <div style={{ padding: '9px 11px', borderRadius: 8, background: 'var(--bg-card)', border: '1px solid var(--border-1)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 5 }}>
          <span style={{ fontSize: 13 }}>{icon}</span>
          <span style={{ fontSize: 10, fontWeight: 700, color: labelColor || 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{label}</span>
        </div>
        {value && <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6 }}>{value}</div>}
        {children}
      </div>
    )
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>

      {/* Posts About */}
      {Array.isArray(d.posts_about) && d.posts_about.length > 0 && (
        <Block icon="✍️" label="Posts About" labelColor="#a5b4fc">
          <TopicPills items={d.posts_about} accent="#6366f1" />
        </Block>
      )}

      {/* Engages With */}
      {Array.isArray(d.engages_with) && d.engages_with.length > 0 && (
        <Block icon="👍" label="Engages With" labelColor="#34d399">
          <TopicPills items={d.engages_with} accent="#10b981" />
        </Block>
      )}

      {/* Style */}
      {d.engagement_style && d.engagement_style !== 'null' && (
        <Block icon="💬" label="Style" labelColor="#c4b5fd" value={d.engagement_style} />
      )}

      {/* Decision Pattern */}
      {d.decision_pattern && d.decision_pattern !== 'null' && (
        <Block icon="🧠" label="Decision Pattern" labelColor="#93c5fd" value={d.decision_pattern} />
      )}

      {/* Pain Point Hints */}
      {Array.isArray(d.pain_points) && d.pain_points.length > 0 && (
        <Block icon="⚡" label="Pain Point Hints" labelColor="#f87171">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {d.pain_points.map((p, i) => (
              <div key={i} style={{ display: 'flex', gap: 7, alignItems: 'flex-start' }}>
                <span style={{ color: '#f87171', flexShrink: 0, fontSize: 10, marginTop: 2 }}>▸</span>
                <span style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.55 }}>{p}</span>
              </div>
            ))}
          </div>
        </Block>
      )}

      {/* Warm Signal */}
      {d.warm_signal && d.warm_signal !== 'null' && (
        <div style={{ padding: '9px 11px', borderRadius: 8, background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.25)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 5 }}>
            <span style={{ fontSize: 13 }}>🔥</span>
            <span style={{ fontSize: 10, fontWeight: 700, color: '#f87171', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Warm Signal</span>
          </div>
          <div style={{ fontSize: 12, color: '#fca5a5', lineHeight: 1.6 }}>{d.warm_signal}</div>
        </div>
      )}

    </div>
  )
}

function AiNluPitch({ d }) {
  if (!d) return null
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {d.pain_point && <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)' }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#f87171', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>⚡ Pain Point</div>
        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6 }}>{d.pain_point}</div>
      </div>}
      {d.value_prop && <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(16,185,129,0.08)', border: '1px solid rgba(16,185,129,0.2)' }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#6ee7b7', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>✓ Value Prop</div>
        <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6 }}>{d.value_prop}</div>
      </div>}
      {d.best_angle && <span style={{ alignSelf: 'flex-start', padding: '3px 10px', borderRadius: 6, fontSize: 11, fontWeight: 600, background: 'rgba(139,92,246,0.15)', border: '1px solid rgba(139,92,246,0.3)', color: '#c4b5fd' }}>Angle: {d.best_angle}</span>}
      {d.cta && <div style={{ padding: '10px 12px', borderRadius: 8, background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)', fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6, fontStyle: 'italic' }}>"{d.cta}"</div>}
      {Array.isArray(d.do_not_pitch) && d.do_not_pitch.length > 0 && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#f87171', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>🚫 Do Not Pitch</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
            {d.do_not_pitch.map((p, i) => <span key={i} style={{ padding: '2px 8px', borderRadius: 5, fontSize: 11, background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.2)', color: '#fca5a5' }}>{p}</span>)}
          </div>
        </div>
      )}
    </div>
  )
}

function AiNluActivity({ d }) {
  if (!d) return null
  const acts = Array.isArray(d.recent_activity) ? d.recent_activity : []
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        {d.authored_posts_count != null && <span style={{ padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 600, background: 'rgba(20,184,166,0.12)', border: '1px solid rgba(20,184,166,0.3)', color: '#5eead4' }}>{d.authored_posts_count} posts</span>}
        {d.engagement_frequency && <span style={{ padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 600, background: 'var(--bg-card)', border: '1px solid var(--border-1)', color: 'var(--text-2)' }}>{d.engagement_frequency}</span>}
      </div>
      {acts.map((a, i) => (
        <div key={i} style={{ padding: '8px 10px', borderRadius: 7, background: a.warm_signal ? 'rgba(16,185,129,0.06)' : 'var(--bg-card)', border: `1px solid ${a.warm_signal ? 'rgba(16,185,129,0.3)' : 'var(--border-1)'}` }}>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginBottom: a.signal_reason ? 4 : 0 }}>
            <span style={{ fontSize: 10, padding: '1px 6px', borderRadius: 4, background: 'var(--bg-elevated)', color: 'var(--text-3)', fontWeight: 600 }}>{a.type}</span>
            {a.warm_signal && <span style={{ fontSize: 10, color: '#10b981', fontWeight: 700 }}>🔥 warm signal</span>}
            <span style={{ fontSize: 12, color: 'var(--text-1)', flex: 1 }}>{a.title}</span>
          </div>
          {a.signal_reason && <div style={{ fontSize: 11, color: 'var(--text-3)', paddingLeft: 2 }}>{a.signal_reason}</div>}
        </div>
      ))}
    </div>
  )
}

function AiNluTags({ d }) {
  if (!d) return null
  const tags = d.tags || []
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: '4px 0' }}>
      {tags.map((t, i) => <span key={i} style={{ padding: '4px 11px', borderRadius: 20, fontSize: 11, fontWeight: 600, background: 'rgba(167,139,250,0.12)', border: '1px solid rgba(167,139,250,0.3)', color: '#c4b5fd' }}>{t}</span>)}
    </div>
  )
}

function CopyBtn({ text }) {
  const [copied, setCopied] = useState(false)
  return (
    <button onClick={() => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 2000) }}
      style={{ padding: '3px 9px', borderRadius: 5, fontSize: 10, fontWeight: 600, border: '1px solid var(--border-1)', background: copied ? 'rgba(16,185,129,0.1)' : 'transparent', color: copied ? '#10b981' : 'var(--text-3)', cursor: 'pointer', transition: 'all 0.15s', flexShrink: 0 }}>
      {copied ? '✓ Copied' : 'Copy'}
    </button>
  )
}

function OutreachBlock({ label, icon, color, text }) {
  if (!text) return null
  return (
    <div style={{ borderRadius: 8, border: `1px solid ${color}30`, overflow: 'hidden' }}>
      <div style={{ padding: '5px 10px', background: `${color}0a`, borderBottom: `1px solid ${color}20`, display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{ fontSize: 11 }}>{icon}</span>
        <span style={{ fontSize: 9, fontWeight: 700, color, textTransform: 'uppercase', letterSpacing: '0.05em', flex: 1 }}>{label}</span>
        <CopyBtn text={text} />
      </div>
      <div style={{ padding: '8px 10px', fontSize: 12, lineHeight: 1.75, color: 'var(--text-1)', whiteSpace: 'pre-wrap' }}>{text}</div>
    </div>
  )
}

function AiNluOutreach({ d }) {
  const [emailView, setEmailView] = useState('full')
  if (!d) return null
  const email    = d.cold_email || {}
  const followUp = d.follow_up  || {}
  const fullEmailText = email.full_email ||
    [email.greeting, email.opening, email.body, email.cta, email.sign_off].filter(Boolean).join('\n\n')

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>

      {/* LinkedIn note */}
      {d.linkedin_note && (
        <div style={{ borderRadius: 9, border: '1px solid rgba(10,102,194,0.3)', overflow: 'hidden' }}>
          <div style={{ padding: '7px 12px', background: 'rgba(10,102,194,0.06)', borderBottom: '1px solid rgba(10,102,194,0.2)', display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ fontSize: 11, fontWeight: 800, color: '#0a66c2', fontFamily: 'serif' }}>in</span>
            <span style={{ fontSize: 10, fontWeight: 700, color: '#5ba4cf', textTransform: 'uppercase', letterSpacing: '0.05em', flex: 1 }}>LinkedIn Connection Note</span>
            <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{d.linkedin_note.split(/\s+/).length} words</span>
            <CopyBtn text={d.linkedin_note} />
          </div>
          <div style={{ padding: '12px', fontSize: 12, lineHeight: 1.8, color: 'var(--text-1)', fontStyle: 'italic' }}>"{d.linkedin_note}"</div>
        </div>
      )}

      {/* Cold email */}
      {(email.subject || fullEmailText) && (
        <div style={{ borderRadius: 9, border: '1px solid rgba(99,102,241,0.3)', overflow: 'hidden' }}>
          <div style={{ padding: '8px 12px', background: 'rgba(99,102,241,0.06)', borderBottom: '1px solid rgba(99,102,241,0.2)', display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <Mail size={12} color="#6366f1" />
            <span style={{ fontSize: 10, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Cold Email</span>
            <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
              {['full', 'parts'].map(v => (
                <button key={v} onClick={() => setEmailView(v)}
                  style={{ padding: '2px 8px', borderRadius: 4, fontSize: 10, fontWeight: 600, cursor: 'pointer', border: '1px solid var(--border-1)', background: emailView === v ? 'rgba(99,102,241,0.15)' : 'transparent', color: emailView === v ? '#a5b4fc' : 'var(--text-3)' }}>
                  {v === 'full' ? 'Full Email' : 'Breakdown'}
                </button>
              ))}
            </div>
          </div>

          {/* Subject */}
          {email.subject && (
            <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-1)', display: 'flex', alignItems: 'center', gap: 8, background: 'var(--bg-base)' }}>
              <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', width: 48, flexShrink: 0 }}>Subject</span>
              <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-1)', flex: 1 }}>{email.subject}</span>
              <CopyBtn text={email.subject} />
            </div>
          )}

          {/* Full email */}
          {emailView === 'full' && fullEmailText && (
            <div>
              <div style={{ padding: '16px 16px 10px', fontSize: 13, lineHeight: 1.9, color: 'var(--text-1)', whiteSpace: 'pre-wrap', fontFamily: 'inherit' }}>
                {fullEmailText}
              </div>
              <div style={{ padding: '6px 12px 10px', display: 'flex', justifyContent: 'flex-end', gap: 6 }}>
                <CopyBtn text={`Subject: ${email.subject || ''}\n\n${fullEmailText}`} />
              </div>
            </div>
          )}

          {/* Parts breakdown */}
          {emailView === 'parts' && (
            <div style={{ padding: '12px', display: 'flex', flexDirection: 'column', gap: 8 }}>
              <OutreachBlock label="Greeting"     icon="👋" color="#6366f1" text={email.greeting} />
              <OutreachBlock label="Opening Hook" icon="🎯" color="#f59e0b" text={email.opening} />
              <OutreachBlock label="Body"         icon="📝" color="#6366f1" text={email.body} />
              <OutreachBlock label="CTA"          icon="💬" color="#10b981" text={email.cta} />
              <OutreachBlock label="Sign Off"     icon="✍️" color="#6b7280" text={email.sign_off} />
            </div>
          )}
        </div>
      )}

      {/* Follow-up sequence */}
      {(followUp.day3 || followUp.day7) && (
        <div style={{ borderRadius: 9, border: '1px solid rgba(245,158,11,0.25)', overflow: 'hidden' }}>
          <div style={{ padding: '7px 12px', background: 'rgba(245,158,11,0.06)', borderBottom: '1px solid rgba(245,158,11,0.2)' }}>
            <span style={{ fontSize: 10, fontWeight: 700, color: '#fbbf24', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Follow-up Sequence</span>
          </div>
          <div style={{ padding: '10px 12px', display: 'flex', flexDirection: 'column', gap: 8 }}>
            {followUp.day3 && (
              <div>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#f59e0b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>Day 3 — if no reply</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.7, padding: '8px 10px', background: 'var(--bg-base)', borderRadius: 7, border: '1px solid var(--border-1)', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 }}>
                  <span>{followUp.day3}</span><CopyBtn text={followUp.day3} />
                </div>
              </div>
            )}
            {followUp.day7 && (
              <div>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#f59e0b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>Day 7 — final bump</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.7, padding: '8px 10px', background: 'var(--bg-base)', borderRadius: 7, border: '1px solid var(--border-1)', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 }}>
                  <span>{followUp.day7}</span><CopyBtn text={followUp.day7} />
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function AiNluPersona({ d }) {
  if (!d) return null
  const crm      = d.crm_score || {}
  const psych    = d.psychographic_profile || {}
  const outIntel = d.outreach_intelligence || {}
  const tierColor = { A: '#10b981', B: '#6366f1', C: '#f59e0b', D: '#ef4444' }[crm.tier] || '#6b7280'
  const personalityColor = {
    Driver: '#ef4444', Analytical: '#3b82f6', Amiable: '#10b981', Expressive: '#f59e0b',
  }[psych.personality] || '#8b5cf6'

  const PillList = ({ items, color, bg, border }) => {
    const list = Array.isArray(items) ? items.filter(Boolean) : []
    if (!list.length) return null
    return (
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
        {list.map((s, i) => (
          <span key={i} style={{
            padding: '2px 9px', borderRadius: 20, fontSize: 11, fontWeight: 500,
            background: bg, border: `1px solid ${border}`, color,
          }}>{s}</span>
        ))}
      </div>
    )
  }

  const Section = ({ label, color = '#8b5cf6', children }) => (
    <div>
      <div style={{ fontSize: 10, fontWeight: 700, color, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>{label}</div>
      {children}
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      {/* CRM Score + Tier */}
      {(crm.tier || crm.score) && (
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', padding: '10px 14px', borderRadius: 10, background: tierColor + '10', border: `1.5px solid ${tierColor}35` }}>
          <div style={{
            width: 52, height: 52, borderRadius: 12, flexShrink: 0,
            background: tierColor + '20', border: `2px solid ${tierColor}60`,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <span style={{ fontSize: 24, fontWeight: 900, color: tierColor }}>{crm.tier || '—'}</span>
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
              <span style={{ fontSize: 22, fontWeight: 800, color: tierColor }}>{crm.score ?? '—'}</span>
              <span style={{ fontSize: 12, color: 'var(--text-3)' }}>/100 CRM Score</span>
            </div>
            {crm.reason && <div style={{ fontSize: 11, color: 'var(--text-2)', marginTop: 2, lineHeight: 1.4 }}>{crm.reason}</div>}
          </div>
        </div>
      )}

      {/* Identity Summary */}
      {d.identity_summary && (
        <Section label="Identity Summary" color="#8b5cf6">
          <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.7 }}>{d.identity_summary}</div>
        </Section>
      )}

      {/* Psychographic Profile */}
      {(psych.personality || psych.motivation || psych.risk_tolerance) && (
        <Section label="Psychographic Profile" color={personalityColor}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {psych.personality && (
                <span style={{
                  padding: '4px 12px', borderRadius: 20, fontSize: 12, fontWeight: 700,
                  background: personalityColor + '18', border: `1.5px solid ${personalityColor}50`, color: personalityColor,
                }}>
                  {psych.personality} Personality
                </span>
              )}
              {psych.risk_tolerance && (
                <span style={{
                  padding: '4px 12px', borderRadius: 20, fontSize: 11, fontWeight: 600,
                  background: 'var(--bg-base)', border: '1px solid var(--border-1)', color: 'var(--text-2)',
                }}>
                  Risk: {psych.risk_tolerance}
                </span>
              )}
            </div>
            {psych.motivation && (
              <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.55, padding: '6px 10px', borderRadius: 7, background: personalityColor + '08', border: `1px solid ${personalityColor}20` }}>
                <span style={{ fontWeight: 600, color: personalityColor }}>Motivation: </span>{psych.motivation}
              </div>
            )}
          </div>
        </Section>
      )}

      {/* Behavior Analysis */}
      {d.behavior_analysis && (
        <Section label="Behaviour Analysis" color="#ec4899">
          <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.65, padding: '8px 10px', borderRadius: 7, background: 'rgba(236,72,153,0.06)', border: '1px solid rgba(236,72,153,0.18)' }}>
            {d.behavior_analysis}
          </div>
        </Section>
      )}

      {/* Writing Style */}
      {d.writing_style && (
        <Section label="Writing Style" color="#14b8a6">
          <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6 }}>{d.writing_style}</div>
        </Section>
      )}

      {/* Outreach Intelligence */}
      {(outIntel.best_channel || outIntel.best_time || outIntel.tone) && (
        <Section label="Outreach Intelligence" color="#6366f1">
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8 }}>
            {outIntel.best_channel && (
              <div style={{ padding: '8px 10px', borderRadius: 8, background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)', textAlign: 'center' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Channel</div>
                <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)' }}>{outIntel.best_channel}</div>
              </div>
            )}
            {outIntel.best_time && (
              <div style={{ padding: '8px 10px', borderRadius: 8, background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)', textAlign: 'center' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Best Time</div>
                <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)' }}>{outIntel.best_time}</div>
              </div>
            )}
            {outIntel.tone && (
              <div style={{ padding: '8px 10px', borderRadius: 8, background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)', textAlign: 'center' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Tone</div>
                <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)' }}>{outIntel.tone}</div>
              </div>
            )}
          </div>
        </Section>
      )}

      {/* Buying Signals */}
      {Array.isArray(d.buying_signals) && d.buying_signals.length > 0 && (
        <Section label="Buying Signals" color="#10b981">
          <PillList items={d.buying_signals} color="#6ee7b7" bg="rgba(16,185,129,0.1)" border="rgba(16,185,129,0.25)" />
        </Section>
      )}

      {/* Interests */}
      {Array.isArray(d.interests) && d.interests.length > 0 && (
        <Section label="Interests" color="#f59e0b">
          <PillList items={d.interests} color="#fcd34d" bg="rgba(245,158,11,0.1)" border="rgba(245,158,11,0.25)" />
        </Section>
      )}

      {/* Skills */}
      {Array.isArray(d.skills) && d.skills.length > 0 && (
        <Section label="Skills" color="#3b82f6">
          <PillList items={d.skills} color="#93c5fd" bg="rgba(59,130,246,0.1)" border="rgba(59,130,246,0.25)" />
        </Section>
      )}

      {/* Smart Tags */}
      {Array.isArray(d.smart_tags) && d.smart_tags.length > 0 && (
        <Section label="Smart Tags" color="#a855f7">
          <PillList items={d.smart_tags} color="#d8b4fe" bg="rgba(168,85,247,0.1)" border="rgba(168,85,247,0.25)" />
        </Section>
      )}

    </div>
  )
}

const AI_NLU_MAP = {
  identity:            AiNluIdentity,
  contact:             AiNluContact,
  scores:              AiNluScores,
  icp_match:           AiNluIcpMatch,
  behavioural_signals: AiNluBehavioural,
  pitch_intelligence:  AiNluPitch,
  activity:            AiNluActivity,
  tags:                AiNluTags,
  outreach:            AiNluOutreach,
  persona_analysis:    AiNluPersona,
}

// ── AiEnrichmentSection ───────────────────────────────────────────────────────
function AiEnrichmentSection({ lead }) {
  const [results,    setResults]    = useState({})   // { moduleId: parsedObject }
  const [statuses,   setStatuses]   = useState({})   // { moduleId: 'idle'|'running'|'done'|'error' }
  const [errors,     setErrors]     = useState({})   // { moduleId: msg }
  const [open,       setOpen]       = useState(new Set())
  const [viewMode,   setViewMode]   = useState({})   // { moduleId: 'nlu'|'json' }
  const [runningAll, setRunningAll] = useState(false)
  const [rawProfile, setRawProfile] = useState(null)

  // Fetch full lead (includes raw_profile) when lead changes
  useEffect(() => {
    if (!lead?.id) return
    setResults({}); setStatuses({}); setErrors({}); setRawProfile(null)
    fetch(`${BACKEND}/leads/${lead.id}`, { headers: jsonHdr() })
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.raw_profile) setRawProfile(d.raw_profile) })
      .catch(() => {})
  }, [lead?.id])

  const getProfile = () => {
    if (!rawProfile) return null
    const p = typeof rawProfile === 'string' ? JSON.parse(rawProfile) : { ...rawProfile }
    delete p._activity_full
    return p
  }

  const runModule = async (moduleId) => {
    const profile = getProfile()
    if (!profile) { toast.error('Profile data not loaded yet'); return null }
    const mod = AI_MODULES.find(m => m.id === moduleId)
    setStatuses(p => ({ ...p, [moduleId]: 'running' }))
    setErrors(p => ({ ...p, [moduleId]: null }))
    setOpen(p => new Set([...p, moduleId]))
    setViewMode(p => ({ ...p, [moduleId]: 'nlu' }))
    try {
      const r = await fetch(`${BACKEND}/v1/ai/${mod.route}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile }),
      })
      if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`) }
      const data = await r.json()
      setResults(p => ({ ...p, [moduleId]: data }))
      setStatuses(p => ({ ...p, [moduleId]: 'done' }))
      return data
    } catch (e) {
      setStatuses(p => ({ ...p, [moduleId]: 'error' }))
      setErrors(p => ({ ...p, [moduleId]: e.message }))
      return null
    }
  }

  const runAll = async () => {
    const profile = getProfile()
    if (!profile) { toast.error('Profile data not loaded yet'); return }
    setRunningAll(true)
    setResults({}); setStatuses({}); setErrors({})
    // Mark all as running
    const running = {}
    AI_MODULES.forEach(m => { running[m.id] = 'running' })
    setStatuses(running)
    setOpen(new Set(AI_MODULES.map(m => m.id)))
    try {
      const r = await fetch(`${BACKEND}/v1/ai/full-enrichment`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile }),
      })
      if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`) }
      const data = await r.json()
      const newStatuses = {}
      const newResults  = {}
      AI_MODULES.forEach(m => {
        if (data[m.id]) {
          newResults[m.id]  = data[m.id]
          newStatuses[m.id] = data[m.id].error ? 'error' : 'done'
        } else {
          newStatuses[m.id] = 'idle'
        }
      })
      setResults(newResults)
      setStatuses(newStatuses)
      setViewMode(Object.fromEntries(AI_MODULES.map(m => [m.id, 'nlu'])))
    } catch (e) {
      toast.error('Full enrichment failed: ' + e.message)
      const errStatuses = {}
      AI_MODULES.forEach(m => { errStatuses[m.id] = 'idle' })
      setStatuses(errStatuses)
    } finally {
      setRunningAll(false)
    }
  }

  const anyRunning = runningAll || Object.values(statuses).includes('running')
  const doneCount  = AI_MODULES.filter(m => statuses[m.id] === 'done').length

  const renderModule = (mod) => {
    const status  = statuses[mod.id] || 'idle'
    const result  = results[mod.id]
    const err     = errors[mod.id]
    const isOpen  = open.has(mod.id)
    const mode    = viewMode[mod.id] || 'nlu'
    const NluComp = AI_NLU_MAP[mod.id]
    const tu      = result?.token_usage

    const statusDot = {
      idle:    <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'var(--bg-card)', border: '1px solid var(--border-1)', display: 'inline-block' }} />,
      running: <span style={{ width: 7, height: 7, borderRadius: '50%', background: '#f59e0b', display: 'inline-block', animation: 'pulse 1s ease-in-out infinite' }} />,
      done:    <Check size={10} color={mod.color} strokeWidth={3} />,
      error:   <AlertCircle size={10} color="#ef4444" />,
    }[status]

    return (
      <div key={mod.id} style={{ borderRadius: 10, border: `1px solid ${status === 'done' ? mod.color + '33' : status === 'error' ? 'rgba(239,68,68,0.3)' : 'var(--border-1)'}`, overflow: 'hidden', transition: 'border-color 0.2s' }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '9px 12px', background: 'var(--bg-card)', cursor: 'pointer' }}
          onClick={() => setOpen(p => { const s = new Set(p); isOpen ? s.delete(mod.id) : s.add(mod.id); return s })}>
          <div style={{ width: 22, height: 22, borderRadius: 6, background: mod.color + '22', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, flexShrink: 0 }}>{mod.icon}</div>
          {statusDot}
          <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)', flex: 1 }}>{mod.name}</span>
          {status === 'done' && <span style={{ fontSize: 9, fontWeight: 700, color: mod.color, letterSpacing: '0.06em', textTransform: 'uppercase' }}>DONE</span>}
          {status === 'running' && <span style={{ fontSize: 9, color: '#f59e0b', fontWeight: 600 }}>Analyzing…</span>}
          {tu && <span style={{ fontSize: 9, color: 'var(--text-3)' }}>{tu.total_tokens}t</span>}
          {status !== 'running' && (
            <button onClick={e => { e.stopPropagation(); runModule(mod.id) }}
              style={{ padding: '3px 9px', borderRadius: 6, border: `1px solid ${mod.color}44`, background: 'transparent', color: mod.color, fontSize: 10, fontWeight: 600, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
              <RefreshCw size={9} /> {status === 'done' ? 'Re-run' : 'Run'}
            </button>
          )}
          <ChevronDown size={12} color="var(--text-3)" style={{ transform: isOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s', flexShrink: 0 }} />
        </div>

        {/* Body */}
        {isOpen && (
          <div style={{ borderTop: '1px solid var(--border-1)', padding: '12px 14px', background: 'var(--bg-elevated)' }}>
            {err && <div style={{ padding: '8px 10px', borderRadius: 7, background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)', fontSize: 12, color: '#fca5a5', marginBottom: 8 }}>{err}</div>}
            {status === 'running' && <div style={{ fontSize: 12, color: 'var(--text-3)', textAlign: 'center', padding: '16px 0' }}>Analyzing with Groq…</div>}
            {result && status !== 'running' && (
              <>
                {/* View toggle */}
                <div style={{ display: 'flex', gap: 4, marginBottom: 10 }}>
                  {['nlu', 'json'].map(v => (
                    <button key={v} onClick={() => setViewMode(p => ({ ...p, [mod.id]: v }))}
                      style={{ padding: '3px 10px', borderRadius: 6, border: `1px solid ${mode === v ? mod.color : 'var(--border-1)'}`, background: mode === v ? mod.color + '18' : 'transparent', color: mode === v ? mod.color : 'var(--text-3)', fontSize: 10, fontWeight: 700, cursor: 'pointer', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                      {v === 'nlu' ? '✦ NLU' : '{ } JSON'}
                    </button>
                  ))}
                  {tu && <span style={{ marginLeft: 'auto', fontSize: 10, color: 'var(--text-3)', alignSelf: 'center' }}>
                    {tu.prompt_tokens}+{tu.completion_tokens} tokens · {tu.model}
                  </span>}
                </div>
                {mode === 'nlu'
                  ? <NluComp d={result} />
                  : <pre style={{ margin: 0, fontSize: 11, lineHeight: 1.6, color: 'var(--text-1)', whiteSpace: 'pre-wrap', wordBreak: 'break-all', fontFamily: "'JetBrains Mono','Fira Code',monospace", maxHeight: 400, overflowY: 'auto' }}>
                      {JSON.stringify(result, null, 2)}
                    </pre>
                }
              </>
            )}
            {!result && status === 'idle' && (
              <div style={{ textAlign: 'center', padding: '16px 0', fontSize: 12, color: 'var(--text-3)' }}>Click Run to analyse</div>
            )}
          </div>
        )}
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', borderRadius: 10, background: 'var(--bg-card)', border: '1px solid var(--border-1)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Sparkles size={14} color="#6366f1" />
          <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-1)' }}>AI Enrichment</span>
          <span style={{ fontSize: 10, color: 'var(--text-3)' }}>· 10 modules · Groq</span>
          {doneCount > 0 && <span style={{ fontSize: 10, fontWeight: 700, color: '#10b981' }}>{doneCount}/{AI_MODULES.length} done</span>}
        </div>
        <button onClick={runAll} disabled={anyRunning || !rawProfile}
          style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '6px 14px', borderRadius: 8, border: 'none', background: anyRunning ? 'rgba(99,102,241,0.3)' : 'rgba(99,102,241,0.85)', color: '#fff', fontSize: 12, fontWeight: 700, cursor: anyRunning || !rawProfile ? 'not-allowed' : 'pointer', opacity: !rawProfile ? 0.5 : 1 }}>
          {anyRunning ? <><Loader size={11} style={{ animation: 'spin 1s linear infinite' }} /> Running all…</> : <><Zap size={11} /> Run All Modules</>}
        </button>
      </div>

      {!rawProfile && <div style={{ textAlign: 'center', padding: '20px', fontSize: 12, color: 'var(--text-3)' }}>Loading profile data…</div>}

      {/* Core group */}
      {rawProfile && (
        <>
          <div style={{ fontSize: 9, fontWeight: 800, color: 'var(--text-3)', letterSpacing: '0.1em', textTransform: 'uppercase', paddingLeft: 2 }}>Core</div>
          {AI_MODULES.filter(m => m.group === 'core').map(renderModule)}

          <div style={{ fontSize: 9, fontWeight: 800, color: 'var(--text-3)', letterSpacing: '0.1em', textTransform: 'uppercase', paddingLeft: 2, marginTop: 4 }}>Intelligence</div>
          {AI_MODULES.filter(m => m.group === 'intel').map(renderModule)}

          <div style={{ fontSize: 9, fontWeight: 800, color: 'var(--text-3)', letterSpacing: '0.1em', textTransform: 'uppercase', paddingLeft: 2, marginTop: 4 }}>Output</div>
          {AI_MODULES.filter(m => m.group === 'output').map(renderModule)}
        </>
      )}
    </div>
  )
}

// ── Old LIO section kept for reference (not used) ─────────────────────────────
function IdentitySection({ lead, full }) {
  const id = full.person_profile || full.identity || {}
  const bannerImg          = full.banner_image || ''
  const recommendations    = full.recommendations || []
  const recsCount          = full.recommendations_count || recommendations.length
  const similarProfiles    = full.similar_profiles || []
  const activityPhones     = full.activity_phones  || []
  const linkedinNumId      = full.linkedin_num_id  || ''
  const isInfluencer       = full.influencer || false
  const isMemorialized     = full.memorialized_account || false
  const bioLinks           = full.bio_links || []
  const bdScrapeTime       = full.bd_scrape_timestamp || ''
  const [showAllProfiles, setShowAllProfiles] = useState(false)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      {/* Memorialized account warning */}
      {isMemorialized && (
        <div style={{ padding: '8px 14px', borderRadius: 9, background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.25)', display: 'flex', alignItems: 'center', gap: 8 }}>
          <AlertCircle size={13} color="#ef4444" />
          <span style={{ fontSize: 11, color: '#ef4444', fontWeight: 600 }}>This is a memorialized LinkedIn account</span>
        </div>
      )}

      {/* Banner + Avatar hero */}
      {(lead.avatar_url || bannerImg) && (
        <div style={{ position: 'relative', borderRadius: 10, overflow: 'hidden', border: '1px solid var(--border-1)' }}>
          {bannerImg && (
            <div style={{ height: 80, overflow: 'hidden' }}>
              <img src={bannerImg} alt="" referrerPolicy="no-referrer"
                style={{ width: '100%', objectFit: 'cover', display: 'block' }}
                onError={e => { e.target.style.display = 'none' }} />
            </div>
          )}
          <div style={{ padding: bannerImg ? '8px 14px 12px' : '12px 14px', display: 'flex', alignItems: 'center', gap: 14 }}>
            <Avatar name={lead.name} tier={lead.score_tier} src={lead.avatar_url} size={72} />
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
                <span style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-1)' }}>{lead.name}</span>
                {isInfluencer && (
                  <span style={{ fontSize: 9, padding: '1px 7px', borderRadius: 10, fontWeight: 700,
                    background: 'rgba(251,191,36,0.15)', color: '#f59e0b', border: '1px solid rgba(251,191,36,0.35)' }}>
                    INFLUENCER
                  </span>
                )}
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-3)', marginTop: 3 }}>{lead.title}</div>
              <div style={{ display: 'flex', gap: 12, marginTop: 4, fontSize: 11, color: 'var(--text-3)' }}>
                {lead.followers > 0 && <span>{lead.followers.toLocaleString()} followers</span>}
                {lead.connections > 0 && <span>{lead.connections.toLocaleString()} connections</span>}
                {recsCount > 0 && <span>{recsCount} recommendations</span>}
              </div>
            </div>
          </div>
        </div>
      )}

      <Grid>
        <Field icon={<User size={12} />}     label="Full Name"      value={lead.name} />
        <Field icon={<Mail size={12} />}     label="Work Email"     value={lead.work_email}
          badge={lead.email_confidence ? <ConfBadge c={lead.email_confidence} src={lead.email_source} /> : null}
          copyable />
        <Field icon={<Mail size={12} />}     label="Personal Email" value={id.personal_email} />
        <Field icon={<Phone size={12} />}    label="Direct Phone"   value={lead.direct_phone || id.direct_phone} copyable />
        <Field icon={<Linkedin size={12} />} label="LinkedIn URL"   value={lead.linkedin_url}
          link={lead.linkedin_url} copyable />
        {linkedinNumId && <Field icon={<Linkedin size={12} />} label="LinkedIn Member ID" value={linkedinNumId} copyable />}
        <Field icon={<Twitter size={12} />}  label="Twitter / X"    value={lead.twitter || id.twitter} />
        <Field icon={<MapPin size={12} />}   label="City"           value={fmtCity(lead.city || id.city)} />
        <Field icon={<Globe size={12} />}    label="Country"        value={fmtCountry(lead.country || id.country)} />
        <Field icon={<Clock size={12} />}    label="Time Zone"      value={lead.timezone || id.timezone} />
        {bdScrapeTime && <Field icon={<Clock size={12} />} label="BD Scraped At" value={new Date(bdScrapeTime).toLocaleString()} />}
      </Grid>

      {/* Bio links */}
      {bioLinks.length > 0 && (
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {bioLinks.map((link, idx) => {
            const href = typeof link === 'string' ? link : (link.url || link.link || '')
            const label = typeof link === 'string' ? link : (link.label || link.title || href)
            return (
              <a key={idx} href={href} target="_blank" rel="noreferrer"
                style={{ fontSize: 11, padding: '2px 9px', borderRadius: 8, fontWeight: 500,
                  background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
                  color: '#6366f1', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 4 }}>
                <Globe size={9} /> {label}
              </a>
            )
          })}
        </div>
      )}

      {/* Phones found in LinkedIn activity posts */}
      {activityPhones.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(99,102,241,0.05)', border: '1px solid rgba(99,102,241,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>
            Phone Numbers Found in LinkedIn Posts
          </div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {activityPhones.map(ph => (
              <span key={ph} style={{ fontSize: 11, padding: '2px 8px', borderRadius: 8, fontWeight: 600,
                background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', border: '1px solid rgba(99,102,241,0.3)',
                display: 'flex', alignItems: 'center', gap: 4 }}>
                <Phone size={9} /> {ph}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* About / bio */}
      {lead.about && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>About</div>
          <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.7 }}>{lead.about}</div>
        </div>
      )}

      {/* Recommendations */}
      {(recommendations.length > 0 || recsCount > 0) && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <span style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
              LinkedIn Recommendations
            </span>
            <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
              {recommendations.length > 0 && recsCount > recommendations.length
                ? `${recommendations.length} shown · ${recsCount} total`
                : `${recsCount || recommendations.length} total`}
            </span>
          </div>
          {recommendations.length > 0 ? recommendations.map((rec, idx) => {
            const text = typeof rec === 'string' ? rec : (rec.text || rec.description || '')
            const author = typeof rec === 'object' ? (rec.recommender_name || rec.author_name || '') : ''
            const headline = typeof rec === 'object' ? (rec.recommender_headline || rec.author_headline || '') : ''
            const url = typeof rec === 'object' ? (rec.recommender_url || '') : ''
            const date = typeof rec === 'object' ? (rec.date || '') : ''
            return (
              <div key={idx} style={{ paddingBottom: idx < recommendations.length - 1 ? 10 : 0,
                borderBottom: idx < recommendations.length - 1 ? '1px solid var(--border-1)' : 'none',
                marginBottom: idx < recommendations.length - 1 ? 10 : 0 }}>
                {(author || headline) && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                    <User size={11} color="#6366f1" />
                    <div>
                      {author && (
                        url
                          ? <a href={url} target="_blank" rel="noreferrer"
                              style={{ fontSize: 11, fontWeight: 700, color: '#6366f1', textDecoration: 'none' }}>
                              {author}
                            </a>
                          : <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-1)' }}>{author}</span>
                      )}
                      {headline && <span style={{ fontSize: 10, color: 'var(--text-3)', marginLeft: 5 }}>{headline}</span>}
                    </div>
                    {date && <span style={{ fontSize: 10, color: 'var(--text-3)', marginLeft: 'auto' }}>{date}</span>}
                  </div>
                )}
                {text && (
                  <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.6, fontStyle: 'italic' }}>
                    "{text}"
                  </div>
                )}
              </div>
            )
          }) : (
            <div style={{ fontSize: 11, color: 'var(--text-3)', fontStyle: 'italic' }}>
              {recsCount} recommendation{recsCount !== 1 ? 's' : ''} on LinkedIn (text not available in this dataset)
            </div>
          )}
        </div>
      )}

      {/* People also viewed */}
      {similarProfiles.length > 0 && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
              People Also Viewed ({similarProfiles.length})
            </div>
            {similarProfiles.length > 8 && (
              <button onClick={() => setShowAllProfiles(p => !p)} style={{
                fontSize: 10, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer', padding: 0,
              }}>{showAllProfiles ? 'Show less' : `Show all ${similarProfiles.length}`}</button>
            )}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
            {(showAllProfiles ? similarProfiles : similarProfiles.slice(0, 8)).map((sp, idx) => (
              <a key={idx} href={sp.link || sp.url || '#'} target="_blank" rel="noreferrer"
                style={{ padding: '7px 10px', borderRadius: 8, fontSize: 11,
                  background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
                  color: 'var(--text-1)', textDecoration: 'none',
                  display: 'flex', alignItems: 'center', gap: 8 }}>
                {sp.image
                  ? <img src={sp.image} alt="" referrerPolicy="no-referrer"
                      style={{ width: 28, height: 28, borderRadius: '50%', objectFit: 'cover', flexShrink: 0,
                        border: '1px solid var(--border-1)', background: 'var(--bg-base)' }}
                      onError={e => { e.target.style.display = 'none' }} />
                  : <div style={{ width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
                      background: 'rgba(99,102,241,0.12)', display: 'flex', alignItems: 'center',
                      justifyContent: 'center' }}>
                      <User size={12} color="#6366f1" />
                    </div>
                }
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 600, color: '#6366f1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {sp.name || 'Profile'}
                  </div>
                  {(sp.title || sp.about) && (
                    <div style={{ fontSize: 10, color: 'var(--text-3)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {sp.title || sp.about}
                    </div>
                  )}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2, flexShrink: 0 }}>
                  {sp.degree && (
                    <span style={{ fontSize: 9, padding: '1px 5px', borderRadius: 5,
                      background: 'rgba(99,102,241,0.1)', color: '#6366f1', fontWeight: 600 }}>
                      {sp.degree}
                    </span>
                  )}
                  {sp.location && <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{fmtCity(sp.location)}</span>}
                </div>
              </a>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function ProfessionalSection({ lead, full }) {
  const p = full.person_profile || full.professional || {}

  // Career history — prefer person_profile.career_history, fall back to professional
  const careerHistory = p.career_history || []

  // Education list — prefer structured list, fall back to DB string
  const eduList = (() => {
    const raw = p.education_list
    if (Array.isArray(raw) && raw.length) return raw
    // Fall back: DB education string → single entry
    const s = lead.education || p.education || ''
    return s ? [{ school: s, degree: '', years: '' }] : []
  })()

  // Skills — all of them
  const skills = parseArr(lead.top_skills || p.top_skills)

  // Certifications
  const certs = parseArr(lead.certifications || p.certifications)

  // Languages
  const langs = parseArr(lead.languages || p.languages)

  // Previous companies
  const prevCos = parseArr(lead.previous_companies || p.previous_companies)

  const SectionHead = ({ icon, title }) => (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8, marginTop: 4 }}>
      <span style={{ color: 'var(--accent)', opacity: 0.7 }}>{icon}</span>
      <span style={{ fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-3)' }}>
        {title}
      </span>
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* ── Basic role info ── */}
      <Grid>
        <Field icon={<Briefcase size={12} />} label="Current Job Title"     value={lead.title} />
        <Field icon={<Layers size={12} />}    label="Seniority Level"        value={lead.seniority_level || p.seniority_level} />
        <Field icon={<Building2 size={12} />} label="Department"             value={lead.department || p.department} />
        <Field icon={<Clock size={12} />}     label="Years in Current Role"  value={lead.years_in_role || p.years_in_role} />
      </Grid>

      {/* ── Career Timeline ── */}
      {careerHistory.length > 0 && (
        <div>
          <SectionHead icon={<Briefcase size={12} />} title="Career History" />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {careerHistory.map((job, i) => (
              <div key={i} style={{
                padding: '10px 14px',
                borderRadius: 8,
                background: 'var(--bg-card)',
                border: '1px solid var(--border-1)',
                display: 'flex', alignItems: 'flex-start', gap: 10,
              }}>
                <div style={{
                  width: 32, height: 32, borderRadius: 8, flexShrink: 0,
                  background: i === 0 ? 'rgba(99,102,241,0.12)' : 'var(--bg-base)',
                  border: '1px solid var(--border-1)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>
                  <Building2 size={14} color={i === 0 ? '#6366f1' : 'var(--text-3)'} />
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 2 }}>
                    {job.title || '—'}
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--text-2)' }}>{job.company || ''}</div>
                  {job.duration && (
                    <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
                      <Clock size={10} style={{ display: 'inline', marginRight: 3, verticalAlign: 'middle' }} />
                      {job.duration}
                    </div>
                  )}
                </div>
                {i === 0 && (
                  <span style={{
                    fontSize: 10, fontWeight: 600, padding: '2px 7px', borderRadius: 5,
                    background: 'rgba(99,102,241,0.12)', color: '#6366f1', flexShrink: 0,
                  }}>Current</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Previous Companies (when no career history) ── */}
      {careerHistory.length === 0 && prevCos.length > 0 && (
        <div>
          <SectionHead icon={<Building2 size={12} />} title="Previous Companies" />
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {prevCos.map((co, i) => (
              <span key={i} style={{
                fontSize: 12, padding: '4px 10px', borderRadius: 6,
                background: 'var(--bg-card)', border: '1px solid var(--border-1)',
                color: 'var(--text-2)',
              }}>{co}</span>
            ))}
          </div>
        </div>
      )}

      {/* ── Skills ── */}
      {skills.length > 0 && (
        <div>
          <SectionHead icon={<Award size={12} />} title={`Top Skills (${skills.length})`} />
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
            {skills.map((sk, i) => (
              <span key={i} style={{
                fontSize: 12, padding: '4px 10px', borderRadius: 6, fontWeight: 500,
                background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.22)',
                color: '#6366f1',
              }}>{sk}</span>
            ))}
          </div>
        </div>
      )}

      {/* ── Education ── */}
      {eduList.length > 0 && (
        <div>
          <SectionHead icon={<GraduationCap size={12} />} title={`Education (${eduList.length})`} />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {eduList.map((e, i) => (
              <div key={i} style={{
                padding: '10px 14px', borderRadius: 8,
                background: 'var(--bg-card)', border: '1px solid var(--border-1)',
                display: 'flex', gap: 10, alignItems: 'flex-start',
              }}>
                <div style={{
                  width: 30, height: 30, borderRadius: 7, flexShrink: 0,
                  background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.2)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>
                  <GraduationCap size={14} color="#10b981" />
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 1 }}>
                    {e.school || e.institution || '—'}
                  </div>
                  {(e.degree || e.field_of_study) && (
                    <div style={{ fontSize: 12, color: 'var(--text-2)' }}>
                      {[e.degree, e.field_of_study].filter(Boolean).join(' · ')}
                    </div>
                  )}
                  {e.years && (
                    <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>{e.years}</div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Certifications ── */}
      {certs.length > 0 && (
        <div>
          <SectionHead icon={<Award size={12} />} title={`Certifications (${certs.length})`} />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {certs.map((cert, i) => {
              const certName   = typeof cert === 'object' ? (cert.name  || cert.title || '') : cert
              const certIssuer = typeof cert === 'object' ? (cert.issuer || '') : ''
              const certDate   = typeof cert === 'object' ? (cert.date  || '') : ''
              const certUrl    = typeof cert === 'object' ? (cert.credential_url || '') : ''
              return (
                <div key={i} style={{
                  padding: '9px 12px', borderRadius: 8,
                  background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.2)',
                  display: 'flex', alignItems: 'flex-start', gap: 9,
                }}>
                  <Award size={13} color="#f59e0b" style={{ flexShrink: 0, marginTop: 1 }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 12, color: 'var(--text-1)', fontWeight: 600 }}>
                      {certUrl
                        ? <a href={certUrl} target="_blank" rel="noreferrer"
                            style={{ color: '#f59e0b', textDecoration: 'none' }}>{certName}</a>
                        : certName}
                    </div>
                    {certIssuer && (
                      <div style={{ fontSize: 11, color: 'var(--text-2)', marginTop: 1 }}>{certIssuer}</div>
                    )}
                    {certDate && (
                      <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 1 }}>{certDate}</div>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* ── Languages ── */}
      {langs.length > 0 && (
        <div>
          <SectionHead icon={<Languages size={12} />} title={`Languages (${langs.length})`} />
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {langs.map((lang, i) => {
              const langName  = typeof lang === 'object' ? (lang.name || lang.language || lang.title || String(lang)) : lang
              const langLevel = typeof lang === 'object' ? (lang.proficiency || lang.subtitle || lang.level || '') : ''
              return (
                <div key={i} style={{
                  padding: '6px 12px', borderRadius: 8, display: 'flex', flexDirection: 'column',
                  background: 'rgba(59,130,246,0.08)', border: '1px solid rgba(59,130,246,0.22)',
                  minWidth: 80,
                }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: '#3b82f6', display: 'flex', alignItems: 'center', gap: 4 }}>
                    <Languages size={11} />
                    {langName}
                  </div>
                  {langLevel && (
                    <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2 }}>{langLevel}</div>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* ── Empty state ── */}
      {careerHistory.length === 0 && skills.length === 0 && eduList.length === 0 && certs.length === 0 && langs.length === 0 && (
        <div style={{ textAlign: 'center', padding: '24px 0', color: 'var(--text-3)', fontSize: 13 }}>
          No professional data available for this lead.
        </div>
      )}
    </div>
  )
}

function CompanySection({ lead, full }) {
  const c = full.company_profile || full.company || {}
  const website = lead.company_website || c.website
  const desc = lead.company_description || c.company_description
  return (
    <div>
      {/* Company identity header */}
      {(lead.company_logo || lead.company) && (
        <div style={{
          display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14,
          padding: '12px 14px', borderRadius: 9,
          background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
        }}>
          <CompanyLogo src={lead.company_logo} name={lead.company} size={44} />
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)' }}>{lead.company}</div>
            {desc && <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 3, lineHeight: 1.5 }}>{desc}</div>}
            <div style={{ display: 'flex', gap: 10, marginTop: 5, flexWrap: 'wrap' }}>
              {website && (
                <a href={website.startsWith('http') ? website : `https://${website}`} target="_blank" rel="noreferrer"
                  style={{ fontSize: 11, color: '#6366f1', textDecoration: 'none', display: 'flex', gap: 3, alignItems: 'center' }}>
                  <Globe size={10} /> {website.replace(/^https?:\/\//, '')}
                </a>
              )}
              {lead.company_email && (
                <span style={{ fontSize: 11, color: '#10b981', display: 'flex', gap: 3, alignItems: 'center' }}>
                  <Mail size={10} /> {lead.company_email}
                </span>
              )}
              {lead.company_phone && (
                <span style={{ fontSize: 11, color: 'var(--text-2)', display: 'flex', gap: 3, alignItems: 'center' }}>
                  <Phone size={10} /> {lead.company_phone}
                </span>
              )}
              {lead.company_linkedin && (
                <a href={lead.company_linkedin} target="_blank" rel="noreferrer"
                  style={{ fontSize: 11, color: '#0077b5', textDecoration: 'none', display: 'flex', gap: 3, alignItems: 'center' }}>
                  <Linkedin size={10} /> LinkedIn
                </a>
              )}
              {lead.company_twitter && (
                <a href={lead.company_twitter} target="_blank" rel="noreferrer"
                  style={{ fontSize: 11, color: '#1da1f2', textDecoration: 'none', display: 'flex', gap: 3, alignItems: 'center' }}>
                  <Twitter size={10} /> Twitter
                </a>
              )}
            </div>
          </div>
        </div>
      )}
      <Grid>
        <Field icon={<Building2 size={12} />} label="Company Name"         value={lead.company} />
        <Field icon={<Globe size={12} />}     label="Website"              value={website} link={website} />
        <Field icon={<Mail size={12} />}      label="Company Email"        value={lead.company_email} copyable />
        <Field icon={<Phone size={12} />}     label="Company Phone"        value={lead.company_phone} copyable />
        <Field icon={<Layers size={12} />}    label="Industry"             value={lead.industry || c.industry} />
        <Field icon={<Users size={12} />}     label="Employee Count"       value={lead.employee_count ? lead.employee_count.toLocaleString() : (c.employee_count || '—')} />
        <Field icon={<MapPin size={12} />}    label="HQ Location"          value={lead.hq_location || c.hq_location} />
        <Field icon={<CalendarDays size={12} />} label="Founded Year"      value={lead.founded_year || c.founded_year} />
        <Field icon={<TrendingUp size={12} />} label="Funding Stage"       value={lead.funding_stage || c.funding_stage}
          badge={lead.funding_stage ? <FundingBadge stage={lead.funding_stage} /> : null} />
        <Field icon={<BarChart2 size={12} />}  label="Total Funding"       value={lead.total_funding || c.total_funding} />
        <Field icon={<CalendarDays size={12} />} label="Last Funding Date" value={lead.last_funding_date || c.last_funding_date} />
        <Field icon={<User size={12} />}       label="Lead Investor"       value={lead.lead_investor || c.lead_investor} />
        <Field icon={<BarChart2 size={12} />}  label="Annual Revenue (est)" value={lead.annual_revenue || c.annual_revenue_est} />
        <Field icon={<Code2 size={12} />}      label="Tech Stack"
          value={<TagList items={parseArr(lead.tech_stack || c.tech_stack)} color="#8b5cf6" />} />
        <Field icon={<Users size={12} />}      label="Hiring Velocity"     value={lead.hiring_velocity || c.hiring_velocity} />
      </Grid>
    </div>
  )
}

function WebsiteIntelSection({ lead, full }) {
  // Merge from multiple sources: flat DB fields + full_data.website_intelligence or website_intel
  const wi = full.website_intelligence || full.website_intel || {}
  const productOfferings = parseArr(lead.product_offerings || wi.product_offerings)
  const targetCustomers = parseArr(lead.target_customers || wi.target_customers)
  const useCases = parseArr(wi.use_cases)
  const keyMessaging = parseArr(wi.key_messaging)
  const integrations = parseArr(wi.integrations_mentioned)
  const techClues = parseArr(wi.tech_stack_clues)
  const openRoles = parseArr(wi.open_roles)
  const recentBlog = parseArr(wi.recent_blog_topics)

  const hasData = !!(lead.value_proposition || wi.company_description || lead.business_model || productOfferings.length)

  const BMODEL_COLORS = {
    'saas': '#6366f1', 'marketplace': '#f97316', 'agency': '#10b981',
    'hardware': '#3b82f6', 'service': '#8b5cf6',
  }
  const bmColor = BMODEL_COLORS[(lead.business_model || '').toLowerCase()] || '#6b7280'

  const HIRING_COLOR = { active: '#10b981', moderate: '#f97316', limited: '#6b7280' }
  const hiringStatus = wi.hiring_signals || ''
  const hiringColor = HIRING_COLOR[hiringStatus.toLowerCase()] || '#6b7280'

  if (!hasData) {
    return (
      <div style={{
        textAlign: 'center', padding: '40px 20px',
        border: '1px dashed var(--border-1)', borderRadius: 10,
        color: 'var(--text-3)',
      }}>
        <Globe size={28} style={{ opacity: 0.2, marginBottom: 8 }} />
        <div style={{ fontSize: 13, marginBottom: 4 }}>Website Intelligence not yet scraped</div>
        <div style={{ fontSize: 11 }}>Re-enrich this lead to trigger Stage 3 website scraping</div>
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Business identity row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(180px,1fr))', gap: 10 }}>
        {lead.business_model && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: `${bmColor}10`, border: `1px solid ${bmColor}30` }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: bmColor, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Business Model</div>
            <div style={{ fontSize: 14, fontWeight: 700, color: bmColor }}>{lead.business_model}</div>
          </div>
        )}
        {lead.product_category && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Product Category</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>{lead.product_category}</div>
          </div>
        )}
        {wi.market_positioning && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Market Position</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>{wi.market_positioning}</div>
          </div>
        )}
        {lead.pricing_signals && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(16,185,129,0.06)', border: '1px solid rgba(16,185,129,0.2)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: '#10b981', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Pricing Model</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: '#10b981' }}>{lead.pricing_signals}</div>
          </div>
        )}
        {hiringStatus && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: `${hiringColor}10`, border: `1px solid ${hiringColor}25` }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: hiringColor, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Hiring Status</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: hiringColor }}>{hiringStatus}</div>
          </div>
        )}
      </div>

      {/* Value proposition */}
      {(lead.value_proposition || wi.value_proposition) && (
        <div style={{ padding: '12px 14px', borderRadius: 9, background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>Value Proposition</div>
          <div style={{ fontSize: 13, color: 'var(--text-1)', lineHeight: 1.6, fontStyle: 'italic' }}>
            "{lead.value_proposition || wi.value_proposition}"
          </div>
        </div>
      )}

      {/* Problem solved */}
      {wi.problem_solved && (
        <div style={{ padding: '12px 14px', borderRadius: 9, background: 'rgba(239,68,68,0.05)', border: '1px solid rgba(239,68,68,0.15)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#ef4444', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>Problem They Solve</div>
          <div style={{ fontSize: 13, color: 'var(--text-1)', lineHeight: 1.6 }}>{wi.problem_solved}</div>
        </div>
      )}

      {/* Company description */}
      {wi.company_description && (
        <div style={{ padding: '12px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>Company Description</div>
          <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.7 }}>{wi.company_description}</div>
        </div>
      )}

      <Grid>
        {productOfferings.length > 0 && (
          <Field icon={<Layers size={12} />} label="Product Offerings"
            value={<TagList items={productOfferings} color="#6366f1" />} wide />
        )}
        {targetCustomers.length > 0 && (
          <Field icon={<Users size={12} />} label="Target Customers"
            value={<TagList items={targetCustomers} color="#f97316" />} wide />
        )}
        {useCases.length > 0 && (
          <Field icon={<Zap size={12} />} label="Use Cases"
            value={<TagList items={useCases} color="#10b981" />} wide />
        )}
        {keyMessaging.length > 0 && (
          <Field icon={<Megaphone size={12} />} label="Key Messaging Themes"
            value={<TagList items={keyMessaging} color="#8b5cf6" />} wide />
        )}
        {integrations.length > 0 && (
          <Field icon={<ArrowRight size={12} />} label="Integrations Mentioned"
            value={<TagList items={integrations} color="#3b82f6" />} wide />
        )}
        {techClues.length > 0 && (
          <Field icon={<Code2 size={12} />} label="Tech Stack Clues"
            value={<TagList items={techClues} color="#6b7280" />} wide />
        )}
        {openRoles.length > 0 && (
          <Field icon={<Users size={12} />} label="Open Roles"
            value={<TagList items={openRoles} color="#10b981" />} wide />
        )}
        {recentBlog.length > 0 && (
          <Field icon={<Newspaper size={12} />} label="Recent Blog Topics"
            value={<TagList items={recentBlog} color="#f59e0b" />} wide />
        )}
      </Grid>

      {/* Pages scraped */}
      {wi.pages_scraped && wi.pages_scraped.length > 0 && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 10, color: 'var(--text-3)' }}>Pages scraped:</span>
          {wi.pages_scraped.map(p => (
            <span key={p} style={{
              fontSize: 9, padding: '1px 7px', borderRadius: 10, fontWeight: 600,
              background: 'rgba(16,185,129,0.1)', color: '#10b981', border: '1px solid rgba(16,185,129,0.25)',
            }}>{p}</span>
          ))}
        </div>
      )}
    </div>
  )
}

function IntentSection({ lead, full }) {
  const i = full.intent_signals || {}
  const activityEmails  = full.activity_emails  || []
  const hiringSignals   = full.hiring_signals   || []
  const activityFull    = full.activity_full    || []
  const linkedinPosts   = full.linkedin_posts   || []
  const [showAllActivity, setShowAllActivity] = useState(false)
  const [showAllPosts, setShowAllPosts] = useState(false)

  const items = [
    { icon: <TrendingUp size={12} />, label: 'Recent Funding Event',  value: lead.recent_funding_event || i.recent_funding_event },
    { icon: <Users size={12} />,      label: 'Hiring Signal',         value: lead.hiring_signal || i.hiring_signal || (hiringSignals[0] || null) },
    { icon: <Briefcase size={12} />,  label: 'Job Change',            value: lead.job_change || i.job_change },
    { icon: <Linkedin size={12} />,   label: 'LinkedIn Activity',     value: lead.linkedin_activity || i.linkedin_activity },
    { icon: <Newspaper size={12} />,  label: 'News Mention',          value: lead.news_mention || i.news_mention },
    { icon: <Megaphone size={12} />,  label: 'Product Launch',        value: lead.product_launch || i.product_launch },
    { icon: <ShoppingCart size={12}/>, label: 'Competitor Usage',     value: lead.competitor_usage || i.competitor_usage },
    { icon: <MessageSquare size={12}/>,label: 'G2 / Review Activity', value: lead.review_activity || i.review_activity },
  ]
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Grid>
        {items.map(({ icon, label, value }) => (
          <Field key={label} icon={icon} label={label} value={value}
            badge={value ? <ActiveBadge /> : null} />
        ))}
      </Grid>

      {/* Activity emails — extracted from LinkedIn post text (high-confidence) */}
      {activityEmails.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(16,185,129,0.05)', border: '1px solid rgba(16,185,129,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#10b981', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>
            Emails Found in LinkedIn Activity Posts
          </div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {activityEmails.map(email => (
              <span key={email} style={{ fontSize: 11, padding: '2px 8px', borderRadius: 8, fontWeight: 600,
                background: 'rgba(16,185,129,0.1)', color: '#10b981', border: '1px solid rgba(16,185,129,0.3)',
                display: 'flex', alignItems: 'center', gap: 4 }}>
                <Mail size={9} /> {email}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Hiring signals — posts where person or company mentioned job openings */}
      {hiringSignals.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(249,115,22,0.05)', border: '1px solid rgba(249,115,22,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#f97316', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>
            Hiring Signals from LinkedIn Posts
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {hiringSignals.map((sig, idx) => (
              <div key={idx} style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.5,
                padding: '4px 0', borderBottom: idx < hiringSignals.length - 1 ? '1px solid var(--border-1)' : 'none' }}>
                {sig}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* LinkedIn Authored Posts (from BD "posts" field) */}
      {linkedinPosts.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <Linkedin size={11} color="#6366f1" />
              <span style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
                LinkedIn Posts ({linkedinPosts.length})
              </span>
            </div>
            {linkedinPosts.length > 4 && (
              <button onClick={() => setShowAllPosts(p => !p)} style={{
                fontSize: 10, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer', padding: 0,
              }}>{showAllPosts ? 'Show less' : `Show all ${linkedinPosts.length}`}</button>
            )}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {(showAllPosts ? linkedinPosts : linkedinPosts.slice(0, 4)).map((post, idx) => (
              <div key={idx} style={{
                padding: '8px 10px', borderRadius: 8,
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, marginBottom: 5 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 5, fontWeight: 600,
                      background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', border: '1px solid rgba(99,102,241,0.25)' }}>
                      Authored Post
                    </span>
                    {post.interaction && (
                      <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{post.interaction}</span>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
                    {post.created_at && (
                      <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
                        {new Date(post.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })}
                      </span>
                    )}
                    {post.link && (
                      <a href={post.link} target="_blank" rel="noreferrer"
                        style={{ fontSize: 9, color: '#6366f1', textDecoration: 'none' }}>
                        View ↗
                      </a>
                    )}
                  </div>
                </div>
                {post.title && (
                  <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-1)', lineHeight: 1.4, marginBottom: post.attribution ? 4 : 0 }}>
                    {post.title}
                  </div>
                )}
                {post.attribution && (
                  <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.6 }}>
                    {post.attribution}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Full LinkedIn Activity Feed (liked/commented/shared) */}
      {activityFull.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
              LinkedIn Activity Feed ({activityFull.length})
            </div>
            {activityFull.length > 5 && (
              <button onClick={() => setShowAllActivity(p => !p)} style={{
                fontSize: 10, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer', padding: 0,
              }}>{showAllActivity ? 'Show less' : `Show all ${activityFull.length}`}</button>
            )}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {(showAllActivity ? activityFull : activityFull.slice(0, 5)).map((item, idx) => (
              <div key={idx} style={{
                padding: '6px 8px', borderRadius: 7,
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, marginBottom: item.title || item.attribution ? 4 : 0 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ fontSize: 9, fontWeight: 600,
                      padding: '1px 6px', borderRadius: 6,
                      background: item.type === 'post' ? 'rgba(99,102,241,0.1)' : 'rgba(107,114,128,0.1)',
                      color: item.type === 'post' ? '#a5b4fc' : 'var(--text-3)',
                      border: `1px solid ${item.type === 'post' ? 'rgba(99,102,241,0.25)' : 'var(--border-1)'}`,
                    }}>
                      {item.interaction || 'Activity'}
                    </span>
                    {item.created_at && (
                      <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
                        {new Date(item.created_at).toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}
                      </span>
                    )}
                  </div>
                  {item.link && (
                    <a href={item.link} target="_blank" rel="noreferrer"
                      style={{ fontSize: 9, color: '#6366f1', textDecoration: 'none', flexShrink: 0 }}>
                      View ↗
                    </a>
                  )}
                </div>
                {item.title && (
                  <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.5, marginBottom: item.attribution ? 3 : 0 }}>
                    {item.title}
                  </div>
                )}
                {item.attribution && (
                  <div style={{ fontSize: 10, color: 'var(--text-3)', lineHeight: 1.5, fontStyle: 'italic' }}>
                    {item.attribution}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── AI Insights Section ───────────────────────────────────────────────────────
function AIInsightsSection({ lead }) {
  const tags = (() => { try { return JSON.parse(lead.auto_tags || '[]') } catch { return [] } })()
  const signals = (() => { try { return JSON.parse(lead.behavioural_signals || '{}') } catch { return {} } })()
  const pitch   = (() => { try { return JSON.parse(lead.pitch_intelligence  || '{}') } catch { return {} } })()
  const warm    = lead.warm_signal

  const TAG_COLORS = [
    '#6366f1','#8b5cf6','#ec4899','#f97316','#10b981','#3b82f6','#14b8a6','#f59e0b',
  ]

  const noData = !tags.length && !signals.posts_about && !pitch.top_pain_point
  if (noData) return (
    <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
      AI Insights not yet generated for this lead.<br />
      <span style={{ fontSize: 11 }}>Re-enrich to generate tags, behavioural signals and pitch intelligence.</span>
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* ── ICP Score summary bar ── */}
      {lead.total_score > 0 && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px',
          borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 10, color: 'var(--text-3)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 5 }}>ICP Score</div>
            <div style={{ height: 8, borderRadius: 4, background: 'var(--bg-base)', overflow: 'hidden' }}>
              <div style={{ height: '100%', borderRadius: 4, width: `${Math.min(lead.total_score, 100)}%`,
                background: lead.total_score >= 80 ? '#ef4444' : lead.total_score >= 55 ? '#f97316' : lead.total_score >= 30 ? '#3b82f6' : '#6b7280',
                transition: 'width 0.8s ease' }} />
            </div>
          </div>
          <div style={{ textAlign: 'right', flexShrink: 0 }}>
            <span style={{ fontSize: 22, fontWeight: 800, color: lead.total_score >= 80 ? '#ef4444' : lead.total_score >= 55 ? '#f97316' : '#6366f1' }}>
              {lead.total_score}
            </span>
            <span style={{ fontSize: 11, color: 'var(--text-3)' }}>/100</span>
            {lead.score_tier && (
              <div style={{ fontSize: 10, fontWeight: 700, color: lead.total_score >= 80 ? '#ef4444' : '#f97316',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                {lead.score_tier === 'hot' ? '🔥' : lead.score_tier === 'warm' ? '⚡' : '●'} {lead.score_tier}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── Auto Tags ── */}
      {tags.length > 0 && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
            letterSpacing: '0.07em', marginBottom: 8 }}>Auto-Generated Tags</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {tags.map((tag, i) => (
              <span key={i} style={{
                padding: '4px 10px', borderRadius: 20, fontSize: 11, fontWeight: 600,
                background: TAG_COLORS[i % TAG_COLORS.length] + '22',
                color: TAG_COLORS[i % TAG_COLORS.length],
                border: `1px solid ${TAG_COLORS[i % TAG_COLORS.length]}44`,
              }}>{tag}</span>
            ))}
          </div>
        </div>
      )}

      {/* ── Warm Signal ── */}
      {warm && (
        <div style={{ padding: '10px 14px', borderRadius: 9,
          background: '#fef9c322', border: '1px solid #fde04788',
          display: 'flex', alignItems: 'flex-start', gap: 8 }}>
          <span style={{ fontSize: 16 }}>⚡</span>
          <div>
            <div style={{ fontSize: 10, fontWeight: 700, color: '#d97706', textTransform: 'uppercase',
              letterSpacing: '0.06em', marginBottom: 2 }}>Warm Signal</div>
            <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{warm}</div>
          </div>
        </div>
      )}

      {/* ── Behavioural Signals ── */}
      {signals.posts_about && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
            letterSpacing: '0.07em', marginBottom: 8 }}>Behavioural Signals</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {[
              { key: 'posts_about',        label: 'POSTS ABOUT',      color: '#6366f1' },
              { key: 'engages_with',       label: 'ENGAGES WITH',     color: '#10b981' },
              { key: 'communication_style',label: 'STYLE',            color: '#3b82f6' },
              { key: 'decision_pattern',   label: 'DECISIONS',        color: '#8b5cf6' },
              { key: 'pain_point_hint',    label: 'PAIN POINT',       color: '#f97316' },
            ].filter(({ key }) => signals[key]).map(({ key, label, color }) => (
              <div key={key} style={{ display: 'flex', gap: 10, padding: '8px 12px', borderRadius: 8,
                background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', alignItems: 'flex-start' }}>
                <div style={{ minWidth: 90, fontSize: 9, fontWeight: 700, color, textTransform: 'uppercase',
                  letterSpacing: '0.07em', paddingTop: 1 }}>{label}</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)', flex: 1 }}>{signals[key]}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Pitch Intelligence ── */}
      {pitch.top_pain_point && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
            letterSpacing: '0.07em', marginBottom: 8 }}>Pitch Intelligence</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>

            {pitch.top_pain_point && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#ef444411', border: '1px solid #ef444433' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#ef4444', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>🎯 Top Pain Point</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.top_pain_point}</div>
              </div>
            )}

            {pitch.best_value_prop && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#10b98111', border: '1px solid #10b98133' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#10b981', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>✅ Best Value Prop</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.best_value_prop}</div>
              </div>
            )}

            {pitch.best_angle && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#6366f111', border: '1px solid #6366f133' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#6366f1', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>✅ Best Angle</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.best_angle}</div>
              </div>
            )}

            {pitch.suggested_cta && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#3b82f611', border: '1px solid #3b82f633' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#3b82f6', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>✅ Suggested CTA</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.suggested_cta}</div>
              </div>
            )}

            {Array.isArray(pitch.do_not_pitch) && pitch.do_not_pitch.length > 0 && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#f9731611', border: '1px solid #f9731633' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#f97316', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 6 }}>❌ Do NOT Pitch</div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                  {pitch.do_not_pitch.map((item, i) => (
                    <span key={i} style={{ padding: '3px 8px', borderRadius: 4, fontSize: 11,
                      background: '#f9731622', color: '#f97316', border: '1px solid #f9731644' }}>
                      {item}
                    </span>
                  ))}
                </div>
              </div>
            )}

          </div>
        </div>
      )}

    </div>
  )
}

function ScoringSection({ lead, full }) {
  // Support both old format (full.scoring) and new 8-stage format (full.lead_scoring)
  const s = full.lead_scoring || full.scoring || {}
  const icp     = lead.icp_fit_score             ?? s.icp_fit_score  ?? 0
  const intent  = lead.intent_score              ?? s.intent_score   ?? 0
  const timing  = lead.timing_score              ?? s.timing_score   ?? 0
  const dataCmp = lead.data_completeness_score   ?? s.data_completeness_score ?? 0
  const total   = lead.total_score               ?? s.overall_score  ?? 0
  const tier    = TIER[lead.score_tier] || TIER.cold
  const flags   = parseArr(lead.disqualification_flags || s.disqualification_flags)

  // Waterfall scoring breakdown
  const scoreBreakdown = [
    { label: 'ICP Fit',          score: icp,     max: 40, color: '#6366f1', desc: 'Seniority, title, company size' },
    { label: 'Intent Signals',   score: intent,  max: 30, color: '#f97316', desc: 'Funding, hiring, job change' },
    { label: 'Timing',           score: timing,  max: 20, color: '#10b981', desc: 'Recency of trigger events' },
    { label: 'Data Completeness',score: dataCmp, max: 10, color: '#3b82f6', desc: 'Email, phone, profile richness' },
  ]

  return (
    <div>
      {/* Waterfall score bars */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
          letterSpacing: '0.07em', marginBottom: 10 }}>Score Breakdown (Waterfall)</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {scoreBreakdown.map(({ label, score, max, color, desc }) => (
            <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ width: 140, flexShrink: 0 }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-2)', marginBottom: 1 }}>{label}</div>
                <div style={{ fontSize: 9, color: 'var(--text-3)' }}>{desc}</div>
              </div>
              <div style={{ flex: 1, height: 8, borderRadius: 4, background: 'var(--bg-base)', overflow: 'hidden' }}>
                <div style={{
                  height: '100%', borderRadius: 4, background: color,
                  width: `${Math.min((score / max) * 100, 100)}%`,
                  transition: 'width 0.6s ease',
                }} />
              </div>
              <div style={{ width: 52, textAlign: 'right', flexShrink: 0 }}>
                <span style={{ fontSize: 13, fontWeight: 800, color }}>{score}</span>
                <span style={{ fontSize: 10, color: 'var(--text-3)' }}>/{max}</span>
              </div>
            </div>
          ))}
          {/* Total divider */}
          <div style={{ borderTop: '1px solid var(--border-1)', paddingTop: 10, display: 'flex', alignItems: 'center', gap: 12 }}>
            <div style={{ width: 140, flexShrink: 0 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)' }}>TOTAL SCORE</div>
            </div>
            <div style={{ flex: 1, height: 10, borderRadius: 5, background: 'var(--bg-base)', overflow: 'hidden' }}>
              <div style={{
                height: '100%', borderRadius: 5,
                background: `linear-gradient(90deg,#6366f1,${tier.color})`,
                width: `${Math.min(total, 100)}%`,
                transition: 'width 0.8s ease',
              }} />
            </div>
            <div style={{ width: 52, textAlign: 'right', display: 'flex', alignItems: 'baseline', gap: 3, flexShrink: 0 }}>
              <span style={{ fontSize: 20, fontWeight: 800, color: tier.color }}>{total}</span>
              <span style={{ fontSize: 10, color: 'var(--text-3)' }}>/100</span>
            </div>
          </div>
        </div>
      </div>

      {/* Tier + explanation */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
        <div style={{ padding: '10px 16px', borderRadius: 9, background: tier.bg, border: `1px solid ${tier.border}`, display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 20 }}>{tier.icon}</span>
          <div>
            <div style={{ fontSize: 10, color: tier.color, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Score Tier</div>
            <div style={{ fontSize: 15, fontWeight: 800, color: tier.color }}>{tier.label}</div>
          </div>
        </div>
      </div>

      <Grid>
        <Field icon={<Target size={12} />}  label="Score Explanation"  value={lead.score_explanation || s.score_explanation} wide />
        <Field icon={<Layers size={12} />}  label="ICP Match Tier"     value={lead.icp_match_tier || s.icp_match_tier} />
        <Field icon={<AlertCircle size={12} />} label="Disqualification Flags"
          value={flags.length ? flags.join(', ') : 'None'} />
      </Grid>
    </div>
  )
}

function OutreachSection({ lead, full }) {
  const o = full.outreach || {}
  const seq = (() => { try { return JSON.parse(lead.outreach_sequence || '{}') } catch { return o.sequence || {} } })()

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Grid>
        <Field icon={<Mail size={12} />}       label="Best Contact Channel" value={lead.best_channel || o.best_channel} />
        <Field icon={<Clock size={12} />}       label="Best Send Time"       value={lead.best_send_time || o.best_send_time} />
        <Field icon={<TrendingUp size={12} />}  label="Outreach Angle"       value={lead.outreach_angle || o.outreach_angle} />
        <Field icon={<Layers size={12} />}      label="Sequence Assigned"    value={lead.sequence_type || o.sequence_type} />
        <Field icon={<CalendarDays size={12} />}label="Last Contacted"       value={lead.last_contacted || o.last_contacted || 'Not yet contacted'} />
        <Field icon={<CheckCircle size={12} />} label="Email Status"         value={lead.email_status || o.email_status}
          badge={<EmailStatusBadge status={lead.email_status || o.email_status} />} />
      </Grid>

      {(lead.email_subject || o.email_subject) && (
        <div>
          <Lbl style={{ display: 'block', marginBottom: 6 }}>Suggested Email Subject</Lbl>
          <CopyBlock text={lead.email_subject || o.email_subject} />
        </div>
      )}
      {(lead.cold_email || o.cold_email) && (
        <div>
          <Lbl style={{ display: 'block', marginBottom: 6 }}>Generated Cold Email</Lbl>
          <CopyBlock text={lead.cold_email || o.cold_email} multiline />
        </div>
      )}
      {(lead.linkedin_note || o.linkedin_note) && (
        <div>
          <Lbl style={{ display: 'block', marginBottom: 6 }}>LinkedIn Connection Note</Lbl>
          <CopyBlock text={lead.linkedin_note || o.linkedin_note} />
        </div>
      )}

      {/* Sequence timeline */}
      {Object.keys(seq).length > 0 && (
        <div>
          <Lbl style={{ display: 'block', marginBottom: 10 }}>Outreach Sequence</Lbl>
          {[
            { key: 'day1', day: 'Day 1', color: '#6366f1' },
            { key: 'day2', day: 'Day 2', color: '#8b5cf6' },
            { key: 'day4', day: 'Day 4', color: '#f97316' },
            { key: 'day7', day: 'Day 7', color: '#10b981' },
            { key: 'day14',day: 'Day 14',color: '#6b7280' },
          ].filter(item => seq[item.key]).map(({ key, day, color }) => (
            <div key={key} style={{ display: 'flex', gap: 12, marginBottom: 8, alignItems: 'flex-start' }}>
              <div style={{
                width: 32, height: 32, borderRadius: '50%', flexShrink: 0,
                background: `${color}18`, border: `2px solid ${color}40`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 9, fontWeight: 700, color,
              }}>{day.replace('Day ','')}</div>
              <div style={{ paddingTop: 6 }}>
                <div style={{ fontSize: 10, color: 'var(--text-3)', marginBottom: 2 }}>{day}</div>
                <div style={{ fontSize: 13, color: 'var(--text-1)' }}>{seq[key]}</div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function CrmSection({ lead, full }) {
  const c = full.crm || {}
  const tags = parseArr(lead.tags || c.tags)
  const waterfallLog = (() => {
    try { return JSON.parse(lead.waterfall_log || '[]') } catch { return [] }
  })()

  const SOURCE_COLORS = {
    'Bright Data': '#0077b5',
    'Apollo.io Org': '#6366f1',
    'Apollo.io Person': '#8b5cf6',
    'Hunter.io Domain': '#f97316',
    'Hunter.io Email': '#f97316',
    'Clearbit Logo API': '#10b981',
    'Pattern Guess': '#6b7280',
  }

  return (
    <div>
      <Grid>
        <Field icon={<Zap size={12} />}         label="Lead Source"       value={lead.lead_source || c.lead_source} />
        <Field icon={<Database size={12} />}     label="Enrichment Source" value={lead.enrichment_source || c.enrichment_source} />
        <Field icon={<CalendarDays size={12} />} label="Enrichment Date"   value={lead.enriched_at ? new Date(lead.enriched_at).toLocaleString() : (c.enrichment_date || '—')} />
        <Field icon={<Layers size={12} />}       label="Enrichment Depth"  value={c.enrichment_depth || 'Deep'} />
        <Field icon={<BarChart2 size={12} />}    label="Data Completeness"
          value={
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <div style={{ flex: 1, height: 6, borderRadius: 3, background: 'var(--bg-base)', overflow: 'hidden' }}>
                <div style={{ height: '100%', borderRadius: 3, width: `${lead.data_completeness || c.data_completeness || 0}%`,
                  background: `hsl(${(lead.data_completeness || 0) * 1.2},70%,50%)`, transition: 'width 0.5s' }} />
              </div>
              <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)', minWidth: 34 }}>
                {lead.data_completeness || c.data_completeness || 0}%
              </span>
            </div>
          } wide />
        <Field icon={<RefreshCw size={12} />}    label="Last Re-Enriched"  value={c.last_re_enriched ? new Date(c.last_re_enriched).toLocaleString() : '—'} />
        <Field icon={<User size={12} />}         label="Assigned Owner"    value={lead.assigned_owner || c.assigned_owner || 'Unassigned'} />
        <Field icon={<Layers size={12} />}       label="CRM Stage"         value={lead.crm_stage || c.crm_stage} />
      </Grid>
      {tags.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <Lbl>Tags</Lbl>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 8 }}>
            {tags.map(t => <TagBadge key={t} text={t} size="md" />)}
          </div>
        </div>
      )}

      {/* Waterfall Log */}
      {waterfallLog.length > 0 && (
        <div style={{ marginTop: 18 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', textTransform: 'uppercase',
            letterSpacing: '0.07em', marginBottom: 10, display: 'flex', alignItems: 'center', gap: 6 }}>
            <Zap size={11} color="#6366f1" /> Enrichment Waterfall Trace
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {waterfallLog.map((entry, i) => {
              const color = SOURCE_COLORS[entry.source] || '#6b7280'
              const fields = (entry.fields_found || []).slice(0, 6)
              return (
                <div key={i} style={{
                  display: 'flex', alignItems: 'flex-start', gap: 10,
                  padding: '8px 12px', borderRadius: 8,
                  background: 'var(--bg-elevated)', border: `1px solid ${color}22`,
                }}>
                  <div style={{
                    width: 22, height: 22, borderRadius: '50%', flexShrink: 0,
                    background: `${color}15`, border: `1.5px solid ${color}40`,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: 9, fontWeight: 800, color,
                  }}>{entry.step}</div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
                      <span style={{ fontSize: 11, fontWeight: 700, color }}>{entry.source}</span>
                      {entry.note && <span style={{ fontSize: 10, color: 'var(--text-3)' }}>· {entry.note}</span>}
                    </div>
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      {fields.map(f => (
                        <span key={f} style={{
                          fontSize: 9, padding: '1px 6px', borderRadius: 6, fontWeight: 600,
                          background: `${color}12`, color, border: `1px solid ${color}28`,
                        }}>{f.replace(/_/g, ' ')}</span>
                      ))}
                      {(entry.fields_found || []).length > 6 && (
                        <span style={{ fontSize: 9, color: 'var(--text-3)' }}>+{entry.fields_found.length - 6} more</span>
                      )}
                    </div>
                  </div>
                  <CheckCircle size={13} color={color} style={{ flexShrink: 0, marginTop: 2 }} />
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Reusable micro-components
// ─────────────────────────────────────────────────────────────────────────────

function Grid({ children }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(220px,1fr))', gap: 8 }}>
      {children}
    </div>
  )
}

function Field({ icon, label, value, badge, link, copyable, wide }) {
  const [copied, setCopied] = useState(false)
  const isEmpty = !value || value === '—' || (typeof value === 'string' && !value.trim())
  const copy = () => {
    if (typeof value === 'string') {
      navigator.clipboard.writeText(value)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    }
  }
  return (
    <div style={{
      padding: '9px 12px', borderRadius: 8,
      background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
      gridColumn: wide ? '1/-1' : undefined,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 4 }}>
        <span style={{ color: 'var(--text-3)', display: 'flex' }}>{icon}</span>
        <span style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, minHeight: 20 }}>
        {link && !isEmpty ? (
          <a href={link.startsWith('http') ? link : `https://${link}`} target="_blank" rel="noreferrer"
            style={{ fontSize: 12, color: '#6366f1', wordBreak: 'break-all', flex: 1, textDecoration: 'none' }}>
            {value}
          </a>
        ) : (
          <span style={{ fontSize: 12, color: isEmpty ? 'var(--text-3)' : 'var(--text-1)', flex: 1, wordBreak: 'break-word', fontStyle: isEmpty ? 'italic' : 'normal' }}>
            {isEmpty ? '—' : (typeof value === 'string' ? value : value)}
          </span>
        )}
        {badge}
        {copyable && !isEmpty && typeof value === 'string' && (
          <button onClick={copy} style={{ background: 'none', border: 'none', cursor: 'pointer', color: copied ? '#10b981' : 'var(--text-3)', padding: 1, flexShrink: 0 }}>
            {copied ? <Check size={11} /> : <Copy size={11} />}
          </button>
        )}
      </div>
    </div>
  )
}

function ScoreBar({ label, score, max, color }) {
  return (
    <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontSize: 11, color: 'var(--text-3)', fontWeight: 500 }}>{label}</span>
        <span style={{ fontSize: 12, fontWeight: 700, color }}>{score}<span style={{ fontSize: 10, fontWeight: 400, color: 'var(--text-3)' }}>/{max}</span></span>
      </div>
      <div style={{ height: 5, borderRadius: 3, background: 'var(--bg-base)', overflow: 'hidden' }}>
        <div style={{ height: '100%', borderRadius: 3, background: color, width: `${Math.min((score/max)*100,100)}%`, transition: 'width 0.5s' }} />
      </div>
    </div>
  )
}

function ScoreCircle({ score, tier }) {
  const cfg = TIER[tier] || TIER.cold
  return (
    <div style={{
      width: 36, height: 36, borderRadius: '50%', flexShrink: 0,
      border: `2px solid ${cfg.border}`, background: cfg.bg,
      display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
    }}>
      <span style={{ fontSize: 11, fontWeight: 800, color: cfg.color, lineHeight: 1 }}>{score}</span>
    </div>
  )
}

function TierBadge({ tier }) {
  const cfg = TIER[tier] || TIER.cold
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3, padding: '2px 8px', borderRadius: 10,
      fontSize: 10, fontWeight: 700, background: cfg.bg, border: `1px solid ${cfg.border}`, color: cfg.color }}>
      {cfg.icon} {cfg.label}
    </span>
  )
}

function Avatar({ name, tier, src, size = 34 }) {
  const cfg = TIER[tier] || TIER.cold
  const [imgErr, setImgErr] = useState(false)
  if (src && !imgErr) {
    return (
      <img src={src} alt={name || ''} onError={() => setImgErr(true)}
        referrerPolicy="no-referrer" crossOrigin="anonymous"
        style={{ width: size, height: size, borderRadius: '50%', objectFit: 'cover', flexShrink: 0,
          border: `2px solid ${cfg.border}` }} />
    )
  }
  return (
    <div style={{
      width: size, height: size, borderRadius: '50%', flexShrink: 0,
      background: `linear-gradient(135deg,${cfg.color}25,${cfg.color}50)`,
      border: `2px solid ${cfg.border}`,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontSize: Math.floor(size * 0.38), fontWeight: 700, color: cfg.color,
    }}>
      {(name || '?')[0].toUpperCase()}
    </div>
  )
}

function CompanyLogo({ src, name, size = 28 }) {
  const [imgErr, setImgErr] = useState(false)
  if (src && !imgErr) {
    return (
      <img src={src} alt={name || ''} onError={() => setImgErr(true)}
        style={{ width: size, height: size, borderRadius: 6, objectFit: 'contain', flexShrink: 0,
          background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', padding: 2 }} />
    )
  }
  return (
    <div style={{
      width: size, height: size, borderRadius: 6, flexShrink: 0,
      background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
    }}>
      <Building2 size={Math.floor(size * 0.55)} color="#6366f1" />
    </div>
  )
}

function TagBadge({ text, size }) {
  return (
    <span style={{
      fontSize: size === 'md' ? 11 : 9, fontWeight: 600, padding: size === 'md' ? '3px 9px' : '1px 6px',
      borderRadius: 10, background: 'rgba(99,102,241,0.1)', color: '#a5b4fc',
      border: '1px solid rgba(99,102,241,0.2)',
    }}>{text}</span>
  )
}

function TagList({ items, color }) {
  if (!items || !items.length) return <span style={{ color: 'var(--text-3)', fontStyle: 'italic', fontSize: 12 }}>—</span>
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
      {items.map(item => (
        <span key={item} style={{ fontSize: 10, padding: '1px 7px', borderRadius: 10, fontWeight: 600,
          background: `${color}15`, color, border: `1px solid ${color}30` }}>{item}</span>
      ))}
    </div>
  )
}

function ConfBadge({ c, src }) {
  const color = c === 'high' ? '#10b981' : c === 'medium' ? '#f97316' : '#6b7280'
  return (
    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 10, fontWeight: 700,
      background: `${color}15`, color, border: `1px solid ${color}35`, whiteSpace: 'nowrap' }}>
      {c} · {src}
    </span>
  )
}

function FundingBadge({ stage }) {
  const colors = {
    'series b': '#8b5cf6', 'series c': '#6366f1', 'series a': '#f97316',
    seed: '#10b981', 'pre-seed': '#3b82f6', angel: '#fbbf24',
  }
  const color = colors[(stage || '').toLowerCase()] || '#6b7280'
  return (
    <span style={{ fontSize: 9, padding: '1px 7px', borderRadius: 10, fontWeight: 700,
      background: `${color}18`, color, border: `1px solid ${color}35` }}>{stage}</span>
  )
}

function EmailStatusBadge({ status }) {
  if (!status) return null
  const isVerified = status.toLowerCase().includes('valid') || status.toLowerCase().includes('verified')
  const color = isVerified ? '#10b981' : '#f97316'
  return (
    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 10, fontWeight: 700,
      background: `${color}15`, color, border: `1px solid ${color}35` }}>
      {isVerified ? '✓ ' : '⚠ '}{status}
    </span>
  )
}

function ActiveBadge() {
  return (
    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 10, fontWeight: 700,
      background: 'rgba(16,185,129,0.12)', color: '#10b981', border: '1px solid rgba(16,185,129,0.3)' }}>
      Active
    </span>
  )
}

function StatusBadge({ status }) {
  const map = {
    running:               { color: '#6366f1', icon: <RefreshSpin size={10} />, label: 'Running' },
    pending:               { color: '#f97316', icon: <Clock size={10} />,       label: 'Pending' },
    completed:             { color: '#10b981', icon: <CheckCircle size={10} />, label: 'Completed' },
    completed_with_errors: { color: '#f97316', icon: <AlertCircle size={10} />, label: 'Completed (errors)' },
    failed:                { color: '#ef4444', icon: <AlertCircle size={10} />, label: 'Failed' },
    fallback:              { color: '#8b5cf6', icon: <RefreshSpin size={10} />, label: 'Processing' },
    stale:                 { color: '#6b7280', icon: <AlertCircle size={10} />, label: 'Stale' },
  }
  const cfg = map[status] || { color: '#6b7280', icon: null, label: status }
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px',
      borderRadius: 10, fontSize: 11, fontWeight: 600,
      background: `${cfg.color}18`, color: cfg.color, border: `1px solid ${cfg.color}35` }}>
      {cfg.icon} {cfg.label}
    </span>
  )
}

function CopyBlock({ text, multiline }) {
  const [copied, setCopied] = useState(false)
  const copy = () => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1800) }
  return (
    <div style={{ position: 'relative' }}>
      <div style={{
        padding: '10px 40px 10px 12px', borderRadius: 8, border: '1px solid var(--border-1)',
        background: 'var(--bg-elevated)', fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6,
        whiteSpace: multiline ? 'pre-wrap' : 'normal',
      }}>{text}</div>
      <button onClick={copy} style={{ position: 'absolute', top: 8, right: 8,
        background: 'none', border: 'none', cursor: 'pointer', color: copied ? '#10b981' : 'var(--text-3)' }}>
        {copied ? <Check size={13} /> : <Copy size={13} />}
      </button>
    </div>
  )
}

function Card({ children, style }) {
  return <div style={{ padding: '16px 18px', borderRadius: 10, background: 'var(--bg-card)', border: '1px solid var(--border-1)', ...style }}>{children}</div>
}

function Lbl({ children, style }) {
  return <span style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', ...style }}>{children}</span>
}

function Btn({ children, onClick, loading, disabled, style }) {
  return (
    <button onClick={onClick} disabled={loading || disabled} style={{
      display: 'flex', alignItems: 'center', gap: 7,
      padding: '10px 20px', borderRadius: 8, border: 'none',
      background: loading || disabled ? 'var(--bg-elevated)' : 'linear-gradient(135deg,#6366f1,#8b5cf6)',
      color: loading || disabled ? 'var(--text-3)' : '#fff',
      cursor: loading || disabled ? 'not-allowed' : 'pointer',
      fontSize: 13, fontWeight: 600, transition: 'all 0.15s', ...style,
    }}>{children}</button>
  )
}

function GhostBtn({ children, onClick, style }) {
  return (
    <button onClick={onClick} style={{
      display: 'flex', alignItems: 'center', gap: 5,
      padding: '5px 10px', borderRadius: 7, fontSize: 12, fontWeight: 500, cursor: 'pointer',
      border: '1px solid var(--border-1)', background: 'transparent', color: 'var(--text-2)',
      ...style,
    }}>{children}</button>
  )
}

function ErrorBox({ msg }) {
  return (
    <div style={{ padding: '11px 14px', borderRadius: 8, marginBottom: 14,
      background: 'rgba(239,68,68,0.07)', border: '1px solid rgba(239,68,68,0.25)',
      color: '#ef4444', fontSize: 13, display: 'flex', gap: 8, alignItems: 'flex-start' }}>
      <AlertCircle size={14} style={{ flexShrink: 0, marginTop: 1 }} />{msg}
    </div>
  )
}

function LoadingCard() {
  const steps = [
    'Connecting to Bright Data…',
    'Scraping LinkedIn profile…',
    'Running email waterfall (Hunter → Apollo)…',
    'Building 7-category intelligence report…',
    'Generating AI outreach sequences…',
    'Calculating lead score & ICP match…',
  ]
  const [step, setStep] = useState(0)
  useEffect(() => {
    const t = setInterval(() => setStep(s => (s + 1) % steps.length), 2000)
    return () => clearInterval(t)
  }, [])
  return (
    <Card>
      <div style={{ padding: '20px 0', textAlign: 'center' }}>
        <div style={{ width: 40, height: 40, borderRadius: '50%', margin: '0 auto 14px',
          border: '3px solid var(--border-1)', borderTop: '3px solid #6366f1',
          animation: 'spin 0.8s linear infinite' }} />
        <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 4 }}>{steps[step]}</div>
        <div style={{ fontSize: 11, color: 'var(--text-3)' }}>30–60 seconds · 7 enrichment categories</div>
      </div>
    </Card>
  )
}

function RefreshSpin({ size = 14 }) {
  return <RefreshCw size={size} style={{ animation: 'spin 1s linear infinite' }} />
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────


// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function parseArr(v) {
  if (Array.isArray(v)) return v.filter(Boolean)
  if (typeof v === 'string' && v.startsWith('[')) {
    try { return JSON.parse(v).filter(Boolean) } catch {}
  }
  if (typeof v === 'string' && v) return v.split(',').map(s => s.trim()).filter(Boolean)
  return []
}

function parseTags(v) { return parseArr(v) }

function _buildJsonPayload(lead) {
  let full = {}
  try {
    full = (lead.full_data && typeof lead.full_data === 'object')
      ? lead.full_data
      : JSON.parse(lead.full_data || '{}')
  } catch {}
  return {
    id: lead.id,
    enriched_at: lead.enriched_at,
    data_completeness: `${lead.data_completeness}%`,
    identity: {
      full_name: lead.name,
      avatar_url: lead.avatar_url,
      work_email: lead.work_email,
      personal_email: lead.personal_email,
      direct_phone: lead.direct_phone,
      linkedin_url: lead.linkedin_url,
      twitter: lead.twitter,
      city: lead.city,
      country: lead.country,
      timezone: lead.timezone,
      ...(full.identity || {}),
    },
    professional: {
      current_title: lead.title,
      seniority_level: lead.seniority_level,
      department: lead.department,
      years_in_role: lead.years_in_role,
      previous_companies: parseArr(lead.previous_companies),
      top_skills: parseArr(lead.top_skills),
      education: lead.education,
      certifications: parseArr(lead.certifications),
      languages: parseArr(lead.languages),
      ...(full.professional || {}),
    },
    company: {
      name: lead.company,
      logo: lead.company_logo,
      website: lead.company_website,
      email: lead.company_email,
      phone: lead.company_phone,
      description: lead.company_description,
      linkedin: lead.company_linkedin,
      twitter: lead.company_twitter,
      industry: lead.industry,
      employee_count: lead.employee_count,
      hq_location: lead.hq_location,
      founded_year: lead.founded_year,
      funding_stage: lead.funding_stage,
      total_funding: lead.total_funding,
      last_funding_date: lead.last_funding_date,
      lead_investor: lead.lead_investor,
      annual_revenue_est: lead.annual_revenue,
      tech_stack: parseArr(lead.tech_stack),
      hiring_velocity: lead.hiring_velocity,
      ...(full.company || {}),
    },
    intent_signals: {
      recent_funding_event: lead.recent_funding_event,
      hiring_signal: lead.hiring_signal,
      job_change: lead.job_change,
      linkedin_activity: lead.linkedin_activity,
      news_mention: lead.news_mention,
      product_launch: lead.product_launch,
      competitor_usage: lead.competitor_usage,
      review_activity: lead.review_activity,
      ...(full.intent_signals || {}),
    },
    scoring: {
      icp_fit_score: lead.icp_fit_score,
      intent_score: lead.intent_score,
      timing_score: lead.timing_score,
      overall_score: lead.total_score,
      score_explanation: lead.score_explanation,
      icp_match_tier: lead.icp_match_tier,
      disqualification_flags: parseArr(lead.disqualification_flags),
      ...(full.scoring || {}),
    },
    outreach: {
      email_subject: lead.email_subject,
      cold_email: lead.cold_email,
      linkedin_note: lead.linkedin_note,
      best_channel: lead.best_channel,
      best_send_time: lead.best_send_time,
      outreach_angle: lead.outreach_angle,
      sequence_type: lead.sequence_type,
      sequence: (() => { try { return JSON.parse(lead.outreach_sequence || '{}') } catch { return {} } })(),
      last_contacted: lead.last_contacted,
      email_status: lead.email_status,
      ...(full.outreach || {}),
    },
    crm: {
      lead_source: lead.lead_source,
      enrichment_source: lead.enrichment_source,
      enrichment_date: lead.enriched_at,
      data_completeness: lead.data_completeness,
      crm_stage: lead.crm_stage,
      assigned_owner: lead.assigned_owner,
      tags: parseArr(lead.tags),
      ...(full.crm || {}),
    },
    website_intelligence: (() => {
      const wi = full.website_intelligence || full.website_intel || {}
      return {
        product_offerings: parseArr(lead.product_offerings),
        value_proposition: lead.value_proposition || wi.value_proposition,
        target_customers: parseArr(lead.target_customers),
        business_model: lead.business_model || wi.business_model,
        pricing_signals: lead.pricing_signals || wi.pricing_signals,
        product_category: lead.product_category || wi.product_category,
        ...wi,
      }
    })(),
    lead_scoring: {
      icp_fit_score: lead.icp_fit_score,
      intent_score: lead.intent_score,
      timing_score: lead.timing_score,
      data_completeness_score: lead.data_completeness_score,
      overall_score: lead.total_score,
      score_tier: lead.score_tier,
      score_explanation: lead.score_explanation,
      icp_match_tier: lead.icp_match_tier,
      disqualification_flags: parseArr(lead.disqualification_flags),
      ...(full.lead_scoring || full.scoring || {}),
    },
    waterfall_log: (() => { try { return JSON.parse(lead.waterfall_log || '[]') } catch { return [] } })(),
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Company Intel Section (P1–P6 company enrichment)
// ───────────────────────────────────────────────────────────────────────────���─
function CompanyIntelSection({ lead }) {
  const companyTags    = (() => { try { return JSON.parse(lead.company_tags    || '[]') } catch { return [] } })()
  const cultureSigs    = (() => { try { return JSON.parse(lead.culture_signals || '{}') } catch { return {} } })()
  const accountPitch   = (() => { try { return JSON.parse(lead.account_pitch   || '{}') } catch { return {} } })()
  const wapTech        = (() => { try { return JSON.parse(lead.wappalyzer_tech || '[]') } catch { return [] } })()
  const newsMentions   = (() => { try { return JSON.parse(lead.news_mentions   || '[]') } catch { return [] } })()
  const cbData         = (() => { try { return JSON.parse(lead.crunchbase_data || '{}') } catch { return {} } })()
  const linkedinPosts  = (() => { try { return JSON.parse(lead.linkedin_posts  || '[]') } catch { return [] } })()

  const companyScore = lead.company_score || 0
  const combinedScore = lead.combined_score || lead.total_score || 0
  const scoreTier = lead.company_score_tier || 'C'

  const TAG_COLORS = ['#6366f1','#8b5cf6','#10b981','#f97316','#3b82f6','#ec4899','#14b8a6','#f59e0b','#ef4444','#84cc16']

  const noData = !companyTags.length && !cultureSigs.growth_stage && !accountPitch.top_account_pain && !wapTech.length

  if (noData) return (
    <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
      Company Intelligence not yet generated.<br />
      <span style={{ fontSize: 11 }}>Re-enrich this lead to run company enrichment (posts, tech stack, news, AI analysis).</span>
    </div>
  )

  const card = (children, extra = {}) => (
    <div style={{ borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', padding: '14px 16px', ...extra }}>
      {children}
    </div>
  )
  const label = (text) => (
    <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-3)', marginBottom: 8 }}>{text}</div>
  )
  const row = (k, v) => v ? (
    <div key={k} style={{ display: 'flex', gap: 8, fontSize: 12, marginBottom: 5 }}>
      <span style={{ color: 'var(--text-3)', flexShrink: 0, minWidth: 120 }}>{k}</span>
      <span style={{ color: 'var(--text-1)', fontWeight: 500 }}>{v}</span>
    </div>
  ) : null

  const tierColor = { 'A+': '#ef4444', A: '#f97316', B: '#3b82f6', C: '#6b7280' }[scoreTier] || '#6b7280'

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>

      {/* ── Score bar ── */}
      {(companyScore > 0 || combinedScore > 0) && card(
        <>
          <div style={{ display: 'flex', gap: 20, alignItems: 'center' }}>
            <div style={{ flex: 1 }}>
              {label('Company Score')}
              <div style={{ height: 8, borderRadius: 4, background: 'var(--bg-base)', overflow: 'hidden', marginBottom: 4 }}>
                <div style={{ height: '100%', borderRadius: 4, width: `${Math.min(companyScore, 100)}%`,
                  background: tierColor, transition: 'width 0.8s ease' }} />
              </div>
              <div style={{ fontSize: 10, color: 'var(--text-3)' }}>Funding · Tech · News · Posts · ICP Size</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 26, fontWeight: 800, color: tierColor, lineHeight: 1 }}>{companyScore}</div>
              <div style={{ fontSize: 10, color: 'var(--text-3)' }}>/100</div>
              <div style={{ fontSize: 11, fontWeight: 700, color: tierColor, marginTop: 2 }}>Tier {scoreTier}</div>
            </div>
            {combinedScore > 0 && (
              <div style={{ textAlign: 'center', paddingLeft: 16, borderLeft: '1px solid var(--border-1)' }}>
                <div style={{ fontSize: 10, color: 'var(--text-3)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 2 }}>Combined</div>
                <div style={{ fontSize: 26, fontWeight: 800,
                  color: combinedScore >= 90 ? '#ef4444' : combinedScore >= 70 ? '#f97316' : '#6366f1', lineHeight: 1 }}>
                  {combinedScore}
                </div>
                <div style={{ fontSize: 10, color: 'var(--text-3)' }}>/100</div>
                {combinedScore >= 90 && <div style={{ fontSize: 10, fontWeight: 700, color: '#ef4444', marginTop: 2 }}>Top 1%</div>}
              </div>
            )}
          </div>
        </>
      )}

      {/* ── Company Tags ── */}
      {companyTags.length > 0 && card(
        <>
          {label('Account Tags')}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {companyTags.map((tag, i) => (
              <span key={i} style={{
                fontSize: 11, fontWeight: 600, padding: '3px 9px', borderRadius: 20,
                background: TAG_COLORS[i % TAG_COLORS.length] + '18',
                color: TAG_COLORS[i % TAG_COLORS.length],
                border: `1px solid ${TAG_COLORS[i % TAG_COLORS.length]}40`,
              }}>{tag}</span>
            ))}
          </div>
        </>
      )}

      {/* ── Culture & Growth Signals ── */}
      {Object.keys(cultureSigs).length > 0 && card(
        <>
          {label('Culture & Growth Signals')}
          {row('Growth Stage',        cultureSigs.growth_stage)}
          {row('Tech Maturity',       cultureSigs.tech_maturity)}
          {row('Hiring Velocity',     cultureSigs.hiring_velocity)}
          {row('Content Themes',      cultureSigs.content_themes)}
          {row('Community Presence',  cultureSigs.community_presence)}
        </>
      )}

      {/* ── Account-Level Pitch ── */}
      {Object.keys(accountPitch).length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          {[
            { key: 'top_account_pain',    label: 'Account Pain',     color: '#ef4444' },
            { key: 'best_value_prop',     label: 'Value Prop',       color: '#10b981' },
            { key: 'executive_objection', label: 'Exec Objection',   color: '#f97316' },
            { key: 'best_angle',          label: 'Best Angle',       color: '#8b5cf6' },
            { key: 'account_cta',         label: 'Account CTA',      color: '#3b82f6' },
          ].filter(f => accountPitch[f.key]).map(f => (
            <div key={f.key} style={{
              borderRadius: 9, padding: '12px 14px',
              background: f.color + '0f', border: `1px solid ${f.color}30`,
            }}>
              <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: f.color, marginBottom: 5 }}>{f.label}</div>
              <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.5 }}>{accountPitch[f.key]}</div>
            </div>
          ))}
        </div>
      )}

      {/* ── Crunchbase Funding ── */}
      {(cbData.funding_stage || cbData.total_funding || cbData.investors?.length) && card(
        <>
          {label('Crunchbase Signals')}
          {row('Funding Stage',    cbData.funding_stage)}
          {row('Total Funding',    cbData.total_funding)}
          {row('Last Round',       cbData.last_funding_date)}
          {row('Lead Investors',   cbData.investors?.slice(0,3).join(', '))}
          {row('Employee Range',   cbData.employee_range)}
        </>
      )}

      {/* ── Wappalyzer Tech Stack ── */}
      {wapTech.length > 0 && card(
        <>
          {label(`Tech Stack (${wapTech.length} detected)`)}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
            {wapTech.map((tech, i) => (
              <span key={i} style={{
                fontSize: 11, padding: '2px 8px', borderRadius: 5,
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
                color: 'var(--text-2)', fontFamily: 'monospace',
              }}>{tech}</span>
            ))}
          </div>
        </>
      )}

      {/* ── Google News ── */}
      {newsMentions.length > 0 && card(
        <>
          {label(`Recent News (${newsMentions.length} mentions)`)}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {newsMentions.slice(0, 5).map((n, i) => (
              <div key={i} style={{ fontSize: 12, borderBottom: i < newsMentions.length - 1 && i < 4 ? '1px solid var(--border-1)' : 'none', paddingBottom: 7 }}>
                <div style={{ color: 'var(--text-1)', fontWeight: 500, marginBottom: 2 }}>{n.title}</div>
                <div style={{ display: 'flex', gap: 10, color: 'var(--text-3)', fontSize: 11 }}>
                  {n.source && <span>{n.source}</span>}
                  {n.date && <span>{n.date.slice(0, 16)}</span>}
                  {n.url && <a href={n.url} target="_blank" rel="noreferrer" style={{ color: '#6366f1' }}>View →</a>}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── LinkedIn Company Posts ── */}
      {linkedinPosts.length > 0 && card(
        <>
          {label(`Company Posts (${linkedinPosts.length})`)}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {linkedinPosts.slice(0, 5).map((p, i) => (
              <div key={i} style={{ fontSize: 12, borderBottom: i < 4 ? '1px solid var(--border-1)' : 'none', paddingBottom: 8 }}>
                <div style={{ color: 'var(--text-1)', lineHeight: 1.5, marginBottom: 3 }}>{p.text?.slice(0, 200)}{p.text?.length > 200 ? '…' : ''}</div>
                <div style={{ display: 'flex', gap: 10, color: 'var(--text-3)', fontSize: 11 }}>
                  {p.likes > 0 && <span>👍 {p.likes}</span>}
                  {p.comments > 0 && <span>💬 {p.comments}</span>}
                  {p.date && <span>{String(p.date).slice(0, 10)}</span>}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

    </div>
  )
}

// Styles
const inputStyle = {
  padding: '9px 13px', borderRadius: 8,
  border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
  color: 'var(--text-1)', fontSize: 13, outline: 'none', flex: 1,
}
const selStyle = {
  padding: '7px 11px', borderRadius: 7, fontSize: 12,
  border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
  color: 'var(--text-1)', cursor: 'pointer', outline: 'none',
}

// Inject global styles once
if (typeof document !== 'undefined' && !document.getElementById('le-styles')) {
  const s = document.createElement('style')
  s.id = 'le-styles'
  s.textContent = `
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
    :root { --bg-card: var(--bg-elevated, #f8f8f8); }
    [data-theme="dark"] { --bg-card: #181818; }
  `
  document.head.appendChild(s)
}
