// LinkedInFinderPage.jsx — LinkedIn profile finder via SerpAPI
import { useState, useEffect, useCallback } from 'react'
import toast from 'react-hot-toast'

const BACKEND = `${import.meta.env.VITE_BACKEND_URL || 'https://api-lead-enrichment-worksbuddy.lbmdemo.com'}/api`
const jsonHdr = () => ({ 'Content-Type': 'application/json' })

const RESULT_OPTIONS = [10, 20, 50, 100]

const TBS_OPTIONS = [
  { value: '', label: 'Any time' },
  { value: 'qdr:d', label: 'Past 24 hours' },
  { value: 'qdr:w', label: 'Past week' },
  { value: 'qdr:m', label: 'Past month' },
  { value: 'qdr:y', label: 'Past year' },
]

const EMPTY_FILTERS = () => ({
  siteType: 'people',       // 'people' = linkedin.com/in | 'company' = linkedin.com/company
  jobTitles: '',            // "Founder, CEO" → ("Founder" OR "CEO")
  locations: '',            // "India, London" → ("India" OR "London")
  industries: '',           // "SaaS, Fintech" → ("SaaS" OR "Fintech")
  contextKeywords: '',      // "automation, CRM" → ("automation" OR "CRM")
  exclusions: '',           // "Intern, Student" → NOT ("Intern" OR "Student")
  companies: '',            // "Stripe, Shopify" → ("Stripe" OR "Shopify")
  tbs: '',                  // SerpAPI freshness
  exactPhrase: '',          // "co-founder at"
})

// Build ("X" OR "Y" OR "Z") from comma-separated string
function boolGroup(csv) {
  const parts = csv.split(',').map(s => s.trim()).filter(Boolean)
  if (!parts.length) return ''
  if (parts.length === 1) return `"${parts[0]}"`
  return `(${parts.map(p => `"${p}"`).join(' OR ')})`
}

function buildQuery(filters) {
  const parts = []
  if (filters.jobTitles)       parts.push(boolGroup(filters.jobTitles))
  if (filters.locations)       parts.push(boolGroup(filters.locations))
  if (filters.industries)      parts.push(boolGroup(filters.industries))
  if (filters.contextKeywords) parts.push(boolGroup(filters.contextKeywords))
  if (filters.companies)       parts.push(boolGroup(filters.companies))
  if (filters.exactPhrase)     parts.push(`"${filters.exactPhrase.trim()}"`)
  if (filters.exclusions) {
    const ex = filters.exclusions.split(',').map(s => s.trim()).filter(Boolean)
    if (ex.length) parts.push(`NOT (${ex.map(p => `"${p}"`).join(' OR ')})`)
  }

  return parts.filter(Boolean).join(' ')
}

function getOrgId() {
  try {
    const token = localStorage.getItem('wb_ai_token')
    if (!token) return 'default'
    const p = JSON.parse(atob(token.split('.')[1]))
    return p.organization_id || p.org_id || 'default'
  } catch { return 'default' }
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function LinkedInFinderPage() {
  const [numResults, setNumResults]   = useState(10)
  const [filters, setFilters]         = useState(EMPTY_FILTERS())
  const [searching, setSearching]     = useState(false)
  const [results, setResults]         = useState([])
  const [allUrls, setAllUrls]         = useState([])
  const [selected, setSelected]       = useState(new Set())
  const [enriching, setEnriching]     = useState(false)
  const [history, setHistory]         = useState([])
  const [historyLoading, setHistoryLoading] = useState(false)
  const [expandedIds, setExpandedIds] = useState(new Set())
  const [activeTab, setActiveTab]     = useState('search')

  const updateFilter = (key, value) => setFilters(f => ({ ...f, [key]: value }))
  const orgId = getOrgId()

  const activeFilterCount = [
    filters.siteType !== 'people',
    filters.jobTitles, filters.locations, filters.industries,
    filters.contextKeywords, filters.exclusions, filters.companies,
    filters.tbs, filters.exactPhrase,
  ].filter(Boolean).length

  // ── History ───────────────────────────────────────────────────────────────

  const loadHistory = useCallback(async () => {
    setHistoryLoading(true)
    try {
      const r = await fetch(`${BACKEND}/linkedin-finder/searches?org_id=${orgId}`, { headers: jsonHdr() })
      const d = await r.json()
      if (d.success) setHistory(d.searches)
    } catch { toast.error('Failed to load history') }
    finally { setHistoryLoading(false) }
  }, [orgId])

  useEffect(() => { loadHistory() }, [loadHistory])

  // ── Search ────────────────────────────────────────────────────────────────

  const handleSearch = async () => {
    const query = buildQuery(filters)
    if (!query) return toast.error('Fill at least one filter to search')
    const queries = [query]

    setSearching(true); setResults([]); setAllUrls([]); setSelected(new Set())
    try {
      const r = await fetch(`${BACKEND}/linkedin-finder/bulk-search`, {
        method: 'POST', headers: jsonHdr(),
        body: JSON.stringify({ queries, org_id: orgId, num: numResults, filters }),
      })
      const data = await r.json()
      if (!r.ok) throw new Error(data.detail || 'Search failed')
      setResults(data.results || [])
      setAllUrls(data.all_urls || [])
      setSelected(new Set(data.all_urls || []))
      toast.success(`Found ${data.total_urls} LinkedIn URLs`)
      loadHistory()
    } catch (e) { toast.error(e.message) }
    finally { setSearching(false) }
  }

  // ── Enrich ────────────────────────────────────────────────────────────────

  const handleEnrich = async () => {
    const urls = [...selected]
    if (!urls.length) return toast.error('Select at least one URL')
    setEnriching(true)
    try {
      const r = await fetch(`${BACKEND}/leads/enrich/bulk`, {
        method: 'POST', headers: jsonHdr(),
        body: JSON.stringify({ linkedin_urls: urls, org_id: orgId }),
      })
      const data = await r.json()
      if (!r.ok) throw new Error(data.detail || 'Enrich failed')
      toast.success(`Job started for ${urls.length} profiles — ${data.job?.id?.slice(0, 8)}…`)
    } catch (e) { toast.error(e.message) }
    finally { setEnriching(false) }
  }

  // ── Delete history ────────────────────────────────────────────────────────

  const handleDelete = async (id) => {
    try {
      await fetch(`${BACKEND}/linkedin-finder/searches/${id}?org_id=${orgId}`, { method: 'DELETE', headers: jsonHdr() })
      setHistory(h => h.filter(s => s.id !== id))
      toast.success('Deleted')
    } catch { toast.error('Delete failed') }
  }

  const toggleUrl    = (url) => setSelected(s => { const n = new Set(s); n.has(url) ? n.delete(url) : n.add(url); return n })
  const toggleAll    = () => setSelected(s => s.size === allUrls.length ? new Set() : new Set(allUrls))
  const toggleExpand = (id) => setExpandedIds(s => { const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n })

  return (
    <div style={{ padding: '24px', maxWidth: 1080, margin: '0 auto', fontFamily: "'Inter', system-ui, sans-serif" }}>

      {/* Header */}
      <div style={{ marginBottom: 22 }}>
        <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--text-1)', marginBottom: 4 }}>LinkedIn Finder</div>
        <div style={{ fontSize: 13, color: 'var(--text-3)' }}>
          Find LinkedIn profiles via SerpAPI · Results auto-saved to database
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 22, borderBottom: '1px solid var(--border-1)' }}>
        {[['search', 'Search'], ['history', `History (${history.length})`]].map(([tab, label]) => (
          <button key={tab} onClick={() => setActiveTab(tab)} style={{
            padding: '7px 16px', fontSize: 12, fontWeight: 600, cursor: 'pointer', border: 'none',
            borderBottom: activeTab === tab ? '2px solid #6366f1' : '2px solid transparent',
            background: 'transparent', color: activeTab === tab ? '#a5b4fc' : 'var(--text-3)',
            marginBottom: -1, borderRadius: '6px 6px 0 0',
          }}>{label}</button>
        ))}
      </div>

      {/* ── SEARCH TAB ──────────────────────────────────────────────────────── */}
      {activeTab === 'search' && (
        <div>

          {/* ── Search Filters ───────────────────────────────────────────────── */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em', display: 'flex', alignItems: 'center', gap: 8 }}>
                Search Filters
                {activeFilterCount > 0 && (
                  <span style={{ background: 'rgba(99,102,241,0.2)', color: '#a5b4fc', fontSize: 10, fontWeight: 700, padding: '1px 7px', borderRadius: 10, textTransform: 'none', letterSpacing: 0 }}>
                    {activeFilterCount} active
                  </span>
                )}
              </div>
              <button onClick={() => setFilters(EMPTY_FILTERS())} style={{ ...ghostBtn, fontSize: 10 }}>Reset all</button>
            </div>

            {/* Row 1: Site Type + Job Titles + Location + Industry */}
            <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr 1fr 1fr', gap: 10, marginBottom: 10 }}>

              {/* 1. Site Type — toggle card */}
              <div style={{ ...filterCard, minWidth: 160 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: 4 }}>
                  Site Filter
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {[
                    { value: 'people',  label: '👤 People Profiles',  sub: 'linkedin.com/in' },
                    { value: 'company', label: '🏢 Company Pages',    sub: 'linkedin.com/company' },
                  ].map(o => (
                    <button key={o.value} onClick={() => updateFilter('siteType', o.value)} style={{
                      padding: '7px 10px', borderRadius: 7, border: '1px solid',
                      borderColor: filters.siteType === o.value ? 'rgba(99,102,241,0.6)' : 'var(--border-1)',
                      background: filters.siteType === o.value ? 'rgba(99,102,241,0.12)' : 'transparent',
                      cursor: 'pointer', textAlign: 'left',
                    }}>
                      <div style={{ fontSize: 12, fontWeight: 600, color: filters.siteType === o.value ? '#a5b4fc' : 'var(--text-2)' }}>{o.label}</div>
                      <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2 }}>{o.sub}</div>
                    </button>
                  ))}
                </div>
              </div>

              {/* 2. Job Titles */}
              <div style={filterCard}>
                <div style={filterLabel}>Job Title / Role</div>
                <textarea
                  value={filters.jobTitles}
                  onChange={e => updateFilter('jobTitles', e.target.value)}
                  placeholder={'Founder, CEO, CTO\nHead of Engineering'}
                  rows={3}
                  style={{ ...filterTextarea }}
                />
                <div style={filterDesc}>Comma-separated → <code style={codeStyle}>("CEO" OR "Founder")</code></div>
              </div>

              {/* 3. Location */}
              <div style={filterCard}>
                <div style={filterLabel}>Location</div>
                <textarea
                  value={filters.locations}
                  onChange={e => updateFilter('locations', e.target.value)}
                  placeholder={'India, United States\nSan Francisco, Bangalore'}
                  rows={3}
                  style={{ ...filterTextarea }}
                />
                <div style={filterDesc}>City, state, or country → <code style={codeStyle}>("India" OR "London")</code></div>
              </div>

              {/* 4. Industry */}
              <div style={filterCard}>
                <div style={filterLabel}>Industry / Niche</div>
                <textarea
                  value={filters.industries}
                  onChange={e => updateFilter('industries', e.target.value)}
                  placeholder={'SaaS, Fintech\nE-commerce, HealthTech'}
                  rows={3}
                  style={{ ...filterTextarea }}
                />
                <div style={filterDesc}>From bio/headline → <code style={codeStyle}>("SaaS" OR "Fintech")</code></div>
              </div>
            </div>

            {/* Row 2: Keywords + Company + Exclusions + Exact Phrase + Freshness */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr 1fr', gap: 10 }}>

              {/* 5. Context Keywords */}
              <div style={filterCard}>
                <div style={filterLabel}>Keywords</div>
                <textarea
                  value={filters.contextKeywords}
                  onChange={e => updateFilter('contextKeywords', e.target.value)}
                  placeholder={'automation, CRM\nworkflow, scaling'}
                  rows={3}
                  style={{ ...filterTextarea }}
                />
                <div style={filterDesc}>Product-fit signals in bio</div>
              </div>

              {/* 7. Company Names */}
              <div style={filterCard}>
                <div style={filterLabel}>Company Names</div>
                <textarea
                  value={filters.companies}
                  onChange={e => updateFilter('companies', e.target.value)}
                  placeholder={'Stripe, Shopify\nZoho, Freshworks'}
                  rows={3}
                  style={{ ...filterTextarea }}
                />
                <div style={filterDesc}>Target specific accounts</div>
              </div>

              {/* 6. Exclusions */}
              <div style={filterCard}>
                <div style={filterLabel}>Exclude</div>
                <textarea
                  value={filters.exclusions}
                  onChange={e => updateFilter('exclusions', e.target.value)}
                  placeholder={'Intern, Student\nFresher, Recruiter'}
                  rows={3}
                  style={{ ...filterTextarea }}
                />
                <div style={filterDesc}>Noise removal → <code style={codeStyle}>NOT ("Intern" OR …)</code></div>
              </div>

              {/* 9. Exact Phrase */}
              <div style={filterCard}>
                <div style={filterLabel}>Exact Phrase</div>
                <input
                  value={filters.exactPhrase}
                  onChange={e => updateFilter('exactPhrase', e.target.value)}
                  placeholder='co-founder at'
                  style={{ ...filterInput }}
                />
                <div style={{ ...filterDesc, marginTop: 6 }}>
                  Wrapped in quotes → <code style={codeStyle}>"co-founder at"</code>
                </div>
              </div>

              {/* 8. Freshness */}
              <div style={filterCard}>
                <div style={filterLabel}>Freshness</div>
                <select value={filters.tbs} onChange={e => updateFilter('tbs', e.target.value)} style={filterSelect}>
                  {TBS_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                </select>
                <div style={{ ...filterDesc, marginTop: 6 }}>When Google indexed the profile</div>
              </div>

            </div>
          </div>

          {/* Options row */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 18 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <label style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-2)', whiteSpace: 'nowrap' }}>Results per query</label>
              <div style={{ display: 'flex', gap: 4 }}>
                {RESULT_OPTIONS.map(n => (
                  <button key={n} onClick={() => setNumResults(n)} style={{
                    padding: '5px 11px', borderRadius: 6, fontSize: 11, fontWeight: 600, cursor: 'pointer',
                    border: numResults === n ? '1px solid rgba(99,102,241,0.5)' : '1px solid var(--border-1)',
                    background: numResults === n ? 'rgba(99,102,241,0.15)' : 'transparent',
                    color: numResults === n ? '#a5b4fc' : 'var(--text-3)',
                  }}>{n}</button>
                ))}
              </div>
              {numResults > 10 && (
                <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
                  ({Math.ceil(numResults / 10)} SerpAPI pages/query)
                </span>
              )}
            </div>
          </div>

          {/* Search button */}
          <button onClick={handleSearch} disabled={searching} style={{
            padding: '9px 22px', borderRadius: 8, border: 'none',
            cursor: searching ? 'not-allowed' : 'pointer',
            background: searching ? 'var(--bg-card)' : '#6366f1',
            color: searching ? 'var(--text-3)' : '#fff',
            fontSize: 13, fontWeight: 600, display: 'flex', alignItems: 'center', gap: 7,
          }}>
            {searching ? <><Spinner /> Searching…</> : <><SearchIcon /> Search LinkedIn</>}
          </button>

          {/* Results */}
          {results.length > 0 && (
            <div style={{ marginTop: 24 }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>
                  {allUrls.length} URLs found
                  <span style={{ fontWeight: 400, color: 'var(--text-3)', marginLeft: 6, fontSize: 12 }}>across {results.length} quer{results.length === 1 ? 'y' : 'ies'}</span>
                </div>
                <div style={{ display: 'flex', gap: 8 }}>
                  <button onClick={toggleAll} style={ghostBtn}>
                    {selected.size === allUrls.length ? 'Deselect All' : `Select All (${allUrls.length})`}
                  </button>
                  <button onClick={handleEnrich} disabled={enriching || !selected.size} style={{
                    ...ghostBtn,
                    ...(selected.size > 0 ? { background: 'rgba(99,102,241,0.15)', color: '#a5b4fc', borderColor: 'rgba(99,102,241,0.4)' } : { opacity: 0.5 }),
                  }}>
                    {enriching ? 'Starting…' : `Enrich Selected (${selected.size})`}
                  </button>
                </div>
              </div>

              {results.map((res, i) => (
                <div key={i} style={{ marginBottom: 14, background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 10, overflow: 'hidden' }}>
                  <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--border-1)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)' }}>{res.query}</div>
                    <div style={{ fontSize: 11, color: res.error ? '#ef4444' : 'var(--text-3)' }}>
                      {res.error || `${res.result_count} results`}
                    </div>
                  </div>
                  {(res.linkedin_urls || []).map((url, j) => (
                    <div key={j} onClick={() => toggleUrl(url)} style={{
                      padding: '9px 14px', display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer',
                      borderBottom: j < res.linkedin_urls.length - 1 ? '1px solid var(--border-1)' : 'none',
                      background: selected.has(url) ? 'rgba(99,102,241,0.06)' : 'transparent',
                    }}>
                      <input type="checkbox" readOnly checked={selected.has(url)} style={{ cursor: 'pointer', accentColor: '#6366f1', flexShrink: 0 }} />
                      <a href={url} target="_blank" rel="noreferrer" onClick={e => e.stopPropagation()}
                        style={{ fontSize: 12, color: '#a5b4fc', textDecoration: 'none', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {url}
                      </a>
                    </div>
                  ))}
                  {!res.linkedin_urls?.length && !res.error && (
                    <div style={{ padding: '12px 14px', fontSize: 12, color: 'var(--text-3)' }}>No results found</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ── HISTORY TAB ─────────────────────────────────────────────────────── */}
      {activeTab === 'history' && (
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
            <div style={{ fontSize: 12, color: 'var(--text-3)' }}>{history.length} saved searches</div>
            <button onClick={loadHistory} style={ghostBtn}>Refresh</button>
          </div>

          {historyLoading && <div style={{ color: 'var(--text-3)', fontSize: 13, padding: 16 }}>Loading…</div>}

          {!historyLoading && !history.length && (
            <div style={{ textAlign: 'center', padding: '48px 0', color: 'var(--text-3)', fontSize: 13 }}>
              No searches yet. Run a search to see history here.
            </div>
          )}

          {history.map(s => {
            const urls     = Array.isArray(s.linkedin_urls) ? s.linkedin_urls : JSON.parse(s.linkedin_urls || '[]')
            const expanded = expandedIds.has(s.id)
            const visible  = expanded ? urls : urls.slice(0, 3)
            const hidden   = urls.length - 3

            return (
              <div key={s.id} style={{ marginBottom: 12, background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 10, overflow: 'hidden' }}>
                <div style={{ padding: '10px 14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: urls.length ? '1px solid var(--border-1)' : 'none' }}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {s.query}
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
                      {s.result_count} URLs · {new Date(s.created_at).toLocaleString()}
                    </div>
                  </div>
                  <div style={{ display: 'flex', gap: 6, flexShrink: 0, marginLeft: 12 }}>
                    <button onClick={() => {
                      setResults([{ query: s.query, linkedin_urls: urls, result_count: s.result_count }])
                      setAllUrls(urls); setSelected(new Set(urls)); setActiveTab('search')
                    }} style={ghostBtn}>Load</button>
                    <button onClick={() => handleDelete(s.id)} style={{ ...ghostBtn, color: '#ef4444', borderColor: 'rgba(239,68,68,0.3)' }}>Delete</button>
                  </div>
                </div>

                {visible.map((url, j) => (
                  <div key={j} style={{ padding: '7px 14px', borderBottom: '1px solid var(--border-1)', display: 'flex', alignItems: 'center', gap: 8 }}>
                    <a href={url} target="_blank" rel="noreferrer"
                      style={{ fontSize: 11, color: '#a5b4fc', textDecoration: 'none', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {url}
                    </a>
                  </div>
                ))}

                {urls.length > 3 && (
                  <button onClick={() => toggleExpand(s.id)} style={{
                    width: '100%', padding: '8px 14px', border: 'none', background: 'transparent',
                    cursor: 'pointer', fontSize: 11, fontWeight: 600,
                    color: expanded ? 'var(--text-3)' : '#a5b4fc',
                    display: 'flex', alignItems: 'center', gap: 5, justifyContent: 'center',
                  }}>
                    <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
                      strokeLinecap="round" strokeLinejoin="round"
                      style={{ transform: expanded ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s' }}>
                      <polyline points="6 9 12 15 18 9"/>
                    </svg>
                    {expanded ? 'Show less' : `Show ${hidden} more URL${hidden !== 1 ? 's' : ''}`}
                  </button>
                )}
              </div>
            )
          })}
        </div>
      )}

      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
    </div>
  )
}

const ghostBtn = {
  padding: '6px 12px', borderRadius: 7, border: '1px solid var(--border-1)',
  background: 'transparent', color: 'var(--text-2)', fontSize: 11, fontWeight: 600, cursor: 'pointer',
}
const filterCard = {
  background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 10,
  padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 4,
}
const filterLabel = { fontSize: 10, fontWeight: 700, color: 'var(--text-2)', textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: 6 }
const filterDesc  = { fontSize: 10, color: 'var(--text-3)', lineHeight: 1.4 }
const filterTextarea = {
  width: '100%', padding: '7px 10px', borderRadius: 7, border: '1px solid var(--border-1)',
  background: 'var(--bg-base)', color: 'var(--text-1)', fontSize: 11, outline: 'none',
  fontFamily: 'inherit', resize: 'none', lineHeight: 1.5, boxSizing: 'border-box',
}
const filterInput = {
  width: '100%', padding: '7px 10px', borderRadius: 7, border: '1px solid var(--border-1)',
  background: 'var(--bg-base)', color: 'var(--text-1)', fontSize: 12, outline: 'none',
  fontFamily: 'inherit', boxSizing: 'border-box',
}
const filterSelect = {
  width: '100%', padding: '7px 10px', borderRadius: 7, border: '1px solid var(--border-1)',
  background: 'var(--bg-base)', color: 'var(--text-1)', fontSize: 12, outline: 'none', cursor: 'pointer',
}
const codeStyle = { fontSize: 9, background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', padding: '1px 4px', borderRadius: 4 }

function SearchIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
    </svg>
  )
}
function Spinner() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
      style={{ animation: 'spin 0.8s linear infinite' }}>
      <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round"/>
    </svg>
  )
}
