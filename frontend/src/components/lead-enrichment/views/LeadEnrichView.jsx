import { useState, useEffect } from 'react'
import { ENRICH_TABS, RAW_DATA_TABS } from '../../../utils/leadConfig.jsx'
import { BACKEND, jsonHdr } from '../../../utils/api'
import {
  EnrichJsonView, _RegenJsonView, LinkedInEnrichView, OutreachEnrichView, CompanyEnrichView,
} from './EnrichJsonView'

export default function LeadEnrichView({ lead }) {
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
