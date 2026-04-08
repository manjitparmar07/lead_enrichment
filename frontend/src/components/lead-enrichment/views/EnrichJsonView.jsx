import { useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { BACKEND, jsonHdr } from '../../../utils/api'

export function EnrichJsonView({ data }) {
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


export function _RegenJsonView({ data, leadenrich_id, endpoint, viewTab, onRegenerated, accentColor, directFetch }) {
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

export function LinkedInEnrichView({ data, leadenrich_id, onRegenerated }) {
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

export function OutreachEnrichView({ data, leadenrich_id, onRegenerated }) {
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

export function CompanyEnrichView({ data, leadenrich_id, onRegenerated }) {
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
