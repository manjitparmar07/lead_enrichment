import { useState } from 'react'
import { Upload, CheckCircle } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { BACKEND, getToken, jsonHdr } from '../../../utils/api'
import { Card, Lbl, Btn, StatusBadge, RefreshSpin, inputStyle } from '../../ui'

export default function BulkTab({ onDone }) {
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
