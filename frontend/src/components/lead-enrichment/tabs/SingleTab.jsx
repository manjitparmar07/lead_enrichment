import { useState, useRef } from 'react'
import {
  Zap, X, CheckCircle, Send, Linkedin, Globe, Users, MapPin,
  Building2, Code2, Mail, Phone, Target, User,
} from 'lucide-react'
import { toast } from 'react-hot-toast'
import { BACKEND, jsonHdr } from '../../../utils/api'
import { STREAM_STAGES } from '../../../utils/leadConfig.jsx'
import { Card, Lbl, Btn, GhostBtn, ErrorBox, ScoreCircle, TierBadge, inputStyle } from '../../ui'
import {
  StageDot, StreamSection, FieldReveal, SkeletonProfile, SkeletonCompany,
  SkeletonContact, SkeletonWebsite, SkeletonScoring, tagStyle, contactRow,
} from '../streaming'
import JsonModal from '../JsonModal'
import LeadFullReport from '../views/LeadFullReport'

export default function SingleTab({ onDone }) {
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
