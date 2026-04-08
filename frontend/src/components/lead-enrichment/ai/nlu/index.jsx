import { useState } from 'react'
import {
  Copy, Check, ChevronDown, ChevronUp, ArrowRight,
  Mail, Phone, Linkedin, Globe, MessageSquare,
} from 'lucide-react'
import { toast } from 'react-hot-toast'

export function _tryParse(raw) {
  if (!raw) return null
  if (typeof raw === 'object') return raw
  try { return JSON.parse(raw) } catch { return null }
}

export function NluCompanyIntel({ raw }) {
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

export function NluTags({ raw }) {
  const tags = _tryParse(raw)
  if (Array.isArray(tags)) return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: '4px 0' }}>
      {tags.map((t, i) => <span key={i} style={{ padding: '4px 11px', borderRadius: 20, fontSize: 11, fontWeight: 600, background: 'rgba(99,102,241,0.12)', border: '1px solid rgba(99,102,241,0.3)', color: '#a5b4fc' }}>{t}</span>)}
    </div>
  )
  return <pre style={{ fontSize: 11, whiteSpace: 'pre-wrap', color: 'var(--text-2)', margin: 0 }}>{raw}</pre>
}

export function NluSignals({ raw }) {
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

export function NluBuyingSignals({ raw }) {
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

export function NluPitchIntel({ raw }) {
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

export function NluOutreach({ raw }) {
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

export function NluLeadScore({ raw }) {
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

export function AiNluIdentity({ d }) {
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

export function AiNluContact({ d }) {
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

export function AiNluScores({ d }) {
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

export function AiNluIcpMatch({ d }) {
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

export function AiNluBehavioural({ d }) {
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

export function AiNluPitch({ d }) {
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

export function AiNluActivity({ d }) {
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

export function AiNluTags({ d }) {
  if (!d) return null
  const tags = d.tags || []
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: '4px 0' }}>
      {tags.map((t, i) => <span key={i} style={{ padding: '4px 11px', borderRadius: 20, fontSize: 11, fontWeight: 600, background: 'rgba(167,139,250,0.12)', border: '1px solid rgba(167,139,250,0.3)', color: '#c4b5fd' }}>{t}</span>)}
    </div>
  )
}

export function CopyBtn({ text }) {
  const [copied, setCopied] = useState(false)
  return (
    <button onClick={() => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 2000) }}
      style={{ padding: '3px 9px', borderRadius: 5, fontSize: 10, fontWeight: 600, border: '1px solid var(--border-1)', background: copied ? 'rgba(16,185,129,0.1)' : 'transparent', color: copied ? '#10b981' : 'var(--text-3)', cursor: 'pointer', transition: 'all 0.15s', flexShrink: 0 }}>
      {copied ? '✓ Copied' : 'Copy'}
    </button>
  )
}

export function OutreachBlock({ label, icon, color, text }) {
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

export function AiNluOutreach({ d }) {
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

export function AiNluPersona({ d }) {
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
