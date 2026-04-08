import { useState } from 'react'
import {
  User, GraduationCap, Building2, Globe, TrendingUp,
  Target, Zap, Send, Database,
} from 'lucide-react'
import { fmtCity, fmtCountry } from '../../../utils/leadFormatters'
import { Avatar, TierBadge, ScoreCircle, CompanyLogo } from '../../ui'
import IdentitySection from '../sections/IdentitySection'
import ProfessionalSection from '../sections/ProfessionalSection'
import CompanySection from '../sections/CompanySection'
import WebsiteIntelSection from '../sections/WebsiteIntelSection'
import IntentSection from '../sections/IntentSection'
import AIInsightsSection from '../sections/AIInsightsSection'
import ScoringSection from '../sections/ScoringSection'
import OutreachSection from '../sections/OutreachSection'
import CrmSection from '../sections/CrmSection'
import CompanyIntelSection from '../sections/CompanyIntelSection'
import AiEnrichmentSection from '../ai/AiEnrichmentSection'

export default function LeadFullReport({ lead, compact }) {
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
