import { Building2, Globe, Mail, Phone, Linkedin, Twitter, Users, MapPin, CalendarDays, TrendingUp, BarChart2, Code2, Layers, User } from 'lucide-react'
import { CompanyLogo, Grid, Field, TagList, FundingBadge } from '../../ui'
import { parseArr } from '../../../utils/leadFormatters'

export default function CompanySection({ lead, full }) {
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
