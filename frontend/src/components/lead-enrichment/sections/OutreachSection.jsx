import { Mail, Clock, TrendingUp, Layers, CalendarDays, CheckCircle } from 'lucide-react'
import { Grid, Field, Lbl, CopyBlock, EmailStatusBadge } from '../../ui'

export default function OutreachSection({ lead, full }) {
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
