import { Zap, Database, CalendarDays, Layers, BarChart2, RefreshCw, User, CheckCircle } from 'lucide-react'
import { Grid, Field, Lbl, TagBadge, CopyBlock } from '../../ui'
import { parseArr } from '../../../utils/leadFormatters'

export default function CrmSection({ lead, full }) {
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
