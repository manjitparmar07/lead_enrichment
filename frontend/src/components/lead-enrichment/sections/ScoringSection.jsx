import { Target, Layers, AlertCircle } from 'lucide-react'
import { Grid, Field } from '../../ui'
import { parseArr } from '../../../utils/leadFormatters'
import { TIER } from '../../../utils/leadConfig.jsx'

export default function ScoringSection({ lead, full }) {
  // Support both old format (full.scoring) and new 8-stage format (full.lead_scoring)
  const s = full.lead_scoring || full.scoring || {}
  const icp     = lead.icp_fit_score             ?? s.icp_fit_score  ?? 0
  const intent  = lead.intent_score              ?? s.intent_score   ?? 0
  const timing  = lead.timing_score              ?? s.timing_score   ?? 0
  const dataCmp = lead.data_completeness_score   ?? s.data_completeness_score ?? 0
  const total   = lead.total_score               ?? s.overall_score  ?? 0
  const tier    = TIER[lead.score_tier] || TIER.cold
  const flags   = parseArr(lead.disqualification_flags || s.disqualification_flags)

  // Waterfall scoring breakdown
  const scoreBreakdown = [
    { label: 'ICP Fit',          score: icp,     max: 40, color: '#6366f1', desc: 'Seniority, title, company size' },
    { label: 'Intent Signals',   score: intent,  max: 30, color: '#f97316', desc: 'Funding, hiring, job change' },
    { label: 'Timing',           score: timing,  max: 20, color: '#10b981', desc: 'Recency of trigger events' },
    { label: 'Data Completeness',score: dataCmp, max: 10, color: '#3b82f6', desc: 'Email, phone, profile richness' },
  ]

  return (
    <div>
      {/* Waterfall score bars */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
          letterSpacing: '0.07em', marginBottom: 10 }}>Score Breakdown (Waterfall)</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {scoreBreakdown.map(({ label, score, max, color, desc }) => (
            <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ width: 140, flexShrink: 0 }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-2)', marginBottom: 1 }}>{label}</div>
                <div style={{ fontSize: 9, color: 'var(--text-3)' }}>{desc}</div>
              </div>
              <div style={{ flex: 1, height: 8, borderRadius: 4, background: 'var(--bg-base)', overflow: 'hidden' }}>
                <div style={{
                  height: '100%', borderRadius: 4, background: color,
                  width: `${Math.min((score / max) * 100, 100)}%`,
                  transition: 'width 0.6s ease',
                }} />
              </div>
              <div style={{ width: 52, textAlign: 'right', flexShrink: 0 }}>
                <span style={{ fontSize: 13, fontWeight: 800, color }}>{score}</span>
                <span style={{ fontSize: 10, color: 'var(--text-3)' }}>/{max}</span>
              </div>
            </div>
          ))}
          {/* Total divider */}
          <div style={{ borderTop: '1px solid var(--border-1)', paddingTop: 10, display: 'flex', alignItems: 'center', gap: 12 }}>
            <div style={{ width: 140, flexShrink: 0 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)' }}>TOTAL SCORE</div>
            </div>
            <div style={{ flex: 1, height: 10, borderRadius: 5, background: 'var(--bg-base)', overflow: 'hidden' }}>
              <div style={{
                height: '100%', borderRadius: 5,
                background: `linear-gradient(90deg,#6366f1,${tier.color})`,
                width: `${Math.min(total, 100)}%`,
                transition: 'width 0.8s ease',
              }} />
            </div>
            <div style={{ width: 52, textAlign: 'right', display: 'flex', alignItems: 'baseline', gap: 3, flexShrink: 0 }}>
              <span style={{ fontSize: 20, fontWeight: 800, color: tier.color }}>{total}</span>
              <span style={{ fontSize: 10, color: 'var(--text-3)' }}>/100</span>
            </div>
          </div>
        </div>
      </div>

      {/* Tier + explanation */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
        <div style={{ padding: '10px 16px', borderRadius: 9, background: tier.bg, border: `1px solid ${tier.border}`, display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 20 }}>{tier.icon}</span>
          <div>
            <div style={{ fontSize: 10, color: tier.color, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Score Tier</div>
            <div style={{ fontSize: 15, fontWeight: 800, color: tier.color }}>{tier.label}</div>
          </div>
        </div>
      </div>

      <Grid>
        <Field icon={<Target size={12} />}  label="Score Explanation"  value={lead.score_explanation || s.score_explanation} wide />
        <Field icon={<Layers size={12} />}  label="ICP Match Tier"     value={lead.icp_match_tier || s.icp_match_tier} />
        <Field icon={<AlertCircle size={12} />} label="Disqualification Flags"
          value={flags.length ? flags.join(', ') : 'None'} />
      </Grid>
    </div>
  )
}
