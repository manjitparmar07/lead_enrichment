export default function AIInsightsSection({ lead }) {
  const tags = (() => { try { return JSON.parse(lead.auto_tags || '[]') } catch { return [] } })()
  const signals = (() => { try { return JSON.parse(lead.behavioural_signals || '{}') } catch { return {} } })()
  const pitch   = (() => { try { return JSON.parse(lead.pitch_intelligence  || '{}') } catch { return {} } })()
  const warm    = lead.warm_signal

  const TAG_COLORS = [
    '#6366f1','#8b5cf6','#ec4899','#f97316','#10b981','#3b82f6','#14b8a6','#f59e0b',
  ]

  const noData = !tags.length && !signals.posts_about && !pitch.top_pain_point
  if (noData) return (
    <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
      AI Insights not yet generated for this lead.<br />
      <span style={{ fontSize: 11 }}>Re-enrich to generate tags, behavioural signals and pitch intelligence.</span>
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* ── ICP Score summary bar ── */}
      {lead.total_score > 0 && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px',
          borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 10, color: 'var(--text-3)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 5 }}>ICP Score</div>
            <div style={{ height: 8, borderRadius: 4, background: 'var(--bg-base)', overflow: 'hidden' }}>
              <div style={{ height: '100%', borderRadius: 4, width: `${Math.min(lead.total_score, 100)}%`,
                background: lead.total_score >= 80 ? '#ef4444' : lead.total_score >= 55 ? '#f97316' : lead.total_score >= 30 ? '#3b82f6' : '#6b7280',
                transition: 'width 0.8s ease' }} />
            </div>
          </div>
          <div style={{ textAlign: 'right', flexShrink: 0 }}>
            <span style={{ fontSize: 22, fontWeight: 800, color: lead.total_score >= 80 ? '#ef4444' : lead.total_score >= 55 ? '#f97316' : '#6366f1' }}>
              {lead.total_score}
            </span>
            <span style={{ fontSize: 11, color: 'var(--text-3)' }}>/100</span>
            {lead.score_tier && (
              <div style={{ fontSize: 10, fontWeight: 700, color: lead.total_score >= 80 ? '#ef4444' : '#f97316',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                {lead.score_tier === 'hot' ? '🔥' : lead.score_tier === 'warm' ? '⚡' : '●'} {lead.score_tier}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── Auto Tags ── */}
      {tags.length > 0 && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
            letterSpacing: '0.07em', marginBottom: 8 }}>Auto-Generated Tags</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {tags.map((tag, i) => (
              <span key={i} style={{
                padding: '4px 10px', borderRadius: 20, fontSize: 11, fontWeight: 600,
                background: TAG_COLORS[i % TAG_COLORS.length] + '22',
                color: TAG_COLORS[i % TAG_COLORS.length],
                border: `1px solid ${TAG_COLORS[i % TAG_COLORS.length]}44`,
              }}>{tag}</span>
            ))}
          </div>
        </div>
      )}

      {/* ── Warm Signal ── */}
      {warm && (
        <div style={{ padding: '10px 14px', borderRadius: 9,
          background: '#fef9c322', border: '1px solid #fde04788',
          display: 'flex', alignItems: 'flex-start', gap: 8 }}>
          <span style={{ fontSize: 16 }}>⚡</span>
          <div>
            <div style={{ fontSize: 10, fontWeight: 700, color: '#d97706', textTransform: 'uppercase',
              letterSpacing: '0.06em', marginBottom: 2 }}>Warm Signal</div>
            <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{warm}</div>
          </div>
        </div>
      )}

      {/* ── Behavioural Signals ── */}
      {signals.posts_about && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
            letterSpacing: '0.07em', marginBottom: 8 }}>Behavioural Signals</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {[
              { key: 'posts_about',        label: 'POSTS ABOUT',      color: '#6366f1' },
              { key: 'engages_with',       label: 'ENGAGES WITH',     color: '#10b981' },
              { key: 'communication_style',label: 'STYLE',            color: '#3b82f6' },
              { key: 'decision_pattern',   label: 'DECISIONS',        color: '#8b5cf6' },
              { key: 'pain_point_hint',    label: 'PAIN POINT',       color: '#f97316' },
            ].filter(({ key }) => signals[key]).map(({ key, label, color }) => (
              <div key={key} style={{ display: 'flex', gap: 10, padding: '8px 12px', borderRadius: 8,
                background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', alignItems: 'flex-start' }}>
                <div style={{ minWidth: 90, fontSize: 9, fontWeight: 700, color, textTransform: 'uppercase',
                  letterSpacing: '0.07em', paddingTop: 1 }}>{label}</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)', flex: 1 }}>{signals[key]}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Pitch Intelligence ── */}
      {pitch.top_pain_point && (
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
            letterSpacing: '0.07em', marginBottom: 8 }}>Pitch Intelligence</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>

            {pitch.top_pain_point && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#ef444411', border: '1px solid #ef444433' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#ef4444', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>🎯 Top Pain Point</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.top_pain_point}</div>
              </div>
            )}

            {pitch.best_value_prop && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#10b98111', border: '1px solid #10b98133' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#10b981', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>✅ Best Value Prop</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.best_value_prop}</div>
              </div>
            )}

            {pitch.best_angle && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#6366f111', border: '1px solid #6366f133' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#6366f1', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>✅ Best Angle</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.best_angle}</div>
              </div>
            )}

            {pitch.suggested_cta && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#3b82f611', border: '1px solid #3b82f633' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#3b82f6', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 3 }}>✅ Suggested CTA</div>
                <div style={{ fontSize: 12, color: 'var(--text-1)' }}>{pitch.suggested_cta}</div>
              </div>
            )}

            {Array.isArray(pitch.do_not_pitch) && pitch.do_not_pitch.length > 0 && (
              <div style={{ padding: '10px 14px', borderRadius: 9,
                background: '#f9731611', border: '1px solid #f9731633' }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: '#f97316', textTransform: 'uppercase',
                  letterSpacing: '0.07em', marginBottom: 6 }}>❌ Do NOT Pitch</div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                  {pitch.do_not_pitch.map((item, i) => (
                    <span key={i} style={{ padding: '3px 8px', borderRadius: 4, fontSize: 11,
                      background: '#f9731622', color: '#f97316', border: '1px solid #f9731644' }}>
                      {item}
                    </span>
                  ))}
                </div>
              </div>
            )}

          </div>
        </div>
      )}

    </div>
  )
}
