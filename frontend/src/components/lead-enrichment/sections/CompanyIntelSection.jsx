export default function CompanyIntelSection({ lead }) {
  const companyTags    = (() => { try { return JSON.parse(lead.company_tags    || '[]') } catch { return [] } })()
  const cultureSigs    = (() => { try { return JSON.parse(lead.culture_signals || '{}') } catch { return {} } })()
  const accountPitch   = (() => { try { return JSON.parse(lead.account_pitch   || '{}') } catch { return {} } })()
  const wapTech        = (() => { try { return JSON.parse(lead.wappalyzer_tech || '[]') } catch { return [] } })()
  const newsMentions   = (() => { try { return JSON.parse(lead.news_mentions   || '[]') } catch { return [] } })()
  const cbData         = (() => { try { return JSON.parse(lead.crunchbase_data || '{}') } catch { return {} } })()
  const linkedinPosts  = (() => { try { return JSON.parse(lead.linkedin_posts  || '[]') } catch { return [] } })()

  const companyScore = lead.company_score || 0
  const combinedScore = lead.combined_score || lead.total_score || 0
  const scoreTier = lead.company_score_tier || 'C'

  const TAG_COLORS = ['#6366f1','#8b5cf6','#10b981','#f97316','#3b82f6','#ec4899','#14b8a6','#f59e0b','#ef4444','#84cc16']

  const noData = !companyTags.length && !cultureSigs.growth_stage && !accountPitch.top_account_pain && !wapTech.length

  if (noData) return (
    <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-3)', fontSize: 13 }}>
      Company Intelligence not yet generated.<br />
      <span style={{ fontSize: 11 }}>Re-enrich this lead to run company enrichment (posts, tech stack, news, AI analysis).</span>
    </div>
  )

  const card = (children, extra = {}) => (
    <div style={{ borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', padding: '14px 16px', ...extra }}>
      {children}
    </div>
  )
  const label = (text) => (
    <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-3)', marginBottom: 8 }}>{text}</div>
  )
  const row = (k, v) => v ? (
    <div key={k} style={{ display: 'flex', gap: 8, fontSize: 12, marginBottom: 5 }}>
      <span style={{ color: 'var(--text-3)', flexShrink: 0, minWidth: 120 }}>{k}</span>
      <span style={{ color: 'var(--text-1)', fontWeight: 500 }}>{v}</span>
    </div>
  ) : null

  const tierColor = { 'A+': '#ef4444', A: '#f97316', B: '#3b82f6', C: '#6b7280' }[scoreTier] || '#6b7280'

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>

      {/* ── Score bar ── */}
      {(companyScore > 0 || combinedScore > 0) && card(
        <>
          <div style={{ display: 'flex', gap: 20, alignItems: 'center' }}>
            <div style={{ flex: 1 }}>
              {label('Company Score')}
              <div style={{ height: 8, borderRadius: 4, background: 'var(--bg-base)', overflow: 'hidden', marginBottom: 4 }}>
                <div style={{ height: '100%', borderRadius: 4, width: `${Math.min(companyScore, 100)}%`,
                  background: tierColor, transition: 'width 0.8s ease' }} />
              </div>
              <div style={{ fontSize: 10, color: 'var(--text-3)' }}>Funding · Tech · News · Posts · ICP Size</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 26, fontWeight: 800, color: tierColor, lineHeight: 1 }}>{companyScore}</div>
              <div style={{ fontSize: 10, color: 'var(--text-3)' }}>/100</div>
              <div style={{ fontSize: 11, fontWeight: 700, color: tierColor, marginTop: 2 }}>Tier {scoreTier}</div>
            </div>
            {combinedScore > 0 && (
              <div style={{ textAlign: 'center', paddingLeft: 16, borderLeft: '1px solid var(--border-1)' }}>
                <div style={{ fontSize: 10, color: 'var(--text-3)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 2 }}>Combined</div>
                <div style={{ fontSize: 26, fontWeight: 800,
                  color: combinedScore >= 90 ? '#ef4444' : combinedScore >= 70 ? '#f97316' : '#6366f1', lineHeight: 1 }}>
                  {combinedScore}
                </div>
                <div style={{ fontSize: 10, color: 'var(--text-3)' }}>/100</div>
                {combinedScore >= 90 && <div style={{ fontSize: 10, fontWeight: 700, color: '#ef4444', marginTop: 2 }}>Top 1%</div>}
              </div>
            )}
          </div>
        </>
      )}

      {/* ── Company Tags ── */}
      {companyTags.length > 0 && card(
        <>
          {label('Account Tags')}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {companyTags.map((tag, i) => (
              <span key={i} style={{
                fontSize: 11, fontWeight: 600, padding: '3px 9px', borderRadius: 20,
                background: TAG_COLORS[i % TAG_COLORS.length] + '18',
                color: TAG_COLORS[i % TAG_COLORS.length],
                border: `1px solid ${TAG_COLORS[i % TAG_COLORS.length]}40`,
              }}>{tag}</span>
            ))}
          </div>
        </>
      )}

      {/* ── Culture & Growth Signals ── */}
      {Object.keys(cultureSigs).length > 0 && card(
        <>
          {label('Culture & Growth Signals')}
          {row('Growth Stage',        cultureSigs.growth_stage)}
          {row('Tech Maturity',       cultureSigs.tech_maturity)}
          {row('Hiring Velocity',     cultureSigs.hiring_velocity)}
          {row('Content Themes',      cultureSigs.content_themes)}
          {row('Community Presence',  cultureSigs.community_presence)}
        </>
      )}

      {/* ── Account-Level Pitch ── */}
      {Object.keys(accountPitch).length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          {[
            { key: 'top_account_pain',    label: 'Account Pain',     color: '#ef4444' },
            { key: 'best_value_prop',     label: 'Value Prop',       color: '#10b981' },
            { key: 'executive_objection', label: 'Exec Objection',   color: '#f97316' },
            { key: 'best_angle',          label: 'Best Angle',       color: '#8b5cf6' },
            { key: 'account_cta',         label: 'Account CTA',      color: '#3b82f6' },
          ].filter(f => accountPitch[f.key]).map(f => (
            <div key={f.key} style={{
              borderRadius: 9, padding: '12px 14px',
              background: f.color + '0f', border: `1px solid ${f.color}30`,
            }}>
              <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: f.color, marginBottom: 5 }}>{f.label}</div>
              <div style={{ fontSize: 12, color: 'var(--text-1)', lineHeight: 1.5 }}>{accountPitch[f.key]}</div>
            </div>
          ))}
        </div>
      )}

      {/* ── Crunchbase Funding ── */}
      {(cbData.funding_stage || cbData.total_funding || cbData.investors?.length) && card(
        <>
          {label('Crunchbase Signals')}
          {row('Funding Stage',    cbData.funding_stage)}
          {row('Total Funding',    cbData.total_funding)}
          {row('Last Round',       cbData.last_funding_date)}
          {row('Lead Investors',   cbData.investors?.slice(0,3).join(', '))}
          {row('Employee Range',   cbData.employee_range)}
        </>
      )}

      {/* ── Wappalyzer Tech Stack ── */}
      {wapTech.length > 0 && card(
        <>
          {label(`Tech Stack (${wapTech.length} detected)`)}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
            {wapTech.map((tech, i) => (
              <span key={i} style={{
                fontSize: 11, padding: '2px 8px', borderRadius: 5,
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
                color: 'var(--text-2)', fontFamily: 'monospace',
              }}>{tech}</span>
            ))}
          </div>
        </>
      )}

      {/* ── Google News ── */}
      {newsMentions.length > 0 && card(
        <>
          {label(`Recent News (${newsMentions.length} mentions)`)}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {newsMentions.slice(0, 5).map((n, i) => (
              <div key={i} style={{ fontSize: 12, borderBottom: i < newsMentions.length - 1 && i < 4 ? '1px solid var(--border-1)' : 'none', paddingBottom: 7 }}>
                <div style={{ color: 'var(--text-1)', fontWeight: 500, marginBottom: 2 }}>{n.title}</div>
                <div style={{ display: 'flex', gap: 10, color: 'var(--text-3)', fontSize: 11 }}>
                  {n.source && <span>{n.source}</span>}
                  {n.date && <span>{n.date.slice(0, 16)}</span>}
                  {n.url && <a href={n.url} target="_blank" rel="noreferrer" style={{ color: '#6366f1' }}>View →</a>}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── LinkedIn Company Posts ── */}
      {linkedinPosts.length > 0 && card(
        <>
          {label(`Company Posts (${linkedinPosts.length})`)}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {linkedinPosts.slice(0, 5).map((p, i) => (
              <div key={i} style={{ fontSize: 12, borderBottom: i < 4 ? '1px solid var(--border-1)' : 'none', paddingBottom: 8 }}>
                <div style={{ color: 'var(--text-1)', lineHeight: 1.5, marginBottom: 3 }}>{p.text?.slice(0, 200)}{p.text?.length > 200 ? '…' : ''}</div>
                <div style={{ display: 'flex', gap: 10, color: 'var(--text-3)', fontSize: 11 }}>
                  {p.likes > 0 && <span>👍 {p.likes}</span>}
                  {p.comments > 0 && <span>💬 {p.comments}</span>}
                  {p.date && <span>{String(p.date).slice(0, 10)}</span>}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

    </div>
  )
}
