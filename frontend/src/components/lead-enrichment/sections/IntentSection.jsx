import { useState } from 'react'
import { TrendingUp, Users, Briefcase, Linkedin, Newspaper, Megaphone, ShoppingCart, MessageSquare, Mail } from 'lucide-react'
import { Grid, Field, ActiveBadge } from '../../ui'

export default function IntentSection({ lead, full }) {
  const i = full.intent_signals || {}
  const activityEmails  = full.activity_emails  || []
  const hiringSignals   = full.hiring_signals   || []
  const activityFull    = full.activity_full    || []
  const linkedinPosts   = full.linkedin_posts   || []
  const [showAllActivity, setShowAllActivity] = useState(false)
  const [showAllPosts, setShowAllPosts] = useState(false)

  const items = [
    { icon: <TrendingUp size={12} />, label: 'Recent Funding Event',  value: lead.recent_funding_event || i.recent_funding_event },
    { icon: <Users size={12} />,      label: 'Hiring Signal',         value: lead.hiring_signal || i.hiring_signal || (hiringSignals[0] || null) },
    { icon: <Briefcase size={12} />,  label: 'Job Change',            value: lead.job_change || i.job_change },
    { icon: <Linkedin size={12} />,   label: 'LinkedIn Activity',     value: lead.linkedin_activity || i.linkedin_activity },
    { icon: <Newspaper size={12} />,  label: 'News Mention',          value: lead.news_mention || i.news_mention },
    { icon: <Megaphone size={12} />,  label: 'Product Launch',        value: lead.product_launch || i.product_launch },
    { icon: <ShoppingCart size={12}/>, label: 'Competitor Usage',     value: lead.competitor_usage || i.competitor_usage },
    { icon: <MessageSquare size={12}/>,label: 'G2 / Review Activity', value: lead.review_activity || i.review_activity },
  ]
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Grid>
        {items.map(({ icon, label, value }) => (
          <Field key={label} icon={icon} label={label} value={value}
            badge={value ? <ActiveBadge /> : null} />
        ))}
      </Grid>

      {/* Activity emails — extracted from LinkedIn post text (high-confidence) */}
      {activityEmails.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(16,185,129,0.05)', border: '1px solid rgba(16,185,129,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#10b981', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>
            Emails Found in LinkedIn Activity Posts
          </div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {activityEmails.map(email => (
              <span key={email} style={{ fontSize: 11, padding: '2px 8px', borderRadius: 8, fontWeight: 600,
                background: 'rgba(16,185,129,0.1)', color: '#10b981', border: '1px solid rgba(16,185,129,0.3)',
                display: 'flex', alignItems: 'center', gap: 4 }}>
                <Mail size={9} /> {email}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Hiring signals — posts where person or company mentioned job openings */}
      {hiringSignals.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(249,115,22,0.05)', border: '1px solid rgba(249,115,22,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#f97316', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>
            Hiring Signals from LinkedIn Posts
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {hiringSignals.map((sig, idx) => (
              <div key={idx} style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.5,
                padding: '4px 0', borderBottom: idx < hiringSignals.length - 1 ? '1px solid var(--border-1)' : 'none' }}>
                {sig}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* LinkedIn Authored Posts (from BD "posts" field) */}
      {linkedinPosts.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <Linkedin size={11} color="#6366f1" />
              <span style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
                LinkedIn Posts ({linkedinPosts.length})
              </span>
            </div>
            {linkedinPosts.length > 4 && (
              <button onClick={() => setShowAllPosts(p => !p)} style={{
                fontSize: 10, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer', padding: 0,
              }}>{showAllPosts ? 'Show less' : `Show all ${linkedinPosts.length}`}</button>
            )}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {(showAllPosts ? linkedinPosts : linkedinPosts.slice(0, 4)).map((post, idx) => (
              <div key={idx} style={{
                padding: '8px 10px', borderRadius: 8,
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, marginBottom: 5 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 5, fontWeight: 600,
                      background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', border: '1px solid rgba(99,102,241,0.25)' }}>
                      Authored Post
                    </span>
                    {post.interaction && (
                      <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{post.interaction}</span>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
                    {post.created_at && (
                      <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
                        {new Date(post.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })}
                      </span>
                    )}
                    {post.link && (
                      <a href={post.link} target="_blank" rel="noreferrer"
                        style={{ fontSize: 9, color: '#6366f1', textDecoration: 'none' }}>
                        View ↗
                      </a>
                    )}
                  </div>
                </div>
                {post.title && (
                  <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-1)', lineHeight: 1.4, marginBottom: post.attribution ? 4 : 0 }}>
                    {post.title}
                  </div>
                )}
                {post.attribution && (
                  <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.6 }}>
                    {post.attribution}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Full LinkedIn Activity Feed (liked/commented/shared) */}
      {activityFull.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
              LinkedIn Activity Feed ({activityFull.length})
            </div>
            {activityFull.length > 5 && (
              <button onClick={() => setShowAllActivity(p => !p)} style={{
                fontSize: 10, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer', padding: 0,
              }}>{showAllActivity ? 'Show less' : `Show all ${activityFull.length}`}</button>
            )}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {(showAllActivity ? activityFull : activityFull.slice(0, 5)).map((item, idx) => (
              <div key={idx} style={{
                padding: '6px 8px', borderRadius: 7,
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, marginBottom: item.title || item.attribution ? 4 : 0 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ fontSize: 9, fontWeight: 600,
                      padding: '1px 6px', borderRadius: 6,
                      background: item.type === 'post' ? 'rgba(99,102,241,0.1)' : 'rgba(107,114,128,0.1)',
                      color: item.type === 'post' ? '#a5b4fc' : 'var(--text-3)',
                      border: `1px solid ${item.type === 'post' ? 'rgba(99,102,241,0.25)' : 'var(--border-1)'}`,
                    }}>
                      {item.interaction || 'Activity'}
                    </span>
                    {item.created_at && (
                      <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
                        {new Date(item.created_at).toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}
                      </span>
                    )}
                  </div>
                  {item.link && (
                    <a href={item.link} target="_blank" rel="noreferrer"
                      style={{ fontSize: 9, color: '#6366f1', textDecoration: 'none', flexShrink: 0 }}>
                      View ↗
                    </a>
                  )}
                </div>
                {item.title && (
                  <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.5, marginBottom: item.attribution ? 3 : 0 }}>
                    {item.title}
                  </div>
                )}
                {item.attribution && (
                  <div style={{ fontSize: 10, color: 'var(--text-3)', lineHeight: 1.5, fontStyle: 'italic' }}>
                    {item.attribution}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
