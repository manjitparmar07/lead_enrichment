import { useState } from 'react'
import { User, Mail, Phone, Linkedin, Twitter, Globe, MapPin, Clock, AlertCircle } from 'lucide-react'
import { Avatar, Grid, Field, ConfBadge } from '../../ui'
import { fmtCity, fmtCountry } from '../../../utils/leadFormatters'

export default function IdentitySection({ lead, full }) {
  const id = full.person_profile || full.identity || {}
  const bannerImg          = full.banner_image || ''
  const recommendations    = full.recommendations || []
  const recsCount          = full.recommendations_count || recommendations.length
  const similarProfiles    = full.similar_profiles || []
  const activityPhones     = full.activity_phones  || []
  const linkedinNumId      = full.linkedin_num_id  || ''
  const isInfluencer       = full.influencer || false
  const isMemorialized     = full.memorialized_account || false
  const bioLinks           = full.bio_links || []
  const bdScrapeTime       = full.bd_scrape_timestamp || ''
  const [showAllProfiles, setShowAllProfiles] = useState(false)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      {/* Memorialized account warning */}
      {isMemorialized && (
        <div style={{ padding: '8px 14px', borderRadius: 9, background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.25)', display: 'flex', alignItems: 'center', gap: 8 }}>
          <AlertCircle size={13} color="#ef4444" />
          <span style={{ fontSize: 11, color: '#ef4444', fontWeight: 600 }}>This is a memorialized LinkedIn account</span>
        </div>
      )}

      {/* Banner + Avatar hero */}
      {(lead.avatar_url || bannerImg) && (
        <div style={{ position: 'relative', borderRadius: 10, overflow: 'hidden', border: '1px solid var(--border-1)' }}>
          {bannerImg && (
            <div style={{ height: 80, overflow: 'hidden' }}>
              <img src={bannerImg} alt="" referrerPolicy="no-referrer"
                style={{ width: '100%', objectFit: 'cover', display: 'block' }}
                onError={e => { e.target.style.display = 'none' }} />
            </div>
          )}
          <div style={{ padding: bannerImg ? '8px 14px 12px' : '12px 14px', display: 'flex', alignItems: 'center', gap: 14 }}>
            <Avatar name={lead.name} tier={lead.score_tier} src={lead.avatar_url} size={72} />
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
                <span style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-1)' }}>{lead.name}</span>
                {isInfluencer && (
                  <span style={{ fontSize: 9, padding: '1px 7px', borderRadius: 10, fontWeight: 700,
                    background: 'rgba(251,191,36,0.15)', color: '#f59e0b', border: '1px solid rgba(251,191,36,0.35)' }}>
                    INFLUENCER
                  </span>
                )}
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-3)', marginTop: 3 }}>{lead.title}</div>
              <div style={{ display: 'flex', gap: 12, marginTop: 4, fontSize: 11, color: 'var(--text-3)' }}>
                {lead.followers > 0 && <span>{lead.followers.toLocaleString()} followers</span>}
                {lead.connections > 0 && <span>{lead.connections.toLocaleString()} connections</span>}
                {recsCount > 0 && <span>{recsCount} recommendations</span>}
              </div>
            </div>
          </div>
        </div>
      )}

      <Grid>
        <Field icon={<User size={12} />}     label="Full Name"      value={lead.name} />
        <Field icon={<Mail size={12} />}     label="Work Email"     value={lead.work_email}
          badge={lead.email_confidence ? <ConfBadge c={lead.email_confidence} src={lead.email_source} /> : null}
          copyable />
        <Field icon={<Mail size={12} />}     label="Personal Email" value={id.personal_email} />
        <Field icon={<Phone size={12} />}    label="Direct Phone"   value={lead.direct_phone || id.direct_phone} copyable />
        <Field icon={<Linkedin size={12} />} label="LinkedIn URL"   value={lead.linkedin_url}
          link={lead.linkedin_url} copyable />
        {linkedinNumId && <Field icon={<Linkedin size={12} />} label="LinkedIn Member ID" value={linkedinNumId} copyable />}
        <Field icon={<Twitter size={12} />}  label="Twitter / X"    value={lead.twitter || id.twitter} />
        <Field icon={<MapPin size={12} />}   label="City"           value={fmtCity(lead.city || id.city)} />
        <Field icon={<Globe size={12} />}    label="Country"        value={fmtCountry(lead.country || id.country)} />
        <Field icon={<Clock size={12} />}    label="Time Zone"      value={lead.timezone || id.timezone} />
        {bdScrapeTime && <Field icon={<Clock size={12} />} label="BD Scraped At" value={new Date(bdScrapeTime).toLocaleString()} />}
      </Grid>

      {/* Bio links */}
      {bioLinks.length > 0 && (
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {bioLinks.map((link, idx) => {
            const href = typeof link === 'string' ? link : (link.url || link.link || '')
            const label = typeof link === 'string' ? link : (link.label || link.title || href)
            return (
              <a key={idx} href={href} target="_blank" rel="noreferrer"
                style={{ fontSize: 11, padding: '2px 9px', borderRadius: 8, fontWeight: 500,
                  background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
                  color: '#6366f1', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 4 }}>
                <Globe size={9} /> {label}
              </a>
            )
          })}
        </div>
      )}

      {/* Phones found in LinkedIn activity posts */}
      {activityPhones.length > 0 && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(99,102,241,0.05)', border: '1px solid rgba(99,102,241,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>
            Phone Numbers Found in LinkedIn Posts
          </div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {activityPhones.map(ph => (
              <span key={ph} style={{ fontSize: 11, padding: '2px 8px', borderRadius: 8, fontWeight: 600,
                background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', border: '1px solid rgba(99,102,241,0.3)',
                display: 'flex', alignItems: 'center', gap: 4 }}>
                <Phone size={9} /> {ph}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* About / bio */}
      {lead.about && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>About</div>
          <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.7 }}>{lead.about}</div>
        </div>
      )}

      {/* Recommendations */}
      {(recommendations.length > 0 || recsCount > 0) && (
        <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <span style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
              LinkedIn Recommendations
            </span>
            <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
              {recommendations.length > 0 && recsCount > recommendations.length
                ? `${recommendations.length} shown · ${recsCount} total`
                : `${recsCount || recommendations.length} total`}
            </span>
          </div>
          {recommendations.length > 0 ? recommendations.map((rec, idx) => {
            const text = typeof rec === 'string' ? rec : (rec.text || rec.description || '')
            const author = typeof rec === 'object' ? (rec.recommender_name || rec.author_name || '') : ''
            const headline = typeof rec === 'object' ? (rec.recommender_headline || rec.author_headline || '') : ''
            const url = typeof rec === 'object' ? (rec.recommender_url || '') : ''
            const date = typeof rec === 'object' ? (rec.date || '') : ''
            return (
              <div key={idx} style={{ paddingBottom: idx < recommendations.length - 1 ? 10 : 0,
                borderBottom: idx < recommendations.length - 1 ? '1px solid var(--border-1)' : 'none',
                marginBottom: idx < recommendations.length - 1 ? 10 : 0 }}>
                {(author || headline) && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                    <User size={11} color="#6366f1" />
                    <div>
                      {author && (
                        url
                          ? <a href={url} target="_blank" rel="noreferrer"
                              style={{ fontSize: 11, fontWeight: 700, color: '#6366f1', textDecoration: 'none' }}>
                              {author}
                            </a>
                          : <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-1)' }}>{author}</span>
                      )}
                      {headline && <span style={{ fontSize: 10, color: 'var(--text-3)', marginLeft: 5 }}>{headline}</span>}
                    </div>
                    {date && <span style={{ fontSize: 10, color: 'var(--text-3)', marginLeft: 'auto' }}>{date}</span>}
                  </div>
                )}
                {text && (
                  <div style={{ fontSize: 11, color: 'var(--text-2)', lineHeight: 1.6, fontStyle: 'italic' }}>
                    "{text}"
                  </div>
                )}
              </div>
            )
          }) : (
            <div style={{ fontSize: 11, color: 'var(--text-3)', fontStyle: 'italic' }}>
              {recsCount} recommendation{recsCount !== 1 ? 's' : ''} on LinkedIn (text not available in this dataset)
            </div>
          )}
        </div>
      )}

      {/* People also viewed */}
      {similarProfiles.length > 0 && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
              People Also Viewed ({similarProfiles.length})
            </div>
            {similarProfiles.length > 8 && (
              <button onClick={() => setShowAllProfiles(p => !p)} style={{
                fontSize: 10, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer', padding: 0,
              }}>{showAllProfiles ? 'Show less' : `Show all ${similarProfiles.length}`}</button>
            )}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
            {(showAllProfiles ? similarProfiles : similarProfiles.slice(0, 8)).map((sp, idx) => (
              <a key={idx} href={sp.link || sp.url || '#'} target="_blank" rel="noreferrer"
                style={{ padding: '7px 10px', borderRadius: 8, fontSize: 11,
                  background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
                  color: 'var(--text-1)', textDecoration: 'none',
                  display: 'flex', alignItems: 'center', gap: 8 }}>
                {sp.image
                  ? <img src={sp.image} alt="" referrerPolicy="no-referrer"
                      style={{ width: 28, height: 28, borderRadius: '50%', objectFit: 'cover', flexShrink: 0,
                        border: '1px solid var(--border-1)', background: 'var(--bg-base)' }}
                      onError={e => { e.target.style.display = 'none' }} />
                  : <div style={{ width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
                      background: 'rgba(99,102,241,0.12)', display: 'flex', alignItems: 'center',
                      justifyContent: 'center' }}>
                      <User size={12} color="#6366f1" />
                    </div>
                }
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 600, color: '#6366f1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {sp.name || 'Profile'}
                  </div>
                  {(sp.title || sp.about) && (
                    <div style={{ fontSize: 10, color: 'var(--text-3)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {sp.title || sp.about}
                    </div>
                  )}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2, flexShrink: 0 }}>
                  {sp.degree && (
                    <span style={{ fontSize: 9, padding: '1px 5px', borderRadius: 5,
                      background: 'rgba(99,102,241,0.1)', color: '#6366f1', fontWeight: 600 }}>
                      {sp.degree}
                    </span>
                  )}
                  {sp.location && <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{fmtCity(sp.location)}</span>}
                </div>
              </a>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
