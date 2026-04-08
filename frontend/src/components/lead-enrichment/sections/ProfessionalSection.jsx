import { Briefcase, Layers, Building2, Clock, Award, GraduationCap, Languages } from 'lucide-react'
import { Grid, Field } from '../../ui'
import { parseArr } from '../../../utils/leadFormatters'

export default function ProfessionalSection({ lead, full }) {
  const p = full.person_profile || full.professional || {}

  // Career history — prefer person_profile.career_history, fall back to professional
  const careerHistory = p.career_history || []

  // Education list — prefer structured list, fall back to DB string
  const eduList = (() => {
    const raw = p.education_list
    if (Array.isArray(raw) && raw.length) return raw
    // Fall back: DB education string → single entry
    const s = lead.education || p.education || ''
    return s ? [{ school: s, degree: '', years: '' }] : []
  })()

  // Skills — all of them
  const skills = parseArr(lead.top_skills || p.top_skills)

  // Certifications
  const certs = parseArr(lead.certifications || p.certifications)

  // Languages
  const langs = parseArr(lead.languages || p.languages)

  // Previous companies
  const prevCos = parseArr(lead.previous_companies || p.previous_companies)

  const SectionHead = ({ icon, title }) => (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8, marginTop: 4 }}>
      <span style={{ color: 'var(--accent)', opacity: 0.7 }}>{icon}</span>
      <span style={{ fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-3)' }}>
        {title}
      </span>
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* ── Basic role info ── */}
      <Grid>
        <Field icon={<Briefcase size={12} />} label="Current Job Title"     value={lead.title} />
        <Field icon={<Layers size={12} />}    label="Seniority Level"        value={lead.seniority_level || p.seniority_level} />
        <Field icon={<Building2 size={12} />} label="Department"             value={lead.department || p.department} />
        <Field icon={<Clock size={12} />}     label="Years in Current Role"  value={lead.years_in_role || p.years_in_role} />
      </Grid>

      {/* ── Career Timeline ── */}
      {careerHistory.length > 0 && (
        <div>
          <SectionHead icon={<Briefcase size={12} />} title="Career History" />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {careerHistory.map((job, i) => (
              <div key={i} style={{
                padding: '10px 14px',
                borderRadius: 8,
                background: 'var(--bg-card)',
                border: '1px solid var(--border-1)',
                display: 'flex', alignItems: 'flex-start', gap: 10,
              }}>
                <div style={{
                  width: 32, height: 32, borderRadius: 8, flexShrink: 0,
                  background: i === 0 ? 'rgba(99,102,241,0.12)' : 'var(--bg-base)',
                  border: '1px solid var(--border-1)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>
                  <Building2 size={14} color={i === 0 ? '#6366f1' : 'var(--text-3)'} />
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 2 }}>
                    {job.title || '—'}
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--text-2)' }}>{job.company || ''}</div>
                  {job.duration && (
                    <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
                      <Clock size={10} style={{ display: 'inline', marginRight: 3, verticalAlign: 'middle' }} />
                      {job.duration}
                    </div>
                  )}
                </div>
                {i === 0 && (
                  <span style={{
                    fontSize: 10, fontWeight: 600, padding: '2px 7px', borderRadius: 5,
                    background: 'rgba(99,102,241,0.12)', color: '#6366f1', flexShrink: 0,
                  }}>Current</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Previous Companies (when no career history) ── */}
      {careerHistory.length === 0 && prevCos.length > 0 && (
        <div>
          <SectionHead icon={<Building2 size={12} />} title="Previous Companies" />
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {prevCos.map((co, i) => (
              <span key={i} style={{
                fontSize: 12, padding: '4px 10px', borderRadius: 6,
                background: 'var(--bg-card)', border: '1px solid var(--border-1)',
                color: 'var(--text-2)',
              }}>{co}</span>
            ))}
          </div>
        </div>
      )}

      {/* ── Skills ── */}
      {skills.length > 0 && (
        <div>
          <SectionHead icon={<Award size={12} />} title={`Top Skills (${skills.length})`} />
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
            {skills.map((sk, i) => (
              <span key={i} style={{
                fontSize: 12, padding: '4px 10px', borderRadius: 6, fontWeight: 500,
                background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.22)',
                color: '#6366f1',
              }}>{sk}</span>
            ))}
          </div>
        </div>
      )}

      {/* ── Education ── */}
      {eduList.length > 0 && (
        <div>
          <SectionHead icon={<GraduationCap size={12} />} title={`Education (${eduList.length})`} />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {eduList.map((e, i) => (
              <div key={i} style={{
                padding: '10px 14px', borderRadius: 8,
                background: 'var(--bg-card)', border: '1px solid var(--border-1)',
                display: 'flex', gap: 10, alignItems: 'flex-start',
              }}>
                <div style={{
                  width: 30, height: 30, borderRadius: 7, flexShrink: 0,
                  background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.2)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>
                  <GraduationCap size={14} color="#10b981" />
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 1 }}>
                    {e.school || e.institution || '—'}
                  </div>
                  {(e.degree || e.field_of_study) && (
                    <div style={{ fontSize: 12, color: 'var(--text-2)' }}>
                      {[e.degree, e.field_of_study].filter(Boolean).join(' · ')}
                    </div>
                  )}
                  {e.years && (
                    <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>{e.years}</div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Certifications ── */}
      {certs.length > 0 && (
        <div>
          <SectionHead icon={<Award size={12} />} title={`Certifications (${certs.length})`} />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {certs.map((cert, i) => {
              const certName   = typeof cert === 'object' ? (cert.name  || cert.title || '') : cert
              const certIssuer = typeof cert === 'object' ? (cert.issuer || '') : ''
              const certDate   = typeof cert === 'object' ? (cert.date  || '') : ''
              const certUrl    = typeof cert === 'object' ? (cert.credential_url || '') : ''
              return (
                <div key={i} style={{
                  padding: '9px 12px', borderRadius: 8,
                  background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.2)',
                  display: 'flex', alignItems: 'flex-start', gap: 9,
                }}>
                  <Award size={13} color="#f59e0b" style={{ flexShrink: 0, marginTop: 1 }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 12, color: 'var(--text-1)', fontWeight: 600 }}>
                      {certUrl
                        ? <a href={certUrl} target="_blank" rel="noreferrer"
                            style={{ color: '#f59e0b', textDecoration: 'none' }}>{certName}</a>
                        : certName}
                    </div>
                    {certIssuer && (
                      <div style={{ fontSize: 11, color: 'var(--text-2)', marginTop: 1 }}>{certIssuer}</div>
                    )}
                    {certDate && (
                      <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 1 }}>{certDate}</div>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* ── Languages ── */}
      {langs.length > 0 && (
        <div>
          <SectionHead icon={<Languages size={12} />} title={`Languages (${langs.length})`} />
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {langs.map((lang, i) => {
              const langName  = typeof lang === 'object' ? (lang.name || lang.language || lang.title || String(lang)) : lang
              const langLevel = typeof lang === 'object' ? (lang.proficiency || lang.subtitle || lang.level || '') : ''
              return (
                <div key={i} style={{
                  padding: '6px 12px', borderRadius: 8, display: 'flex', flexDirection: 'column',
                  background: 'rgba(59,130,246,0.08)', border: '1px solid rgba(59,130,246,0.22)',
                  minWidth: 80,
                }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: '#3b82f6', display: 'flex', alignItems: 'center', gap: 4 }}>
                    <Languages size={11} />
                    {langName}
                  </div>
                  {langLevel && (
                    <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2 }}>{langLevel}</div>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* ── Empty state ── */}
      {careerHistory.length === 0 && skills.length === 0 && eduList.length === 0 && certs.length === 0 && langs.length === 0 && (
        <div style={{ textAlign: 'center', padding: '24px 0', color: 'var(--text-3)', fontSize: 13 }}>
          No professional data available for this lead.
        </div>
      )}
    </div>
  )
}
