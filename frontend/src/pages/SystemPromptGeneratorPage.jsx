// SystemPromptGeneratorPage.jsx
// Phase 1: Enter URL → Scrape → show profile data
// Phase 2: Click "Generate Prompts" → AI writes system prompt per section
import { useState, useRef, useCallback, useEffect } from 'react'
import toast from 'react-hot-toast'

const API = import.meta.env.VITE_BACKEND_URL || 'https://leadenrichment-production-5b78.up.railway.app'

function authHeader() {
  const token = localStorage.getItem('wb_ai_token')
  return token ? { Authorization: `Bearer ${token}` } : {}
}

// ── Section metadata ──────────────────────────────────────────────────────────

const SECTIONS = [
  { key: 'identity',            label: 'Identity',            icon: '👤', color: '#3b82f6', desc: 'Name, title, company, location' },
  { key: 'contact',             label: 'Contact',             icon: '📧', color: '#06b6d4', desc: 'Email, phone, social links' },
  { key: 'scores',              label: 'Scores',              icon: '📊', color: '#f59e0b', desc: 'ICP fit, intent, timing, total' },
  { key: 'icp_match',           label: 'ICP Match',           icon: '🎯', color: '#8b5cf6', desc: 'Fit analysis, match tier, gaps' },
  { key: 'behavioural_signals', label: 'Behavioural Signal',  icon: '🧠', color: '#ec4899', desc: 'Funding, hiring, job change signals' },
  { key: 'pitch_intelligence',  label: 'Pitch Intelligence',  icon: '⚡', color: '#f97316', desc: 'Pain points, value props, angles' },
  { key: 'activity',            label: 'Activity',            icon: '📡', color: '#14b8a6', desc: 'Recent posts, interactions, themes' },
  { key: 'tags',                label: 'Tags',                icon: '🏷',  color: '#a855f7', desc: 'Auto tags, persona tags, intent tags' },
  { key: 'outreach',            label: 'Outreach',            icon: '✉',  color: '#ef4444', desc: 'Cold email, LinkedIn note, sequence' },
  { key: 'person_analysis',     label: 'Person Analysis',     icon: '🔍', color: '#84cc16', desc: 'Personality, style, motivations' },
]

// ── Profile preview helpers ────────────────────────────────────────────────────

function ProfileField({ label, value }) {
  if (!value) return null
  return (
    <div style={{ marginBottom: 6 }}>
      <span style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        {label}
      </span>
      <div style={{ fontSize: 12, color: 'var(--text-1)', marginTop: 2 }}>
        {Array.isArray(value)
          ? <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginTop: 4 }}>
              {value.slice(0, 8).map((v, i) => (
                <span key={i} style={{
                  fontSize: 11, padding: '2px 7px', borderRadius: 10,
                  background: 'rgba(99,102,241,0.1)', color: '#a5b4fc',
                  border: '1px solid rgba(99,102,241,0.2)',
                }}>{typeof v === 'string' ? v : JSON.stringify(v)}</span>
              ))}
              {value.length > 8 && <span style={{ fontSize: 11, color: 'var(--text-3)' }}>+{value.length - 8} more</span>}
            </div>
          : String(value)}
      </div>
    </div>
  )
}

function TagList({ items, color = '#a5b4fc', bg = 'rgba(99,102,241,0.1)', border = 'rgba(99,102,241,0.2)' }) {
  if (!items || !Array.isArray(items) || items.length === 0) return null
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginTop: 4 }}>
      {items.slice(0, 10).map((v, i) => (
        <span key={i} style={{
          fontSize: 11, padding: '2px 8px', borderRadius: 10,
          background: bg, color, border: `1px solid ${border}`,
        }}>{typeof v === 'string' ? v : JSON.stringify(v)}</span>
      ))}
      {items.length > 10 && <span style={{ fontSize: 11, color: 'var(--text-3)', alignSelf: 'center' }}>+{items.length - 10} more</span>}
    </div>
  )
}

function SectionBlock({ label, children }) {
  return (
    <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid var(--border-1)' }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
        {label}
      </div>
      {children}
    </div>
  )
}

function ProfilePreview({ profile, sourceType }) {
  const li = sourceType === 'linkedin'

  if (li) {
    // ── LinkedIn profile view ──────────────────────────────────────────────────
    const name        = profile.name || profile.full_name || profile.title
    const title       = profile.headline || profile.position || profile.current_title
    const company     = profile.current_company?.name || profile.company || profile.organization
    const location    = profile.location || profile.city
    const about       = profile.about || profile.summary || profile.description
    const email       = profile.work_email || profile.email
    const phone       = profile.direct_phone || profile.phone
    const skills      = profile.skills
    const connections = profile.connections || profile.followers
    const education   = profile.education

    return (
      <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 14, padding: 20, marginBottom: 24 }}>
        <div style={{ display: 'flex', gap: 14, alignItems: 'flex-start', marginBottom: 16 }}>
          <div style={{ width: 48, height: 48, borderRadius: '50%', flexShrink: 0, background: 'linear-gradient(135deg,#0a66c2,#60a5fa)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 18, fontWeight: 700, color: '#fff' }}>
            {(name || 'L')[0].toUpperCase()}
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-1)' }}>{name || '—'}</div>
            <div style={{ fontSize: 13, color: 'var(--text-2)', marginTop: 2 }}>{[title, company].filter(Boolean).join(' · ')}</div>
            {location    && <div style={{ fontSize: 12, color: 'var(--text-3)', marginTop: 3 }}>📍 {location}</div>}
            {connections && <div style={{ fontSize: 12, color: 'var(--text-3)', marginTop: 2 }}>🔗 {connections} connections</div>}
          </div>
          <div style={{ fontSize: 10, fontWeight: 700, padding: '3px 8px', borderRadius: 6, background: 'rgba(10,102,194,0.12)', color: '#60a5fa', flexShrink: 0 }}>LinkedIn</div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0 24px' }}>
          <ProfileField label="Email" value={email} />
          <ProfileField label="Phone" value={phone} />
          <ProfileField label="Education" value={education && Array.isArray(education) ? education.slice(0, 2).map(e => e.school || e.name || JSON.stringify(e)).join(', ') : null} />
        </div>

        {about && (
          <SectionBlock label="About">
            <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.7, maxHeight: 80, overflow: 'hidden' }}>
              {String(about).slice(0, 400)}{String(about).length > 400 ? '…' : ''}
            </div>
          </SectionBlock>
        )}
        {skills && Array.isArray(skills) && skills.length > 0 && (
          <SectionBlock label="Skills">
            <TagList items={skills.map(s => typeof s === 'string' ? s : s.name || s.skill || JSON.stringify(s))} />
          </SectionBlock>
        )}

        <DataFooter count={Object.keys(profile).length} />
      </div>
    )
  }

  // ── Company / Website view ─────────────────────────────────────────────────
  const name           = profile.company_name || profile.title || profile.name
  const tagline        = profile.tagline || profile.description
  const about          = profile.about_text || profile.homepage_text?.slice(0, 300)
  const industry       = profile.industry
  const location       = profile.location
  const companySize    = profile.company_size
  const foundedYear    = profile.founded_year
  const targetAudience = profile.target_audience
  const products       = profile.products
  const services       = profile.services
  const keyFeatures    = profile.key_features
  const socialLinks    = profile.social_links && typeof profile.social_links === 'object'
    ? Object.entries(profile.social_links).filter(([, v]) => v).map(([k, v]) => `${k}: ${v}`)
    : null

  return (
    <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 14, padding: 20, marginBottom: 24 }}>

      {/* Header */}
      <div style={{ display: 'flex', gap: 14, alignItems: 'flex-start', marginBottom: 16 }}>
        <div style={{ width: 48, height: 48, borderRadius: 12, flexShrink: 0, background: 'linear-gradient(135deg,#10b981,#059669)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 18, fontWeight: 700, color: '#fff' }}>
          {(name || 'W')[0].toUpperCase()}
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-1)' }}>{name || profile.url || '—'}</div>
          {tagline && <div style={{ fontSize: 13, color: 'var(--text-2)', marginTop: 2, lineHeight: 1.4 }}>{String(tagline).slice(0, 120)}{String(tagline).length > 120 ? '…' : ''}</div>}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 5 }}>
            {industry    && <span style={{ fontSize: 11, color: '#10b981', background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.2)', padding: '1px 7px', borderRadius: 8 }}>{industry}</span>}
            {location    && <span style={{ fontSize: 11, color: 'var(--text-3)' }}>📍 {location}</span>}
            {companySize && <span style={{ fontSize: 11, color: 'var(--text-3)' }}>👥 {companySize}</span>}
            {foundedYear && <span style={{ fontSize: 11, color: 'var(--text-3)' }}>🗓 Founded {foundedYear}</span>}
          </div>
        </div>
        <a href={profile.url} target="_blank" rel="noreferrer" style={{ fontSize: 10, fontWeight: 700, padding: '3px 8px', borderRadius: 6, background: 'rgba(16,185,129,0.12)', color: '#10b981', flexShrink: 0, textDecoration: 'none' }}>
          Website ↗
        </a>
      </div>

      {/* About */}
      {about && (
        <SectionBlock label="About">
          <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.7, maxHeight: 72, overflow: 'hidden' }}>
            {String(about).slice(0, 350)}{String(about).length > 350 ? '…' : ''}
          </div>
        </SectionBlock>
      )}

      {/* Products & Services side by side */}
      {(products?.length > 0 || services?.length > 0) && (
        <div style={{ display: 'grid', gridTemplateColumns: products?.length > 0 && services?.length > 0 ? '1fr 1fr' : '1fr', gap: 12, marginTop: 10, paddingTop: 10, borderTop: '1px solid var(--border-1)' }}>
          {products?.length > 0 && (
            <div>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#f59e0b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>Products</div>
              <TagList items={products} color="#fbbf24" bg="rgba(245,158,11,0.1)" border="rgba(245,158,11,0.25)" />
            </div>
          )}
          {services?.length > 0 && (
            <div>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#8b5cf6', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>Services</div>
              <TagList items={services} color="#c4b5fd" bg="rgba(139,92,246,0.1)" border="rgba(139,92,246,0.25)" />
            </div>
          )}
        </div>
      )}

      {/* Key Features */}
      {keyFeatures?.length > 0 && (
        <SectionBlock label="Key Features">
          <TagList items={keyFeatures} color="#60a5fa" bg="rgba(59,130,246,0.1)" border="rgba(59,130,246,0.25)" />
        </SectionBlock>
      )}

      {/* Target Audience */}
      {targetAudience && (
        <SectionBlock label="Target Audience">
          <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.6 }}>{String(targetAudience)}</div>
        </SectionBlock>
      )}

      {/* Social Links */}
      {socialLinks?.length > 0 && (
        <SectionBlock label="Social Links">
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {socialLinks.map((l, i) => <span key={i} style={{ fontSize: 11, color: 'var(--text-3)' }}>{l}</span>)}
          </div>
        </SectionBlock>
      )}

      <DataFooter count={Object.keys(profile).length} />
    </div>
  )
}

function DataFooter({ count }) {
  return (
    <div style={{ marginTop: 12, padding: '6px 10px', borderRadius: 8, background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.15)', fontSize: 11, color: 'var(--text-3)', display: 'flex', alignItems: 'center', gap: 6 }}>
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#6366f1" strokeWidth="2">
        <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
      </svg>
      {count} data fields scraped — ready to generate prompts
    </div>
  )
}

// ── Section prompt card ────────────────────────────────────────────────────────

function SectionCard({ meta, state, prompt, error, onSave }) {
  const [text, setText]     = useState('')
  const [saving, setSaving] = useState(false)
  const [saved, setSaved]   = useState(false)
  const [expanded, setExpanded] = useState(true)

  // Sync textarea when prompt arrives from stream
  useEffect(() => {
    if (prompt) {
      setText(prompt)
      setSaved(false)
    }
  }, [prompt])

  const isDone    = state === 'done'
  const isRunning = state === 'running'
  const isError   = state === 'error'
  const isIdle    = state === 'idle'

  const handleSave = async () => {
    const content = text.trim()
    if (!content) return
    setSaving(true)
    try {
      const resp = await fetch(`${API}/api/prompt-generator/save`, {
        method: 'POST',
        headers: { ...authHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ section_key: meta.key, section_label: meta.label, content }),
      })
      if (!resp.ok) throw new Error(await resp.text())
      toast.success(`${meta.label} saved as system prompt`)
      setSaved(true)
      onSave && onSave()
    } catch (e) {
      toast.error(`Save failed: ${e.message}`)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border-1)',
      borderRadius: 12, overflow: 'hidden', marginBottom: 10,
      borderLeft: `3px solid ${isError ? '#ef4444' : isIdle ? 'var(--border-1)' : meta.color}`,
      opacity: isIdle ? 0.45 : 1,
      transition: 'opacity 0.2s, border-left-color 0.2s',
    }}>
      {/* Header row */}
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 10, padding: '10px 16px',
          cursor: isDone ? 'pointer' : 'default',
        }}
        onClick={() => isDone && setExpanded(e => !e)}
      >
        <span style={{ fontSize: 15 }}>{meta.icon}</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>{meta.label}</div>
          <div style={{ fontSize: 11, color: 'var(--text-3)' }}>{meta.desc}</div>
        </div>

        {isRunning && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: meta.color }}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
              style={{ animation: 'spin 0.8s linear infinite' }}>
              <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round"/>
            </svg>
            Generating…
          </div>
        )}
        {isDone && !saved && (
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#10b981" strokeWidth="2.5">
            <polyline points="20 6 9 17 4 12"/>
          </svg>
        )}
        {saved && (
          <span style={{ fontSize: 10, fontWeight: 700, color: '#10b981' }}>SAVED</span>
        )}
        {isError && (
          <span style={{ fontSize: 11, color: '#ef4444' }}>Error</span>
        )}
        {isDone && (
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="var(--text-3)" strokeWidth="2"
            style={{ transform: expanded ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>
            <polyline points="6 9 12 15 18 9"/>
          </svg>
        )}
      </div>

      {/* Prompt textarea */}
      {isDone && expanded && (
        <div style={{ padding: '0 16px 16px' }}>
          <div style={{ fontSize: 11, color: 'var(--text-3)', marginBottom: 6 }}>
            Generated system prompt — edit before saving if needed
          </div>
          <textarea
            value={text}
            onChange={e => { setText(e.target.value); setSaved(false) }}
            rows={6}
            style={{
              width: '100%', boxSizing: 'border-box',
              background: 'var(--bg-base)', border: '1px solid var(--border-1)',
              borderRadius: 8, padding: '10px 12px',
              color: 'var(--text-1)', fontSize: 12, lineHeight: 1.7,
              resize: 'vertical', fontFamily: 'inherit',
              outline: 'none',
            }}
          />
          <div style={{ marginTop: 8, display: 'flex', gap: 8 }}>
            <button
              onClick={handleSave}
              disabled={saving || !text.trim()}
              style={{
                padding: '6px 16px', borderRadius: 7, fontSize: 12, fontWeight: 600,
                cursor: saving || !text.trim() ? 'not-allowed' : 'pointer',
                background: saved ? 'rgba(16,185,129,0.12)' : 'rgba(99,102,241,0.15)',
                border: `1px solid ${saved ? 'rgba(16,185,129,0.3)' : 'rgba(99,102,241,0.3)'}`,
                color: saved ? '#10b981' : '#a5b4fc',
              }}
            >
              {saving ? 'Saving…' : saved ? 'Saved ✓' : 'Save as System Prompt'}
            </button>
            <button
              onClick={() => { setText(prompt); setSaved(false) }}
              style={{
                padding: '6px 12px', borderRadius: 7, fontSize: 12,
                cursor: 'pointer', background: 'transparent',
                border: '1px solid var(--border-1)', color: 'var(--text-3)',
              }}
            >
              Reset
            </button>
          </div>
        </div>
      )}

      {isError && (
        <div style={{ padding: '0 16px 12px', fontSize: 12, color: '#ef4444' }}>{error}</div>
      )}
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────

const PHASE_IDLE      = 'idle'
const PHASE_SCRAPING  = 'scraping'
const PHASE_SCRAPED   = 'scraped'
const PHASE_GENERATING = 'generating'
const PHASE_DONE      = 'done'

function initSectionStates() {
  return Object.fromEntries(SECTIONS.map(s => [s.key, { state: 'idle', prompt: '', error: '' }]))
}

export default function SystemPromptGeneratorPage() {
  const [url,       setUrl]       = useState('')
  const [phase,     setPhase]     = useState(PHASE_IDLE)
  const [sourceType, setSourceType] = useState('')
  const [profile,   setProfile]   = useState(null)
  const [scrapeErr, setScrapeErr] = useState('')
  const [sections,  setSections]  = useState(initSectionStates)
  const abortRef = useRef(null)

  const isLinkedIn = /linkedin\.com\/in\//i.test(url)

  // ── Phase 1: Scrape ──────────────────────────────────────────────────────────
  const handleScrape = async () => {
    const trimmed = url.trim()
    if (!trimmed) { toast.error('Enter a URL first'); return }
    if (!trimmed.startsWith('http')) { toast.error('URL must start with http:// or https://'); return }

    setPhase(PHASE_SCRAPING)
    setScrapeErr('')
    setProfile(null)
    setSections(initSectionStates())

    try {
      const resp = await fetch(`${API}/api/prompt-generator/scrape`, {
        method: 'POST',
        headers: { ...authHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: trimmed }),
      })
      const data = await resp.json()
      if (!resp.ok) throw new Error(data.detail || resp.statusText)
      setProfile(data.profile)
      setSourceType(data.source_type)
      setPhase(PHASE_SCRAPED)
    } catch (e) {
      setScrapeErr(e.message)
      setPhase(PHASE_IDLE)
      toast.error(`Scrape failed: ${e.message}`)
    }
  }

  // ── Phase 2: Generate prompts ────────────────────────────────────────────────
  const handleGenerate = async () => {
    if (!profile) return

    if (abortRef.current) abortRef.current.abort()
    const controller = new AbortController()
    abortRef.current = controller

    setSections(initSectionStates())
    setPhase(PHASE_GENERATING)

    try {
      const resp = await fetch(`${API}/api/prompt-generator/generate`, {
        method: 'POST',
        headers: { ...authHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile, source_type: sourceType }),
        signal: controller.signal,
      })
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }))
        throw new Error(err.detail || 'Generate failed')
      }

      const data = await resp.json()
      const prompts = data.prompts || {}

      setSections(prev => {
        const next = { ...prev }
        for (const [key, val] of Object.entries(prompts)) {
          if (val.error) {
            next[key] = { state: 'error', prompt: '', error: val.error }
          } else {
            next[key] = { state: 'done', prompt: val.prompt || '', error: '' }
          }
        }
        return next
      })
      setPhase(PHASE_DONE)
    } catch (e) {
      if (e.name === 'AbortError') return
      toast.error(e.message || 'Generation failed')
      setPhase(PHASE_SCRAPED)
    }
  }

  const handleStop = () => {
    if (abortRef.current) abortRef.current.abort()
    setPhase(PHASE_SCRAPED)
  }

  const handleReset = () => {
    if (abortRef.current) abortRef.current.abort()
    setUrl('')
    setProfile(null)
    setSourceType('')
    setScrapeErr('')
    setSections(initSectionStates())
    setPhase(PHASE_IDLE)
  }

  const doneCount    = Object.values(sections).filter(s => s.state === 'done').length
  const isGenerating = phase === PHASE_GENERATING

  return (
    <div style={{ padding: '24px 28px', maxWidth: 860, margin: '0 auto' }}>

      {/* Page header */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 9,
            background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/>
            </svg>
          </div>
          <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: 'var(--text-1)' }}>
            System Prompt Generator
          </h1>
        </div>
        <p style={{ margin: 0, fontSize: 13, color: 'var(--text-3)' }}>
          Scrape a LinkedIn profile or company website, then generate a system prompt for each intelligence section.
        </p>
      </div>

      {/* ── Step 1: URL Input ──────────────────────────────────────────── */}
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border-1)',
        borderRadius: 14, padding: 20, marginBottom: 20,
      }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 10 }}>
          Step 1 — Enter URL
        </div>

        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <div style={{ flex: 1, position: 'relative' }}>
            <input
              type="text"
              value={url}
              onChange={e => setUrl(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && phase === PHASE_IDLE && handleScrape()}
              placeholder="https://www.linkedin.com/in/username  or  https://company.com"
              disabled={phase !== PHASE_IDLE && phase !== PHASE_SCRAPED && phase !== PHASE_DONE}
              style={{
                width: '100%', boxSizing: 'border-box',
                background: 'var(--bg-base)', border: '1px solid var(--border-1)',
                borderRadius: 9, padding: '10px 44px 10px 12px',
                color: 'var(--text-1)', fontSize: 13, outline: 'none',
              }}
            />
            {url.trim() && (
              <span style={{
                position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 5,
                background: isLinkedIn ? 'rgba(10,102,194,0.15)' : 'rgba(16,185,129,0.12)',
                color: isLinkedIn ? '#60a5fa' : '#10b981',
              }}>{isLinkedIn ? 'LinkedIn' : 'Website'}</span>
            )}
          </div>

          {phase === PHASE_SCRAPING ? (
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#6366f1', whiteSpace: 'nowrap' }}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
                style={{ animation: 'spin 0.8s linear infinite' }}>
                <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round"/>
              </svg>
              Scraping…
            </div>
          ) : (
            <button
              onClick={handleScrape}
              disabled={!url.trim() || (phase !== PHASE_IDLE && phase !== PHASE_SCRAPED && phase !== PHASE_DONE)}
              style={{
                padding: '10px 22px', borderRadius: 9, fontSize: 13, fontWeight: 700,
                cursor: url.trim() ? 'pointer' : 'not-allowed',
                background: url.trim() ? 'linear-gradient(135deg,#6366f1,#8b5cf6)' : 'var(--bg-base)',
                border: '1px solid transparent',
                color: url.trim() ? '#fff' : 'var(--text-3)',
                whiteSpace: 'nowrap',
              }}
            >
              Scrape
            </button>
          )}

          {(phase === PHASE_SCRAPED || phase === PHASE_DONE) && (
            <button
              onClick={handleReset}
              style={{
                padding: '10px 14px', borderRadius: 9, fontSize: 12,
                cursor: 'pointer', background: 'transparent',
                border: '1px solid var(--border-1)', color: 'var(--text-3)',
                whiteSpace: 'nowrap',
              }}
            >
              Reset
            </button>
          )}
        </div>

        {scrapeErr && (
          <div style={{ marginTop: 10, fontSize: 12, color: '#ef4444', padding: '8px 10px', background: 'rgba(239,68,68,0.06)', borderRadius: 7 }}>
            {scrapeErr}
          </div>
        )}
      </div>

      {/* ── Profile preview (after scrape) ──────────────────────────────── */}
      {profile && (
        <ProfilePreview profile={profile} sourceType={sourceType} />
      )}

      {/* ── Step 2: Generate Prompts ─────────────────────────────────────── */}
      {profile && (
        <div style={{
          background: 'var(--bg-card)', border: '1px solid var(--border-1)',
          borderRadius: 14, padding: 20, marginBottom: 24,
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 12 }}>
            Step 2 — Generate System Prompts
          </div>

          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 14 }}>
            {SECTIONS.map(s => (
              <div key={s.key} style={{
                display: 'flex', alignItems: 'center', gap: 5,
                fontSize: 11, padding: '4px 10px', borderRadius: 8,
                background: sections[s.key].state === 'done'    ? 'rgba(16,185,129,0.1)'
                          : sections[s.key].state === 'running' ? `rgba(99,102,241,0.1)`
                          : sections[s.key].state === 'error'   ? 'rgba(239,68,68,0.1)'
                          : 'var(--bg-base)',
                border: `1px solid ${
                  sections[s.key].state === 'done'    ? 'rgba(16,185,129,0.25)'
                : sections[s.key].state === 'running' ? 'rgba(99,102,241,0.3)'
                : sections[s.key].state === 'error'   ? 'rgba(239,68,68,0.25)'
                : 'var(--border-1)'}`,
                color: sections[s.key].state === 'done'    ? '#10b981'
                     : sections[s.key].state === 'running' ? '#a5b4fc'
                     : sections[s.key].state === 'error'   ? '#ef4444'
                     : 'var(--text-3)',
              }}>
                <span>{s.icon}</span> {s.label}
              </div>
            ))}
          </div>

          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
            {isGenerating ? (
              <>
                <button
                  onClick={handleStop}
                  style={{
                    padding: '10px 22px', borderRadius: 9, fontSize: 13, fontWeight: 600,
                    cursor: 'pointer', background: 'rgba(239,68,68,0.1)',
                    border: '1px solid rgba(239,68,68,0.3)', color: '#ef4444',
                  }}
                >
                  Stop
                </button>
                <div style={{ fontSize: 12, color: 'var(--text-3)', display: 'flex', alignItems: 'center', gap: 6 }}>
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#6366f1" strokeWidth="2"
                    style={{ animation: 'spin 0.8s linear infinite' }}>
                    <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round"/>
                  </svg>
                  Generating {doneCount} / {SECTIONS.length} prompts…
                </div>
              </>
            ) : (
              <button
                onClick={handleGenerate}
                style={{
                  padding: '10px 28px', borderRadius: 9, fontSize: 14, fontWeight: 700,
                  cursor: 'pointer',
                  background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
                  border: 'none', color: '#fff',
                  boxShadow: '0 2px 12px rgba(99,102,241,0.35)',
                }}
              >
                Generate Prompts
              </button>
            )}

            {phase === PHASE_DONE && (
              <div style={{ fontSize: 12, color: '#10b981', display: 'flex', alignItems: 'center', gap: 6 }}>
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#10b981" strokeWidth="2.5">
                  <polyline points="20 6 9 17 4 12"/>
                </svg>
                All {doneCount} prompts generated
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── Section cards ────────────────────────────────────────────────── */}
      {(isGenerating || phase === PHASE_DONE) && (
        <div>
          {SECTIONS.map(meta => (
            <SectionCard
              key={meta.key}
              meta={meta}
              state={sections[meta.key].state}
              prompt={sections[meta.key].prompt}
              error={sections[meta.key].error}
            />
          ))}
        </div>
      )}

      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
    </div>
  )
}
