// LeadEnrichmentConfigPage.jsx — Lead Enrichment Tool Configuration
import { useState, useEffect, useCallback } from 'react'
import toast from 'react-hot-toast'

const API = import.meta.env.VITE_BACKEND_URL || 'https://api-lead-enrichment-worksbuddy.lbmdemo.com'

const CATEGORY_LABELS = {
  enrichment: 'Data Enrichment',
  email:      'Email Discovery',
  ai:         'AI / LLM',
  realtime:   'Realtime',
}

const CATEGORY_COLORS = {
  enrichment: { bg: 'rgba(99,102,241,0.12)', border: 'rgba(99,102,241,0.3)', text: '#a5b4fc' },
  email:      { bg: 'rgba(16,185,129,0.12)', border: 'rgba(16,185,129,0.3)', text: '#6ee7b7' },
  ai:         { bg: 'rgba(245,158,11,0.12)', border: 'rgba(245,158,11,0.3)', text: '#fcd34d' },
  realtime:   { bg: 'rgba(239,68,68,0.12)',  border: 'rgba(239,68,68,0.3)',  text: '#fca5a5' },
}

function authHeader() {
  const token = localStorage.getItem('wb_ai_token')
  return token ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } : { 'Content-Type': 'application/json' }
}

async function apiFetch(path, opts = {}) {
  const r = await fetch(`${API}${path}`, { headers: authHeader(), ...opts })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Request failed')
  }
  return r.json()
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function CategoryBadge({ category }) {
  const c = CATEGORY_COLORS[category] || CATEGORY_COLORS.enrichment
  return (
    <span style={{
      fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 20,
      background: c.bg, border: `1px solid ${c.border}`, color: c.text,
      textTransform: 'uppercase', letterSpacing: '0.05em',
    }}>
      {CATEGORY_LABELS[category] || category}
    </span>
  )
}

function Toggle({ checked, onChange, disabled }) {
  return (
    <button
      onClick={() => !disabled && onChange(!checked)}
      style={{
        width: 44, height: 24, borderRadius: 12, border: 'none', cursor: disabled ? 'not-allowed' : 'pointer',
        background: checked ? '#6366f1' : 'var(--border-1)',
        position: 'relative', transition: 'background 0.2s', flexShrink: 0,
        opacity: disabled ? 0.5 : 1,
      }}
    >
      <span style={{
        position: 'absolute', top: 3, left: checked ? 23 : 3,
        width: 18, height: 18, borderRadius: '50%', background: '#fff',
        transition: 'left 0.2s', boxShadow: '0 1px 3px rgba(0,0,0,0.3)',
      }} />
    </button>
  )
}

function CreditBar({ used, total }) {
  if (!total) return null
  const pct = Math.min(100, Math.round((used / total) * 100))
  const remaining = total - used
  const color = pct > 90 ? '#ef4444' : pct > 70 ? '#f59e0b' : '#10b981'
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
        <span style={{ fontSize: 11, color: 'var(--text-3)' }}>
          {remaining.toLocaleString()} remaining
        </span>
        <span style={{ fontSize: 11, color: 'var(--text-3)' }}>
          {used.toLocaleString()} / {total.toLocaleString()}
        </span>
      </div>
      <div style={{ height: 6, borderRadius: 3, background: 'var(--border-1)', overflow: 'hidden' }}>
        <div style={{
          height: '100%', width: `${pct}%`, borderRadius: 3,
          background: color, transition: 'width 0.4s',
        }} />
      </div>
    </div>
  )
}

function CreditsPanel({ tool, onUpdate }) {
  const [addAmt, setAddAmt]   = useState('')
  const [setAmt, setSetAmt]   = useState('')
  const [loading, setLoading] = useState(false)
  const [showAdd, setShowAdd] = useState(false)
  const [showSet, setShowSet] = useState(false)

  const handleAdd = async () => {
    const n = parseInt(addAmt)
    if (!n || n <= 0) return toast.error('Enter a positive number')
    setLoading(true)
    try {
      await apiFetch(`/api/config/tools/${tool.tool_name}/credits/add`, {
        method: 'POST', body: JSON.stringify({ amount: n }),
      })
      toast.success(`+${n} credits added to ${tool.label}`)
      setAddAmt(''); setShowAdd(false)
      onUpdate()
    } catch (e) { toast.error(e.message) }
    finally { setLoading(false) }
  }

  const handleSet = async () => {
    const n = parseInt(setAmt)
    if (isNaN(n) || n < 0) return toast.error('Enter a valid number')
    setLoading(true)
    try {
      await apiFetch(`/api/config/tools/${tool.tool_name}/credits`, {
        method: 'PUT', body: JSON.stringify({ total_credits: n, reset_used: false }),
      })
      toast.success(`Credits set to ${n} for ${tool.label}`)
      setSetAmt(''); setShowSet(false)
      onUpdate()
    } catch (e) { toast.error(e.message) }
    finally { setLoading(false) }
  }

  const handleReset = async () => {
    if (!window.confirm(`Reset used credits for ${tool.label}? This clears the usage counter.`)) return
    setLoading(true)
    try {
      await apiFetch(`/api/config/tools/${tool.tool_name}/credits`, {
        method: 'PUT', body: JSON.stringify({ total_credits: tool.total_credits, reset_used: true }),
      })
      toast.success(`Usage counter reset for ${tool.label}`)
      onUpdate()
    } catch (e) { toast.error(e.message) }
    finally { setLoading(false) }
  }

  return (
    <div style={{
      marginTop: 12, padding: 14, borderRadius: 10,
      background: 'var(--bg-base)', border: '1px solid var(--border-1)',
    }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        Credits — {tool.credit_unit}
      </div>

      {tool.total_credits > 0
        ? <CreditBar used={tool.used_credits} total={tool.total_credits} />
        : (
          <div style={{ fontSize: 12, color: 'var(--text-3)', marginBottom: 8 }}>
            No credit limit set — unlimited usage
          </div>
        )
      }

      <div style={{ display: 'flex', gap: 8, marginTop: 12, flexWrap: 'wrap' }}>
        <button
          onClick={() => { setShowAdd(!showAdd); setShowSet(false) }}
          style={btnStyle('#6366f1')}
        >
          + Add Credits
        </button>
        <button
          onClick={() => { setShowSet(!showSet); setShowAdd(false) }}
          style={btnStyle('var(--text-3)', true)}
        >
          Set Total
        </button>
        {tool.total_credits > 0 && (
          <button onClick={handleReset} disabled={loading} style={btnStyle('#ef4444', true)}>
            Reset Used
          </button>
        )}
      </div>

      {showAdd && (
        <div style={{ display: 'flex', gap: 8, marginTop: 10 }}>
          <input
            type="number" min="1" placeholder="Amount to add"
            value={addAmt} onChange={e => setAddAmt(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleAdd()}
            style={inputStyle}
          />
          <button onClick={handleAdd} disabled={loading} style={btnStyle('#6366f1')}>
            {loading ? '...' : 'Add'}
          </button>
        </div>
      )}

      {showSet && (
        <div style={{ display: 'flex', gap: 8, marginTop: 10 }}>
          <input
            type="number" min="0" placeholder="New total credits"
            value={setAmt} onChange={e => setSetAmt(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleSet()}
            style={inputStyle}
          />
          <button onClick={handleSet} disabled={loading} style={btnStyle('#6366f1')}>
            {loading ? '...' : 'Save'}
          </button>
        </div>
      )}
    </div>
  )
}

// ── Tool Card ──────────────────────────────────────────────────────────────────

function ToolCard({ tool, onUpdate }) {
  const [saving,   setSaving]   = useState(false)
  const [apiKey,   setApiKey]   = useState('')
  const [showKey,  setShowKey]  = useState(false)
  const [extra,    setExtra]    = useState({})
  const [dirty,    setDirty]    = useState(false)
  const [showCred, setShowCred] = useState(false)

  // Init extra fields from tool data
  useEffect(() => {
    const init = {}
    ;(tool.config_fields || []).forEach(f => {
      if (f.key !== 'api_key') init[f.key] = tool.extra_config?.[f.key] || ''
    })
    setExtra(init)
    setApiKey('')
    setDirty(false)
  }, [tool.tool_name])

  const handleToggle = async (val) => {
    setSaving(true)
    try {
      await apiFetch(`/api/config/tools/${tool.tool_name}`, {
        method: 'PUT', body: JSON.stringify({ is_enabled: val }),
      })
      toast.success(`${tool.label} ${val ? 'enabled' : 'disabled'}`)
      onUpdate()
    } catch (e) { toast.error(e.message) }
    finally { setSaving(false) }
  }

  const handleSave = async () => {
    setSaving(true)
    try {
      const payload = { is_enabled: tool.is_enabled }
      if (apiKey) payload.api_key = apiKey
      const extraFields = {}
      ;(tool.config_fields || []).forEach(f => {
        if (f.key !== 'api_key' && extra[f.key] !== undefined) {
          extraFields[f.key] = extra[f.key]
        }
      })
      if (Object.keys(extraFields).length) payload.extra_config = extraFields
      await apiFetch(`/api/config/tools/${tool.tool_name}`, {
        method: 'PUT', body: JSON.stringify(payload),
      })
      toast.success(`${tool.label} configuration saved`)
      setApiKey('')
      setDirty(false)
      onUpdate()
    } catch (e) { toast.error(e.message) }
    finally { setSaving(false) }
  }

  const hasExtraFields = (tool.config_fields || []).filter(f => f.key !== 'api_key').length > 0

  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border-1)',
      borderRadius: 14, padding: 20, display: 'flex', flexDirection: 'column', gap: 0,
      boxShadow: tool.is_enabled ? '0 0 0 1px rgba(99,102,241,0.2)' : 'none',
      transition: 'box-shadow 0.2s',
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12, marginBottom: 12 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)' }}>{tool.label}</span>
            <CategoryBadge category={tool.category} />
            {tool.is_configured && (
              <span style={{
                fontSize: 10, padding: '1px 6px', borderRadius: 10, fontWeight: 600,
                background: 'rgba(16,185,129,0.15)', color: '#6ee7b7', border: '1px solid rgba(16,185,129,0.3)',
              }}>KEY SET</span>
            )}
          </div>
          <p style={{ fontSize: 12, color: 'var(--text-3)', margin: 0, lineHeight: 1.5 }}>
            {tool.description}
          </p>
        </div>
        <Toggle checked={tool.is_enabled} onChange={handleToggle} disabled={saving} />
      </div>

      {/* API Key */}
      <div style={{ marginBottom: 12 }}>
        <label style={labelStyle}>API Key</label>
        <div style={{ display: 'flex', gap: 8 }}>
          <div style={{ flex: 1, position: 'relative' }}>
            <input
              type={showKey ? 'text' : 'password'}
              placeholder={tool.api_key_masked || (tool.config_fields?.[0]?.placeholder || 'Enter API key')}
              value={apiKey}
              onChange={e => { setApiKey(e.target.value); setDirty(true) }}
              style={{ ...inputStyle, paddingRight: 36 }}
            />
            <button
              onClick={() => setShowKey(!showKey)}
              style={{
                position: 'absolute', right: 8, top: '50%', transform: 'translateY(-50%)',
                background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-3)', padding: 2,
              }}
            >
              {showKey ? <EyeOff size={14} /> : <Eye size={14} />}
            </button>
          </div>
        </div>
        {tool.api_key_masked && !apiKey && (
          <p style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 4 }}>
            Current: {tool.api_key_masked} — enter new key to update
          </p>
        )}
      </div>

      {/* Extra config fields */}
      {hasExtraFields && (
        <div style={{ marginBottom: 12 }}>
          {(tool.config_fields || []).filter(f => f.key !== 'api_key').map(field => (
            <div key={field.key} style={{ marginBottom: 8 }}>
              <label style={labelStyle}>{field.label}</label>
              <input
                type="text"
                placeholder={field.default || ''}
                value={extra[field.key] || ''}
                onChange={e => { setExtra(p => ({ ...p, [field.key]: e.target.value })); setDirty(true) }}
                style={inputStyle}
              />
            </div>
          ))}
        </div>
      )}

      {/* Save button */}
      {dirty && (
        <button
          onClick={handleSave}
          disabled={saving}
          style={{
            ...btnStyle('#6366f1'),
            marginBottom: 12,
            width: '100%',
            justifyContent: 'center',
          }}
        >
          {saving ? 'Saving...' : 'Save Configuration'}
        </button>
      )}

      {/* Credits toggle */}
      <button
        onClick={() => setShowCred(!showCred)}
        style={{
          background: 'none', border: '1px solid var(--border-1)', borderRadius: 8,
          padding: '7px 12px', cursor: 'pointer', color: 'var(--text-2)', fontSize: 12,
          display: 'flex', alignItems: 'center', gap: 6, fontWeight: 500,
        }}
      >
        <CreditIcon size={13} />
        Credits
        <span style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-3)' }}>
          {tool.total_credits > 0
            ? `${(tool.total_credits - tool.used_credits).toLocaleString()} / ${tool.total_credits.toLocaleString()}`
            : 'Unlimited'}
        </span>
        <Chevron size={12} open={showCred} />
      </button>

      {showCred && <CreditsPanel tool={tool} onUpdate={onUpdate} />}
    </div>
  )
}

// ── Icon helpers ───────────────────────────────────────────────────────────────

function Eye({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>
    </svg>
  )
}

function EyeOff({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/>
      <line x1="1" y1="1" x2="23" y2="23"/>
    </svg>
  )
}

function CreditIcon({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="1" y="4" width="22" height="16" rx="2" ry="2"/><line x1="1" y1="10" x2="23" y2="10"/>
    </svg>
  )
}

function Chevron({ size = 16, open }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"
      style={{ transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>
      <polyline points="6 9 12 15 18 9"/>
    </svg>
  )
}

// ── Style helpers ──────────────────────────────────────────────────────────────

const labelStyle = {
  display: 'block', fontSize: 11, fontWeight: 600,
  color: 'var(--text-2)', marginBottom: 5, textTransform: 'uppercase', letterSpacing: '0.04em',
}

const inputStyle = {
  width: '100%', boxSizing: 'border-box',
  background: 'var(--bg-base)', border: '1px solid var(--border-1)',
  borderRadius: 8, padding: '8px 12px', fontSize: 12, color: 'var(--text-1)',
  outline: 'none', fontFamily: 'inherit',
}

function btnStyle(color, ghost = false) {
  return {
    display: 'inline-flex', alignItems: 'center', gap: 6,
    padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 600,
    cursor: 'pointer', transition: 'opacity 0.15s',
    background: ghost ? 'transparent' : color,
    color: ghost ? color : '#fff',
    border: ghost ? `1px solid ${color}` : 'none',
  }
}

// ── Summary Stats Bar ──────────────────────────────────────────────────────────

function SummaryBar({ tools }) {
  const configured = tools.filter(t => t.is_configured).length
  const enabled    = tools.filter(t => t.is_enabled).length
  const withCredit = tools.filter(t => t.total_credits > 0).length

  return (
    <div style={{
      display: 'flex', gap: 16, marginBottom: 24, flexWrap: 'wrap',
    }}>
      {[
        { label: 'Total Tools',   value: tools.length,  color: '#a5b4fc' },
        { label: 'Enabled',       value: enabled,       color: '#6ee7b7' },
        { label: 'Keys Set',      value: configured,    color: '#fcd34d' },
        { label: 'With Credits',  value: withCredit,    color: '#f9a8d4' },
      ].map(s => (
        <div key={s.label} style={{
          flex: '1 1 100px', background: 'var(--bg-card)',
          border: '1px solid var(--border-1)', borderRadius: 12,
          padding: '12px 16px',
        }}>
          <div style={{ fontSize: 22, fontWeight: 700, color: s.color }}>{s.value}</div>
          <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>{s.label}</div>
        </div>
      ))}
    </div>
  )
}

// ── Main Page ──────────────────────────────────────────────────────────────────

// ── Groq model catalogue ───────────────────────────────────────────────────────
const GROQ_MODELS = [
  {
    id:          'qwen/qwen3-32b',
    label:       'Qwen3 32B',
    badge:       '🏆 Best reasoning',
    badgeColor:  '#f59e0b',
    description: 'Chain-of-thought reasoning model. Best for CRM briefs, deep lead analysis, and complex scoring.',
  },
  {
    id:          'openai/gpt-oss-120b',
    label:       'GPT OSS 120B',
    badge:       '💎 Highest quality',
    badgeColor:  '#10b981',
    description: 'Largest model available on Groq. Best output quality — slightly slower.',
  },
  {
    id:          'llama-3.3-70b-versatile',
    label:       'Llama 3.3 70B Versatile',
    badge:       '⭐ Recommended',
    badgeColor:  '#6366f1',
    description: 'Best overall balance of speed + quality for outreach generation and lead summaries.',
  },
  {
    id:          'meta-llama/llama-4-scout-17b-16e-instruct',
    label:       'Llama 4 Scout 17B',
    badge:       '🆕 Llama 4',
    badgeColor:  '#3b82f6',
    description: 'Newest Llama model — fast, smart, great for structured JSON output.',
  },
  {
    id:          'moonshotai/kimi-k2-instruct',
    label:       'Kimi K2',
    badge:       '🌙 Long context',
    badgeColor:  '#8b5cf6',
    description: 'Strong at long-context reasoning and tool use. Good for complex profiles.',
  },
  {
    id:          'groq/compound',
    label:       'Groq Compound',
    badge:       'Groq Native',
    badgeColor:  '#ec4899',
    description: 'Groq native model — optimized for complex multi-step tasks on Groq infrastructure.',
  },
  {
    id:          'groq/compound-mini',
    label:       'Groq Compound Mini',
    badge:       '⚡ Fast + Smart',
    badgeColor:  '#14b8a6',
    description: 'Groq native mini model — fast, capable, good balance of speed and quality.',
  },
  {
    id:          'llama-3.1-8b-instant',
    label:       'Llama 3.1 8B Instant',
    badge:       '⚡ Fastest',
    badgeColor:  '#64748b',
    description: 'Ultra-fast responses (~1000 tok/s). Good for quick summaries when speed matters most.',
  },
]

// ── Prompt accordion for LIO stage / AI module editing ───────────────────────
function PromptAccordion({ prompts, onChange }) {
  const [open, setOpen] = useState(null)
  if (!prompts || prompts.length === 0) return null
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {prompts.map((p, idx) => {
        const isOpen = open === idx
        return (
          <div key={p.id ?? idx} style={{
            borderRadius: 8, border: '1px solid var(--border-1)',
            background: 'var(--bg-base)', overflow: 'hidden',
          }}>
            <button
              onClick={() => setOpen(isOpen ? null : idx)}
              style={{
                width: '100%', padding: '10px 14px', background: 'none', border: 'none',
                cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 10, textAlign: 'left',
              }}
            >
              <span style={{
                fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 10,
                background: 'rgba(99,102,241,0.15)', color: '#a5b4fc',
                border: '1px solid rgba(99,102,241,0.25)', flexShrink: 0,
              }}>
                {p.id ?? idx}
              </span>
              <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', flex: 1 }}>{p.name}</span>
              {p.description && (
                <span style={{ fontSize: 11, color: 'var(--text-3)', flex: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {p.description}
                </span>
              )}
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="var(--text-3)" strokeWidth="2.5"
                style={{ transform: isOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s', flexShrink: 0 }}>
                <polyline points="6 9 12 15 18 9"/>
              </svg>
            </button>
            {isOpen && (
              <div style={{ padding: '0 14px 14px', borderTop: '1px solid var(--border-1)', display: 'flex', flexDirection: 'column', gap: 10 }}>
                <div style={{ marginTop: 10 }}>
                  <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.05em' }}>System Prompt</div>
                  <textarea
                    value={p.system || ''}
                    onChange={e => { const next = [...prompts]; next[idx] = { ...p, system: e.target.value }; onChange(next) }}
                    rows={4}
                    style={{
                      width: '100%', boxSizing: 'border-box', padding: '8px 10px', borderRadius: 7,
                      border: '1px solid var(--border-1)', background: 'rgba(0,0,0,0.2)',
                      color: 'var(--text-1)', fontSize: 11, lineHeight: 1.6,
                      fontFamily: "'JetBrains Mono','Fira Code',monospace", resize: 'vertical', outline: 'none',
                    }}
                  />
                </div>
                {p.user_template !== undefined && (
                  <div>
                    <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.05em' }}>User Prompt Template</div>
                    <div style={{ fontSize: 10, color: 'var(--text-3)', marginBottom: 6, lineHeight: 1.5 }}>
                      Use <code style={{ color: '#a5b4fc' }}>{'{variable}'}</code> placeholders. Available vars depend on the stage.
                    </div>
                    <textarea
                      value={p.user_template || ''}
                      onChange={e => { const next = [...prompts]; next[idx] = { ...p, user_template: e.target.value }; onChange(next) }}
                      rows={6}
                      style={{
                        width: '100%', boxSizing: 'border-box', padding: '8px 10px', borderRadius: 7,
                        border: '1px solid var(--border-1)', background: 'rgba(0,0,0,0.2)',
                        color: 'var(--text-1)', fontSize: 11, lineHeight: 1.6,
                        fontFamily: "'JetBrains Mono','Fira Code',monospace", resize: 'vertical', outline: 'none',
                      }}
                    />
                  </div>
                )}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

export default function LeadEnrichmentConfigPage() {
  const [tools,    setTools]    = useState([])
  const [loading,  setLoading]  = useState(true)
  const [filter,   setFilter]   = useState('all')
  const [lioPrompt,    setLioPrompt]    = useState('')
  const [lioModel,     setLioModel]     = useState('')
  const [lioSaving,    setLioSaving]    = useState(false)
  const [lioLoaded,    setLioLoaded]    = useState(false)
  const [lioStatus,    setLioStatus]    = useState(null)
  // LIO stage prompts
  const [lioStages,       setLioStages]       = useState([])
  const [lioStagesSaving, setLioStagesSaving] = useState(false)
  const [lioStagesLoaded, setLioStagesLoaded] = useState(false)
  // AI module prompts
  const [aiModules,       setAiModules]       = useState([])
  const [aiModulesSaving, setAiModulesSaving] = useState(false)
  const [aiModulesLoaded, setAiModulesLoaded] = useState(false)

  const load = useCallback(async () => {
    try {
      const data = await apiFetch('/api/config/tools')
      setTools(data)
    } catch (e) {
      toast.error('Failed to load config: ' + e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    apiFetch('/api/leads/lio/prompt')
      .then(d => {
        setLioPrompt(d.system_prompt || '')
        setLioModel(d.model || '')
        setLioLoaded(true)
      })
      .catch(() => setLioLoaded(true))
    apiFetch('/api/leads/lio/status')
      .then(d => setLioStatus(d))
      .catch(() => {})
    apiFetch('/api/leads/lio/config')
      .then(d => { setLioStages(d.prompts || []); setLioStagesLoaded(true) })
      .catch(() => setLioStagesLoaded(true))
    apiFetch('/api/v1/ai/config')
      .then(d => { setAiModules(d.prompts || []); setAiModulesLoaded(true) })
      .catch(() => setAiModulesLoaded(true))
  }, [])

  const saveLioPrompt = async () => {
    setLioSaving(true)
    try {
      await apiFetch('/api/leads/lio/prompt', {
        method: 'POST',
        body: JSON.stringify({ system_prompt: lioPrompt, model: lioModel || null }),
      })
      toast.success('LIO configuration saved')
      apiFetch('/api/leads/lio/status').then(d => setLioStatus(d)).catch(() => {})
    } catch (e) {
      toast.error('Save failed: ' + e.message)
    } finally {
      setLioSaving(false)
    }
  }

  const saveLioStages = async () => {
    setLioStagesSaving(true)
    try {
      await apiFetch('/api/leads/lio/config', {
        method: 'PUT',
        body: JSON.stringify({ prompts: lioStages }),
      })
      toast.success('LIO stage prompts saved')
    } catch (e) {
      toast.error('Save failed: ' + e.message)
    } finally {
      setLioStagesSaving(false)
    }
  }

  const saveAiModules = async () => {
    setAiModulesSaving(true)
    try {
      await apiFetch('/api/v1/ai/config', {
        method: 'PUT',
        body: JSON.stringify({ prompts: aiModules }),
      })
      toast.success('AI module prompts saved')
    } catch (e) {
      toast.error('Save failed: ' + e.message)
    } finally {
      setAiModulesSaving(false)
    }
  }

  const categories = ['all', ...new Set(tools.map(t => t.category))]
  const visible    = filter === 'all' ? tools : tools.filter(t => t.category === filter)

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '60vh' }}>
        <div style={{ fontSize: 13, color: 'var(--text-3)' }}>Loading tool configurations...</div>
      </div>
    )
  }

  return (
    <div style={{ padding: '28px 32px', maxWidth: 960, margin: '0 auto' }}>
      {/* Page header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, color: 'var(--text-1)', margin: 0 }}>
          Lead Enrichment Configuration
        </h1>
        <p style={{ fontSize: 13, color: 'var(--text-3)', marginTop: 4 }}>
          Configure 3rd party tools, API keys, and credit limits used during lead enrichment.
          Only enabled tools with available credits will be used.
        </p>
      </div>

      <SummaryBar tools={tools} />

      {/* Category filter tabs */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 20, flexWrap: 'wrap' }}>
        {categories.map(cat => (
          <button
            key={cat}
            onClick={() => setFilter(cat)}
            style={{
              padding: '6px 14px', borderRadius: 20, fontSize: 12, fontWeight: 600,
              cursor: 'pointer', border: '1px solid',
              background: filter === cat ? '#6366f1' : 'transparent',
              borderColor: filter === cat ? '#6366f1' : 'var(--border-1)',
              color: filter === cat ? '#fff' : 'var(--text-2)',
              transition: 'all 0.15s',
            }}
          >
            {cat === 'all' ? 'All Tools' : (CATEGORY_LABELS[cat] || cat)}
            <span style={{
              marginLeft: 6, fontSize: 10, padding: '0 5px', borderRadius: 10,
              background: 'rgba(255,255,255,0.15)',
            }}>
              {cat === 'all' ? tools.length : tools.filter(t => t.category === cat).length}
            </span>
          </button>
        ))}
      </div>

      {/* Tool cards grid */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(420px, 1fr))',
        gap: 16,
      }}>
        {visible.map(tool => (
          <ToolCard key={tool.tool_name} tool={tool} onUpdate={load} />
        ))}
      </div>

      {visible.length === 0 && (
        <div style={{ textAlign: 'center', padding: '48px 0', color: 'var(--text-3)', fontSize: 13 }}>
          No tools in this category.
        </div>
      )}

      {/* ── LIO Enrichment Configuration ── */}
      <div style={{
        marginTop: 32, borderRadius: 12,
        border: '1px solid rgba(139,92,246,0.3)',
        background: 'rgba(139,92,246,0.05)', overflow: 'hidden',
      }}>
        {/* Header */}
        <div style={{
          padding: '14px 20px', borderBottom: '1px solid rgba(139,92,246,0.2)',
          display: 'flex', alignItems: 'center', gap: 12,
        }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8,
            background: 'linear-gradient(135deg,#8b5cf6,#6366f1)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
          }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
            </svg>
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)' }}>LIO Enrichment — AI Configuration</div>
            <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
              System prompt + model selection for the LIO lead analysis tab.
            </div>
          </div>
          <span style={{
            fontSize: 9, fontWeight: 700, padding: '2px 8px', borderRadius: 20,
            background: 'rgba(139,92,246,0.15)', border: '1px solid rgba(139,92,246,0.35)',
            color: '#c4b5fd', textTransform: 'uppercase', letterSpacing: '0.06em',
          }}>AI / LLM</span>
        </div>

        <div style={{ padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: 16 }}>

          {/* ── Provider Status ── */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              LLM Provider Status
            </div>
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              {[
                {
                  key: 'wb_llm', label: 'WorksBuddy LLM',
                  note: 'Primary (checked first)',
                  configured: lioStatus?.wb_llm?.configured,
                  enabled:    lioStatus?.wb_llm?.enabled,
                },
                {
                  key: 'groq', label: 'Groq',
                  note: 'Fallback provider',
                  configured: lioStatus?.groq?.configured,
                  enabled:    lioStatus?.groq?.enabled,
                },
              ].map(p => {
                const ok     = p.configured && p.enabled
                const color  = lioStatus == null ? '#64748b' : ok ? '#10b981' : p.configured ? '#f59e0b' : '#ef4444'
                const status = lioStatus == null ? 'Checking…' : ok ? 'Ready' : p.configured ? 'Key set but disabled' : 'Not configured'
                const dot    = lioStatus == null ? '○' : ok ? '●' : '○'
                return (
                  <div key={p.key} style={{
                    flex: '1 1 200px', padding: '10px 14px', borderRadius: 10,
                    background: 'var(--bg-base)', border: `1px solid ${ok ? 'rgba(16,185,129,0.25)' : 'var(--border-1)'}`,
                    display: 'flex', flexDirection: 'column', gap: 4,
                  }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <span style={{ color, fontSize: 14, lineHeight: 1 }}>{dot}</span>
                      <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)' }}>{p.label}</span>
                      {lioStatus?.active_provider === p.key && (
                        <span style={{
                          marginLeft: 'auto', fontSize: 9, padding: '1px 6px', borderRadius: 8, fontWeight: 700,
                          background: 'rgba(16,185,129,0.15)', color: '#6ee7b7', border: '1px solid rgba(16,185,129,0.3)',
                        }}>ACTIVE</span>
                      )}
                    </div>
                    <div style={{ fontSize: 11, color }}>
                      {status}
                    </div>
                    <div style={{ fontSize: 10, color: 'var(--text-3)' }}>{p.note}</div>
                    {!p.configured && lioStatus != null && (
                      <div style={{ fontSize: 10, color: '#f59e0b', marginTop: 2 }}>
                        → Add API key in {p.label} tool card above
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>

          {/* ── Groq Model Selector ── */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Groq Model Selection
            </div>
            <div style={{ fontSize: 11, color: 'var(--text-3)', marginBottom: 10, lineHeight: 1.5 }}>
              When WB LLM is unavailable, LIO uses Groq. Pick the model that best fits your analysis depth needs.
              {lioModel && <span style={{ color: '#a5b4fc' }}> Current: <strong>{lioModel}</strong></span>}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {GROQ_MODELS.map(m => {
                const selected = lioModel === m.id || (!lioModel && m.id === 'llama-3.3-70b-versatile')
                return (
                  <button
                    key={m.id}
                    onClick={() => setLioModel(m.id)}
                    style={{
                      background: selected ? 'rgba(99,102,241,0.1)' : 'var(--bg-base)',
                      border: `1px solid ${selected ? 'rgba(99,102,241,0.4)' : 'var(--border-1)'}`,
                      borderRadius: 10, padding: '10px 14px', cursor: 'pointer',
                      textAlign: 'left', transition: 'all 0.15s',
                    }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                      <div style={{
                        width: 16, height: 16, borderRadius: '50%', flexShrink: 0, border: '2px solid',
                        borderColor: selected ? '#6366f1' : 'var(--border-1)',
                        background: selected ? '#6366f1' : 'transparent',
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                      }}>
                        {selected && <div style={{ width: 6, height: 6, borderRadius: '50%', background: '#fff' }} />}
                      </div>
                      <span style={{ fontSize: 12, fontWeight: 600, color: selected ? '#a5b4fc' : 'var(--text-1)' }}>
                        {m.label}
                      </span>
                      <span style={{
                        marginLeft: 4, fontSize: 9, padding: '1px 7px', borderRadius: 20, fontWeight: 700,
                        background: `${m.badgeColor}22`, color: m.badgeColor,
                        border: `1px solid ${m.badgeColor}44`,
                      }}>{m.badge}</span>
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--text-3)', paddingLeft: 24 }}>
                      {m.description}
                    </div>
                  </button>
                )
              })}
            </div>
          </div>

          {/* ── How it works ── */}
          <div style={{ fontSize: 11, color: 'var(--text-3)', lineHeight: 1.6 }}>
            <strong style={{ color: 'var(--text-2)' }}>How it works:</strong> When you open a lead profile and click "LIO Enrichment",
            the system sends this prompt as the system role and the lead's full BrightData JSON as the user role
            to the active LLM provider. The response streams live in the tab.
          </div>

          {/* ── System Prompt textarea ── */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              System Prompt
            </div>
            <textarea
              value={lioLoaded ? lioPrompt : 'Loading...'}
              onChange={e => setLioPrompt(e.target.value)}
              disabled={!lioLoaded}
              rows={10}
              placeholder={`Example:\nYou are a B2B sales intelligence analyst. Analyze the provided lead data and return:\n1. A 3-line executive summary of the lead\n2. Top 3 reasons they are a good ICP fit\n3. Best outreach angle and timing\n4. Any red flags or disqualifiers\nBe concise and actionable.`}
              style={{
                width: '100%', boxSizing: 'border-box',
                padding: '12px 14px', borderRadius: 9,
                border: '1px solid rgba(139,92,246,0.3)',
                background: 'rgba(0,0,0,0.25)', color: 'var(--text-1)',
                fontSize: 12, lineHeight: 1.7, fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                resize: 'vertical', outline: 'none',
              }}
            />
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div style={{ fontSize: 11, color: 'var(--text-3)' }}>
              {lioPrompt.length > 0 ? `${lioPrompt.length} characters · ~${Math.round(lioPrompt.length / 4)} tokens` : 'No prompt configured'}
            </div>
            <button
              onClick={saveLioPrompt}
              disabled={lioSaving || !lioLoaded}
              style={{
                padding: '8px 20px', borderRadius: 8, border: 'none', cursor: lioSaving ? 'not-allowed' : 'pointer',
                background: 'linear-gradient(135deg,#8b5cf6,#6366f1)',
                color: '#fff', fontSize: 12, fontWeight: 600,
                opacity: lioSaving ? 0.7 : 1, transition: 'opacity 0.15s',
              }}
            >
              {lioSaving ? 'Saving…' : 'Save Configuration'}
            </button>
          </div>
        </div>
      </div>

      {/* ── LIO Stage Prompts ── */}
      <div style={{
        marginTop: 24, borderRadius: 12,
        border: '1px solid rgba(16,185,129,0.3)',
        background: 'rgba(16,185,129,0.04)', overflow: 'hidden',
      }}>
        <div style={{
          padding: '14px 20px', borderBottom: '1px solid rgba(16,185,129,0.2)',
          display: 'flex', alignItems: 'center', gap: 12,
        }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8,
            background: 'linear-gradient(135deg,#10b981,#059669)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
          }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>
            </svg>
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)' }}>LIO Stage Prompts</div>
            <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
              System + user prompt templates for each LIO enrichment stage (outreach, tags, scoring, etc.)
            </div>
          </div>
          <span style={{
            fontSize: 9, fontWeight: 700, padding: '2px 8px', borderRadius: 20,
            background: 'rgba(16,185,129,0.15)', border: '1px solid rgba(16,185,129,0.35)',
            color: '#6ee7b7', textTransform: 'uppercase', letterSpacing: '0.06em',
          }}>{lioStages.length} stages</span>
        </div>
        <div style={{ padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: 12 }}>
          {lioStagesLoaded ? (
            <PromptAccordion prompts={lioStages} onChange={setLioStages} />
          ) : (
            <div style={{ fontSize: 12, color: 'var(--text-3)' }}>Loading stage prompts…</div>
          )}
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <button
              onClick={saveLioStages}
              disabled={lioStagesSaving || !lioStagesLoaded}
              style={{
                padding: '8px 20px', borderRadius: 8, border: 'none', cursor: lioStagesSaving ? 'not-allowed' : 'pointer',
                background: 'linear-gradient(135deg,#10b981,#059669)',
                color: '#fff', fontSize: 12, fontWeight: 600,
                opacity: lioStagesSaving ? 0.7 : 1, transition: 'opacity 0.15s',
              }}
            >
              {lioStagesSaving ? 'Saving…' : 'Save Stage Prompts'}
            </button>
          </div>
        </div>
      </div>

      {/* ── AI Module Prompts ── */}
      <div style={{
        marginTop: 24, borderRadius: 12,
        border: '1px solid rgba(245,158,11,0.3)',
        background: 'rgba(245,158,11,0.04)', overflow: 'hidden',
      }}>
        <div style={{
          padding: '14px 20px', borderBottom: '1px solid rgba(245,158,11,0.2)',
          display: 'flex', alignItems: 'center', gap: 12,
        }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8,
            background: 'linear-gradient(135deg,#f59e0b,#d97706)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
          }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/>
            </svg>
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)' }}>AI Module Prompts</div>
            <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
              System + user templates for each AI enrichment module (identity, contact, scores, ICP, outreach…)
            </div>
          </div>
          <span style={{
            fontSize: 9, fontWeight: 700, padding: '2px 8px', borderRadius: 20,
            background: 'rgba(245,158,11,0.15)', border: '1px solid rgba(245,158,11,0.35)',
            color: '#fcd34d', textTransform: 'uppercase', letterSpacing: '0.06em',
          }}>{aiModules.length} modules</span>
        </div>
        <div style={{ padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: 12 }}>
          {aiModulesLoaded ? (
            <PromptAccordion prompts={aiModules} onChange={setAiModules} />
          ) : (
            <div style={{ fontSize: 12, color: 'var(--text-3)' }}>Loading module prompts…</div>
          )}
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <button
              onClick={saveAiModules}
              disabled={aiModulesSaving || !aiModulesLoaded}
              style={{
                padding: '8px 20px', borderRadius: 8, border: 'none', cursor: aiModulesSaving ? 'not-allowed' : 'pointer',
                background: 'linear-gradient(135deg,#f59e0b,#d97706)',
                color: '#fff', fontSize: 12, fontWeight: 600,
                opacity: aiModulesSaving ? 0.7 : 1, transition: 'opacity 0.15s',
              }}
            >
              {aiModulesSaving ? 'Saving…' : 'Save Module Prompts'}
            </button>
          </div>
        </div>
      </div>

    </div>
  )
}