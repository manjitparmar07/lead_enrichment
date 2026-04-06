// LioConfigPage.jsx — AI Enrichment Module Configuration
import { useState, useEffect, useCallback } from 'react'
import toast from 'react-hot-toast'

const API = import.meta.env.VITE_BACKEND_URL || 'https://leadenrichment-production-5b78.up.railway.app'

function authHeader() {
  const token = localStorage.getItem('wb_ai_token')
  return token
    ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
    : { 'Content-Type': 'application/json' }
}
async function apiFetch(path, opts = {}) {
  const r = await fetch(`${API}${path}`, { headers: authHeader(), ...opts })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Request failed')
  }
  return r.json()
}

// ── Module definitions (mirrors backend DEFAULT_AI_MODULE_PROMPTS) ─────────────
const GROUPS = [
  { id: 'core',         label: 'Core',          color: '#3b82f6', bg: 'rgba(59,130,246,0.08)' },
  { id: 'intelligence', label: 'Intelligence',   color: '#8b5cf6', bg: 'rgba(139,92,246,0.08)' },
  { id: 'output',       label: 'Output',         color: '#10b981', bg: 'rgba(16,185,129,0.08)' },
]

const MODULE_META = {
  identity:             { group: 'core',         color: '#3b82f6', icon: '👤' },
  contact:              { group: 'core',         color: '#06b6d4', icon: '📧' },
  scores:               { group: 'core',         color: '#f59e0b', icon: '📊' },
  icp_match:            { group: 'intelligence', color: '#8b5cf6', icon: '🎯' },
  behavioural_signals:  { group: 'intelligence', color: '#ec4899', icon: '🧠' },
  pitch_intelligence:   { group: 'intelligence', color: '#f97316', icon: '⚡' },
  activity:             { group: 'intelligence', color: '#14b8a6', icon: '📡' },
  tags:                 { group: 'output',       color: '#a855f7', icon: '🏷️' },
  outreach:             { group: 'output',       color: '#ef4444', icon: '✉️' },
  persona_analysis:     { group: 'output',       color: '#84cc16', icon: '🔍' },
}

const TIER_META = {
  fast:    { color: '#10b981', bg: 'rgba(16,185,129,0.12)', border: 'rgba(16,185,129,0.3)', label: 'FAST' },
  quality: { color: '#8b5cf6', bg: 'rgba(139,92,246,0.12)', border: 'rgba(139,92,246,0.3)', label: 'QUALITY' },
}

function getTier(modelId, models) {
  const flat = Object.values(models).flat()
  const m = flat.find(x => x.id === modelId)
  return m ? m.tier : 'quality'
}

// ── useLlmModels — fetch model list from DB via API ───────────────────────────
function useLlmModels() {
  const [models, setModels] = useState({ fast: [], quality: [] })
  useEffect(() => {
    apiFetch('/api/config/llm-models')
      .then(rows => {
        const grouped = { fast: [], quality: [] }
        rows.forEach(m => {
          const tier = m.tier === 'fast' ? 'fast' : 'quality'
          grouped[tier].push({ id: m.id, label: m.label, note: m.note, tier })
        })
        setModels(grouped)
      })
      .catch(() => {}) // silently keep empty; ModelSelector handles no-models state
  }, [])
  return models
}

// ── Model Selector ────────────────────────────────────────────────────────────
function ModelSelector({ value, onChange, accentColor, models }) {
  const [open, setOpen] = useState(false)
  const allModels = [...(models.fast || []), ...(models.quality || [])]
  const current = allModels.find(m => m.id === value) || allModels[0] || { label: value, tier: 'quality' }
  const tier = getTier(value, models)
  const tm = TIER_META[tier] || TIER_META.quality

  return (
    <div style={{ position: 'relative' }}>
      <button onClick={() => setOpen(o => !o)}
        style={{
          width: '100%', display: 'flex', alignItems: 'center', gap: 8,
          padding: '9px 12px', borderRadius: 8, cursor: 'pointer',
          background: 'var(--bg-base)', border: `1.5px solid ${open ? accentColor : 'var(--border-1)'}`,
          transition: 'border-color 0.15s',
        }}>
        <span style={{
          fontSize: 9, fontWeight: 700, padding: '2px 6px', borderRadius: 4,
          background: tm.bg, color: tm.color, border: `1px solid ${tm.border}`,
          textTransform: 'uppercase', letterSpacing: '0.05em', flexShrink: 0,
        }}>{tm.label}</span>
        <span style={{ flex: 1, fontSize: 12, fontWeight: 600, color: 'var(--text-1)', textAlign: 'left' }}>{current.label}</span>
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
          style={{ color: 'var(--text-3)', transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s', flexShrink: 0 }}>
          <polyline points="6 9 12 15 18 9"/>
        </svg>
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 200, marginTop: 4,
          background: 'var(--bg-card)', border: '1px solid var(--border-1)',
          borderRadius: 10, overflow: 'hidden', boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
        }}>
          {['fast', 'quality'].map(tierKey => {
            const tmk = TIER_META[tierKey]
            return (
              <div key={tierKey}>
                <div style={{ padding: '6px 12px 4px', fontSize: 9, fontWeight: 700, color: tmk.color, textTransform: 'uppercase', letterSpacing: '0.06em', borderBottom: '1px solid var(--border-1)', background: tmk.bg }}>
                  {tmk.label} MODELS
                </div>
                {(models[tierKey] || []).map(m => (
                  <button key={m.id} onClick={() => { onChange(m.id); setOpen(false) }}
                    style={{
                      width: '100%', padding: '9px 12px', textAlign: 'left', cursor: 'pointer',
                      background: value === m.id ? tmk.bg : 'transparent',
                      border: 'none', borderBottom: '1px solid var(--border-1)',
                      display: 'flex', alignItems: 'center', gap: 10,
                    }}>
                    <div style={{
                      width: 14, height: 14, borderRadius: '50%', flexShrink: 0,
                      border: `2px solid ${value === m.id ? tmk.color : 'var(--border-1)'}`,
                      background: value === m.id ? tmk.color : 'transparent',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}>
                      {value === m.id && <div style={{ width: 5, height: 5, borderRadius: '50%', background: '#fff' }} />}
                    </div>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)' }}>{m.label}</div>
                      <div style={{ fontSize: 10, color: 'var(--text-3)' }}>{m.note}</div>
                    </div>
                  </button>
                ))}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── Temperature Slider ────────────────────────────────────────────────────────
function TempSlider({ value, onChange }) {
  const color = value <= 0.3 ? '#3b82f6' : value <= 0.6 ? '#10b981' : '#f59e0b'
  const label = value <= 0.3 ? 'Precise' : value <= 0.6 ? 'Balanced' : 'Creative'
  return (
    <div style={{ background: 'var(--bg-base)', borderRadius: 8, padding: '10px 12px', border: '1.5px solid var(--border-1)' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
        <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Temperature</span>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 10, color, fontWeight: 600 }}>{label}</span>
          <span style={{ fontSize: 14, fontWeight: 800, color }}>{value.toFixed(2)}</span>
        </div>
      </div>
      <input type="range" min="0" max="1" step="0.05" value={value}
        onChange={e => onChange(parseFloat(e.target.value))}
        style={{ width: '100%', accentColor: color, cursor: 'pointer' }}
      />
      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 2 }}>
        <span style={{ fontSize: 9, color: 'var(--text-3)' }}>0 Precise</span>
        <span style={{ fontSize: 9, color: 'var(--text-3)' }}>1 Creative</span>
      </div>
    </div>
  )
}

// ── Module Editor Panel ───────────────────────────────────────────────────────
function ModuleEditor({ module, onChange, models }) {
  const [activeTab, setActiveTab] = useState('user')
  const meta  = MODULE_META[module.id] || { color: '#6366f1', icon: '◆' }
  const group = GROUPS.find(g => g.id === meta.group) || GROUPS[0]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, height: '100%' }}>

      {/* Module description */}
      <div style={{ padding: '12px 16px', borderRadius: 10, background: `${meta.color}0d`, border: `1px solid ${meta.color}30` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
          <span style={{ fontSize: 18 }}>{meta.icon}</span>
          <span style={{ fontSize: 14, fontWeight: 700, color: meta.color }}>{module.name}</span>
          <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: group.bg, color: group.color, border: `1px solid ${group.color}40`, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            {group.label}
          </span>
        </div>
        <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.55 }}>{module.description}</div>
      </div>

      {/* Model + Temperature */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        <div>
          <div style={labelStyle}>Model</div>
          <ModelSelector
            value={module.model}
            onChange={model => onChange({ ...module, model })}
            accentColor={meta.color}
            models={models}
          />
        </div>
        <div>
          <div style={labelStyle}>Temperature</div>
          <TempSlider
            value={module.temperature || 0.2}
            onChange={temperature => onChange({ ...module, temperature })}
          />
        </div>
      </div>

      {/* Prompt tabs */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        <div style={{ display: 'flex', gap: 0, borderBottom: '1.5px solid var(--border-1)', marginBottom: 12 }}>
          {[
            { key: 'user',   label: 'User Prompt Template' },
            { key: 'system', label: 'System Prompt' },
          ].map(tab => (
            <button key={tab.key} onClick={() => setActiveTab(tab.key)}
              style={{
                padding: '8px 18px', border: 'none', cursor: 'pointer', fontSize: 12, fontWeight: 700,
                background: 'transparent',
                color: activeTab === tab.key ? meta.color : 'var(--text-3)',
                borderBottom: activeTab === tab.key ? `2.5px solid ${meta.color}` : '2.5px solid transparent',
                transition: 'all 0.15s', marginBottom: -1.5,
              }}
            >{tab.label}</button>
          ))}
        </div>

        {activeTab === 'user' ? (
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8 }}>
            <div style={{ padding: '8px 10px', borderRadius: 7, background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)', fontSize: 10, color: '#a5b4fc', lineHeight: 1.5 }}>
              Use <code style={{ background: 'rgba(99,102,241,0.15)', padding: '1px 5px', borderRadius: 3 }}>{'{{profile_json}}'}</code> as a placeholder — it is replaced with the full BrightData profile JSON at runtime.
            </div>
            <textarea
              value={module.user_template || ''}
              onChange={e => onChange({ ...module, user_template: e.target.value })}
              rows={14}
              placeholder="User prompt template with {{profile_json}} placeholder..."
              style={{
                flex: 1, width: '100%', boxSizing: 'border-box', padding: '12px 14px', borderRadius: 9,
                border: `1.5px solid ${meta.color}33`, background: 'var(--bg-base)',
                color: 'var(--text-1)', fontSize: 12, lineHeight: 1.7,
                fontFamily: "'JetBrains Mono','Fira Code',monospace",
                resize: 'vertical', outline: 'none',
              }}
            />
            <div style={{ display: 'flex', gap: 12, fontSize: 10, color: 'var(--text-3)' }}>
              <span>{(module.user_template || '').length} chars</span>
              <span>·</span>
              <span>~{Math.round((module.user_template || '').length / 4)} tokens</span>
              <span>·</span>
              <span style={{ color: '#a5b4fc' }}>{'{{profile_json}}'} replaced at runtime</span>
            </div>
          </div>
        ) : (
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8 }}>
            <textarea
              value={module.system || ''}
              onChange={e => onChange({ ...module, system: e.target.value })}
              rows={8}
              placeholder="System role instruction..."
              style={{
                flex: 1, width: '100%', boxSizing: 'border-box', padding: '12px 14px', borderRadius: 9,
                border: `1.5px solid ${meta.color}33`, background: 'var(--bg-base)',
                color: 'var(--text-1)', fontSize: 12, lineHeight: 1.6,
                fontFamily: "'JetBrains Mono','Fira Code',monospace",
                resize: 'vertical', outline: 'none',
              }}
            />
            <div style={{ display: 'flex', gap: 12, fontSize: 10, color: 'var(--text-3)' }}>
              <span>{(module.system || '').length} chars</span>
              <span>·</span>
              <span>~{Math.round((module.system || '').length / 4)} tokens</span>
              <span style={{ marginLeft: 4, color: '#f59e0b' }}>· billed on every call — keep concise</span>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

const labelStyle = {
  display: 'block', fontSize: 10, fontWeight: 700,
  color: 'var(--text-2)', marginBottom: 5,
  textTransform: 'uppercase', letterSpacing: '0.05em',
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function LioConfigPage() {
  const [modules,        setModules]        = useState([])
  const [loading,        setLoading]        = useState(true)
  const [saving,         setSaving]         = useState(false)
  const [dirty,          setDirty]          = useState(false)
  const [selected,       setSelected]       = useState('identity')
  const [view,           setView]           = useState('module') // 'module' | 'models'
  const [enrichPrompt,   setEnrichPrompt]   = useState('')
  const [enrichModel,    setEnrichModel]    = useState('')
  const [enrichDirty,    setEnrichDirty]    = useState(false)
  const [enrichSaving,   setEnrichSaving]   = useState(false)
  const llmModels = useLlmModels()

  const load = useCallback(async () => {
    try {
      const [data, promptData] = await Promise.all([
        apiFetch('/api/v1/ai/config'),
        apiFetch('/api/leads/lio/prompt'),
      ])
      if (data.prompts && Array.isArray(data.prompts)) {
        setModules(data.prompts)
        if (data.prompts.length > 0) setSelected(data.prompts[0].id)
      }
      setEnrichPrompt(promptData.system_prompt || '')
      setEnrichModel(promptData.model || '')
    } catch (e) {
      toast.error('Failed to load AI config: ' + e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  const handleChange = (moduleId, updated) => {
    setModules(prev => prev.map(m => m.id === moduleId ? updated : m))
    setDirty(true)
  }

  const handleSave = async () => {
    setSaving(true)
    try {
      await apiFetch('/api/v1/ai/config', {
        method: 'PUT',
        body: JSON.stringify({ prompts: modules }),
      })
      toast.success('AI module configuration saved')
      setDirty(false)
    } catch (e) {
      toast.error('Save failed: ' + e.message)
    } finally {
      setSaving(false)
    }
  }

  const handleEnrichSave = async () => {
    setEnrichSaving(true)
    try {
      await apiFetch('/api/leads/lio/prompt', {
        method: 'POST',
        body: JSON.stringify({ system_prompt: enrichPrompt, model: enrichModel }),
      })
      toast.success('Enrichment system prompt saved')
      setEnrichDirty(false)
    } catch (e) {
      toast.error('Save failed: ' + e.message)
    } finally {
      setEnrichSaving(false)
    }
  }

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '80vh' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 36, height: 36, borderRadius: '50%', border: '3px solid var(--border-1)', borderTopColor: '#8b5cf6', animation: 'spin 0.8s linear infinite', margin: '0 auto 12px' }} />
          <div style={{ fontSize: 13, color: 'var(--text-3)' }}>Loading AI configuration…</div>
        </div>
      </div>
    )
  }

  const fastCount    = modules.filter(m => getTier(m.model, llmModels) === 'fast').length
  const qualityCount = modules.length - fastCount
  const estCost      = ((fastCount * 0.8 + qualityCount * 2.5) * 0.0001).toFixed(4)

  const activeModule = modules.find(m => m.id === selected)
  const grouped = GROUPS.map(g => ({
    ...g,
    modules: modules.filter(m => (MODULE_META[m.id]?.group || 'core') === g.id),
  }))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 56px)', overflow: 'hidden' }}>

      {/* ── Top bar ── */}
      <div style={{
        padding: '0 24px', height: 56, flexShrink: 0,
        borderBottom: '1px solid var(--border-1)',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16,
        background: 'var(--bg-card)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 9,
            background: 'linear-gradient(135deg,#8b5cf6,#6366f1)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
          }}>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
            </svg>
          </div>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)', lineHeight: 1.2 }}>AI Enrichment Config</div>
            <div style={{ fontSize: 10, color: 'var(--text-3)', lineHeight: 1.2 }}>10 modules · Groq LLM · per-org customisable</div>
          </div>
          <div style={{ width: 1, height: 28, background: 'var(--border-1)', margin: '0 4px' }} />
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <span style={{ fontSize: 10, fontWeight: 700, padding: '3px 8px', borderRadius: 5, background: TIER_META.fast.bg, color: TIER_META.fast.color, border: `1px solid ${TIER_META.fast.border}` }}>
              {fastCount} FAST
            </span>
            <span style={{ fontSize: 10, fontWeight: 700, padding: '3px 8px', borderRadius: 5, background: TIER_META.quality.bg, color: TIER_META.quality.color, border: `1px solid ${TIER_META.quality.border}` }}>
              {qualityCount} QUALITY
            </span>
            <span style={{ fontSize: 10, color: '#f59e0b', fontWeight: 700 }}>~${estCost}/lead</span>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {dirty && (
            <span style={{ fontSize: 11, color: '#c4b5fd', padding: '3px 10px', borderRadius: 5, background: 'rgba(139,92,246,0.1)', border: '1px solid rgba(139,92,246,0.2)' }}>
              Unsaved changes
            </span>
          )}
          <button onClick={handleSave} disabled={saving || !dirty}
            style={{
              padding: '6px 18px', borderRadius: 7, fontSize: 11, fontWeight: 700,
              cursor: saving || !dirty ? 'not-allowed' : 'pointer',
              background: dirty ? 'linear-gradient(135deg,#8b5cf6,#6366f1)' : 'var(--bg-base)',
              border: dirty ? 'none' : '1px solid var(--border-1)',
              color: dirty ? '#fff' : 'var(--text-3)',
              opacity: saving ? 0.7 : 1, transition: 'all 0.2s',
            }}>
            {saving ? 'Saving…' : dirty ? 'Save All' : 'Saved'}
          </button>
        </div>
      </div>

      {/* ── Body: sidebar + content ── */}
      <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>

        {/* ── Sidebar ── */}
        <div style={{
          width: 230, flexShrink: 0, borderRight: '1px solid var(--border-1)',
          background: 'var(--bg-card)', display: 'flex', flexDirection: 'column', overflowY: 'auto',
        }}>
          {grouped.map(group => (
            <div key={group.id}>
              <div style={{
                padding: '10px 14px 5px',
                fontSize: 9, fontWeight: 700, color: group.color,
                textTransform: 'uppercase', letterSpacing: '0.07em',
                display: 'flex', alignItems: 'center', gap: 6,
              }}>
                <div style={{ width: 6, height: 6, borderRadius: '50%', background: group.color }} />
                {group.label}
              </div>
              {group.modules.map(m => {
                const meta  = MODULE_META[m.id] || { color: '#6366f1', icon: '◆' }
                const tier  = getTier(m.model, llmModels)
                const tm    = TIER_META[tier]
                const active = view === 'module' && selected === m.id
                return (
                  <button key={m.id}
                    onClick={() => { setSelected(m.id); setView('module') }}
                    style={{
                      width: '100%', padding: '9px 14px', border: 'none', cursor: 'pointer', textAlign: 'left',
                      background: active ? `${meta.color}12` : 'transparent',
                      borderLeft: `3px solid ${active ? meta.color : 'transparent'}`,
                      transition: 'all 0.12s',
                      display: 'flex', alignItems: 'center', gap: 9,
                    }}
                  >
                    <span style={{ fontSize: 16, lineHeight: 1, flexShrink: 0 }}>{meta.icon}</span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 12, fontWeight: 700, color: active ? 'var(--text-1)' : 'var(--text-2)', lineHeight: 1.2 }}>{m.name}</div>
                      <div style={{ display: 'flex', gap: 5, marginTop: 2 }}>
                        <span style={{
                          fontSize: 8, fontWeight: 700, padding: '1px 5px', borderRadius: 3,
                          background: tm.bg, color: tm.color, border: `1px solid ${tm.border}`,
                          textTransform: 'uppercase',
                        }}>{tm.label}</span>
                        <span style={{ fontSize: 9, color: 'var(--text-3)', fontFamily: 'monospace', lineHeight: 1.6 }}>
                          temp {m.temperature || 0.2}
                        </span>
                      </div>
                    </div>
                  </button>
                )
              })}
            </div>
          ))}

          <div style={{ borderTop: '1px solid var(--border-1)', marginTop: 4 }}>
            <button
              onClick={() => setView('models')}
              style={{
                width: '100%', padding: '9px 14px', border: 'none', cursor: 'pointer', textAlign: 'left',
                background: view === 'models' ? 'rgba(251,191,36,0.08)' : 'transparent',
                borderLeft: `3px solid ${view === 'models' ? '#f59e0b' : 'transparent'}`,
                display: 'flex', alignItems: 'center', gap: 9, transition: 'all 0.12s',
              }}>
              <span style={{ fontSize: 14 }}>◈</span>
              <span style={{ fontSize: 12, fontWeight: 600, color: view === 'models' ? 'var(--text-1)' : 'var(--text-2)' }}>Model Reference</span>
            </button>
          </div>
        </div>

        {/* ── Main content area ── */}
        <div style={{ flex: 1, minWidth: 0, overflowY: 'auto', padding: '20px 24px' }}>

          {/* ── Enrichment Prompt — always visible at top ── */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12, maxWidth: 860, marginBottom: 24, padding: '14px 16px', borderRadius: 12, background: 'rgba(16,185,129,0.04)', border: '1px solid rgba(16,185,129,0.2)' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
              <div>
                <div style={{ fontSize: 13, fontWeight: 700, color: '#10b981', marginBottom: 2 }}>⚙ CRM Brief Prompt</div>
                <div style={{ fontSize: 11, color: 'var(--text-3)' }}>
                  Runs as a <strong style={{ color: 'var(--text-2)' }}>second LLM call</strong> after JSON enrichment — produces a human-readable brief stored as <code style={{ fontSize: 10, background: 'rgba(16,185,129,0.12)', padding: '1px 4px', borderRadius: 3 }}>crm_brief</code> in the output &amp; forwarded to LIO. Leave blank to skip.
                </div>
              </div>
              <button
                onClick={handleEnrichSave}
                disabled={enrichSaving || !enrichDirty}
                style={{
                  flexShrink: 0, padding: '6px 16px', borderRadius: 7, fontSize: 11, fontWeight: 700,
                  cursor: enrichSaving || !enrichDirty ? 'not-allowed' : 'pointer',
                  background: enrichDirty ? 'linear-gradient(135deg,#10b981,#059669)' : 'var(--bg-base)',
                  border: enrichDirty ? 'none' : '1px solid var(--border-1)',
                  color: enrichDirty ? '#fff' : 'var(--text-3)',
                  opacity: enrichSaving ? 0.7 : 1, transition: 'all 0.2s',
                }}>
                {enrichSaving ? 'Saving…' : enrichDirty ? 'Save Prompt' : 'Saved'}
              </button>
            </div>
            <textarea
              value={enrichPrompt}
              onChange={e => { setEnrichPrompt(e.target.value); setEnrichDirty(true) }}
              rows={4}
              placeholder={'Expert B2B lead intelligence analyst. Return ONLY valid JSON.'}
              style={{
                width: '100%', boxSizing: 'border-box', padding: '10px 12px', borderRadius: 8,
                border: '1.5px solid rgba(16,185,129,0.3)', background: 'var(--bg-base)',
                color: 'var(--text-1)', fontSize: 12, lineHeight: 1.7,
                fontFamily: "'JetBrains Mono','Fira Code',monospace",
                resize: 'vertical', outline: 'none',
              }}
            />
            <div style={{ display: 'flex', gap: 16, alignItems: 'center', flexWrap: 'wrap' }}>
              <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{enrichPrompt.length} chars · ~{Math.round(enrichPrompt.length / 4)} tokens</span>
              <div style={{ flex: '0 0 220px' }}>
                <ModelSelector value={enrichModel} onChange={v => { setEnrichModel(v); setEnrichDirty(true) }} accentColor="#10b981" models={llmModels} />
              </div>
              <span style={{ fontSize: 10, color: 'var(--text-3)' }}>Model override (optional)</span>
            </div>
          </div>

          {view === 'module' && activeModule && (
            <ModuleEditor
              module={activeModule}
              onChange={updated => handleChange(activeModule.id, updated)}
              models={llmModels}
            />
          )}

          {view === 'models' && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              <div>
                <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-1)', marginBottom: 4 }}>Groq Model Reference</div>
                <div style={{ fontSize: 12, color: 'var(--text-3)' }}>Choose the right model for each AI module — fast for classification, quality for strategy and copy.</div>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
                {['fast', 'quality'].map(tier => {
                  const tm = TIER_META[tier]
                  return (
                    <div key={tier} style={{ borderRadius: 12, border: `1px solid ${tm.border}`, overflow: 'hidden' }}>
                      <div style={{ padding: '10px 16px', background: tm.bg, borderBottom: `1px solid ${tm.border}` }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color: tm.color, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                          {tier === 'fast' ? '⚡ Fast / Cheap' : '✦ Quality'}
                        </div>
                      </div>
                      {(llmModels[tier] || []).map(m => (
                        <div key={m.id} style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-1)' }}>
                          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)', fontFamily: 'monospace', marginBottom: 2 }}>{m.id}</div>
                          <div style={{ fontSize: 11, color: 'var(--text-3)' }}>{m.note}</div>
                        </div>
                      ))}
                    </div>
                  )
                })}
              </div>

              {/* Module overview table */}
              <div style={{ borderRadius: 12, border: '1px solid var(--border-1)', overflow: 'hidden' }}>
                <div style={{ padding: '10px 16px', background: 'var(--bg-card)', borderBottom: '1px solid var(--border-1)', fontSize: 11, fontWeight: 700, color: 'var(--text-2)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  Module Overview
                </div>
                {grouped.map(group => (
                  <div key={group.id}>
                    <div style={{ padding: '6px 16px', background: group.bg, borderBottom: '1px solid var(--border-1)', fontSize: 9, fontWeight: 700, color: group.color, textTransform: 'uppercase', letterSpacing: '0.07em' }}>
                      {group.label}
                    </div>
                    {group.modules.map(m => {
                      const meta = MODULE_META[m.id] || { color: '#6366f1', icon: '◆' }
                      const tier = getTier(m.model, llmModels)
                      const tm   = TIER_META[tier]
                      return (
                        <div key={m.id} style={{ padding: '10px 16px', borderBottom: '1px solid var(--border-1)', display: 'flex', alignItems: 'center', gap: 12 }}>
                          <span style={{ fontSize: 18, flexShrink: 0 }}>{meta.icon}</span>
                          <div style={{ flex: 1 }}>
                            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)' }}>{m.name}</div>
                            <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 1 }}>{m.description}</div>
                          </div>
                          <div style={{ display: 'flex', gap: 8, flexShrink: 0, alignItems: 'center' }}>
                            <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 6px', borderRadius: 4, background: tm.bg, color: tm.color }}>{tm.label}</span>
                            <span style={{ fontSize: 10, color: 'var(--text-3)', fontFamily: 'monospace' }}>{m.temperature || 0.2}</span>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                ))}
              </div>

              <div style={{ padding: '14px 18px', borderRadius: 10, background: 'var(--bg-card)', border: '1px solid var(--border-1)', fontSize: 12, color: 'var(--text-2)', lineHeight: 1.7 }}>
                <strong style={{ color: 'var(--text-1)' }}>Pricing (approx 2026):</strong><br/>
                <span>llama-3.1-8b-instant → ~$0.0001/1K tokens</span><br/>
                <span>llama-3.3-70b-versatile → ~$0.0006/1K tokens</span><br/>
                <strong style={{ color: '#10b981' }}>Total per lead (Run All): ~$0.003–$0.005</strong>
                <span style={{ color: 'var(--text-3)' }}> vs ~$0.02 on Claude/GPT-4</span>
              </div>
            </div>
          )}

        </div>
      </div>
    </div>
  )
}