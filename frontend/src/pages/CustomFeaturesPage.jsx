// CustomFeaturesPage.jsx — Create, manage, and run custom AI feature endpoints
import { useState, useEffect, useRef } from 'react'
import toast from 'react-hot-toast'

const BACKEND = import.meta.env.VITE_BACKEND_URL || ''
const API     = `${BACKEND}/api`

function authHeaders() {
  const token = localStorage.getItem('wb_ai_token') || ''
  return { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }
}

const MODULES    = ['CUSTOM', 'TARO', 'LIO']
const TASK_TYPES = ['ANALYZE', 'GENERATE', 'SCORE', 'EXTRACT', 'CLASSIFY']
const GROQ_MODELS = [
  'llama-3.1-8b-instant',
  'llama-3.3-70b-versatile',
  'llama-3.1-70b-versatile',
  'mixtral-8x7b-32768',
  'gemma2-9b-it',
]
const PROVIDERS = [
  { value: 'groq',        label: 'Groq' },
  { value: 'huggingface', label: 'HuggingFace' },
  { value: 'worksbuddy',  label: 'WorksBuddy LLM' },
]

const DEFAULT_FORM = {
  feature_code:   '',
  feature_name:   '',
  module:         'CUSTOM',
  endpoint_slug:  '',
  task_type:      'ANALYZE',
  description:    '',
  system_prompt:  '',
  model_provider: 'groq',
  model_name:     'llama-3.1-8b-instant',
  temperature:    0.3,
  input_params:   [{ key: '', label: '', type: 'text', placeholder: '' }],
}

// ── Styles ───────────────────────────────────────────────────────────────────

const s = {
  label: {
    fontSize: 10, fontWeight: 700, color: 'var(--text-3)',
    textTransform: 'uppercase', letterSpacing: '0.06em',
    display: 'block', marginBottom: 6,
  },
  input: {
    width: '100%', background: 'var(--bg-input,#0d0d14)',
    border: '1px solid var(--border-1)', borderRadius: 8,
    color: 'var(--text-1)', fontSize: 13, padding: '9px 12px',
    boxSizing: 'border-box', outline: 'none', fontFamily: 'inherit',
  },
  select: {
    width: '100%', background: 'var(--bg-input,#0d0d14)',
    border: '1px solid var(--border-1)', borderRadius: 8,
    color: 'var(--text-1)', fontSize: 13, padding: '9px 12px',
    boxSizing: 'border-box', outline: 'none', cursor: 'pointer',
  },
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function CustomFeaturesPage() {
  const [features, setFeatures]       = useState([])
  const [loading, setLoading]         = useState(true)
  const [showForm, setShowForm]       = useState(false)
  const [editingId, setEditingId]     = useState(null)
  const [form, setForm]               = useState(DEFAULT_FORM)
  const [saving, setSaving]           = useState(false)
  const [deleting, setDeleting]       = useState(null)
  const [testOpen, setTestOpen]       = useState(null)   // feature id for test panel

  useEffect(() => { fetchFeatures() }, [])

  async function fetchFeatures() {
    try {
      const r    = await fetch(`${API}/v1/features`, { headers: authHeaders() })
      const data = await r.json()
      setFeatures(data.features || [])
    } catch {
      toast.error('Failed to load features')
    } finally {
      setLoading(false)
    }
  }

  function openCreate() {
    setEditingId(null)
    setForm(DEFAULT_FORM)
    setShowForm(true)
    setTestOpen(null)
  }

  function openEdit(feature) {
    setEditingId(feature.id)
    const params = typeof feature.input_params === 'string'
      ? JSON.parse(feature.input_params || '[]')
      : (feature.input_params || [])
    setForm({
      feature_code:   feature.feature_code   || '',
      feature_name:   feature.feature_name   || '',
      module:         feature.module         || 'CUSTOM',
      endpoint_slug:  feature.endpoint_slug  || '',
      task_type:      feature.task_type      || 'ANALYZE',
      description:    feature.description    || '',
      system_prompt:  feature.system_prompt  || '',
      model_provider: feature.model_provider || 'groq',
      model_name:     feature.model_name     || '',
      temperature:    feature.temperature    ?? 0.3,
      input_params:   params.length ? params : [{ key: '', label: '', type: 'text', placeholder: '' }],
    })
    setShowForm(true)
    setTestOpen(null)
  }

  async function saveFeature() {
    if (!form.feature_code.trim())  { toast.error('Feature code required'); return }
    if (!form.feature_name.trim())  { toast.error('Feature name required'); return }
    if (!form.endpoint_slug.trim()) { toast.error('Endpoint slug required'); return }

    const body = {
      ...form,
      endpoint_slug: form.endpoint_slug.replace(/[^a-z0-9-]/g, '-').toLowerCase(),
      input_params:  form.input_params.filter(p => p.key.trim()),
    }

    setSaving(true)
    try {
      const url    = editingId ? `${API}/v1/features/${editingId}` : `${API}/v1/features`
      const method = editingId ? 'PUT' : 'POST'
      const r      = await fetch(url, { method, headers: authHeaders(), body: JSON.stringify(body) })
      if (!r.ok) {
        const err = await r.json()
        throw new Error(err.detail || 'Save failed')
      }
      toast.success(editingId ? 'Feature updated!' : 'Feature created!')
      setShowForm(false)
      fetchFeatures()
    } catch (e) {
      toast.error(e.message || 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  async function deleteFeature(id) {
    if (!window.confirm('Delete this feature?')) return
    setDeleting(id)
    try {
      await fetch(`${API}/v1/features/${id}`, { method: 'DELETE', headers: authHeaders() })
      toast.success('Feature deleted')
      setFeatures(f => f.filter(x => x.id !== id))
    } catch {
      toast.error('Delete failed')
    } finally {
      setDeleting(null)
    }
  }

  const setField   = (k, v)          => setForm(f => ({ ...f, [k]: v }))
  const addParam   = ()              => setForm(f => ({ ...f, input_params: [...f.input_params, { key: '', label: '', type: 'text', placeholder: '' }] }))
  const removeParam = i              => setForm(f => ({ ...f, input_params: f.input_params.filter((_, idx) => idx !== i) }))
  const updateParam = (i, k, v)      => setForm(f => ({ ...f, input_params: f.input_params.map((p, idx) => idx === i ? { ...p, [k]: v } : p) }))

  return (
    <div style={{ padding: '28px 32px', maxWidth: 960 }}>
      {/* Page header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 28 }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 700, color: 'var(--text-1)', margin: 0 }}>Custom API Features</h1>
          <p style={{ fontSize: 13, color: 'var(--text-3)', margin: '6px 0 0' }}>
            Define new AI feature endpoints — they get their own URL and use the same AI runner pipeline.
          </p>
        </div>
        {!showForm && (
          <button onClick={openCreate} style={btnPrimary}>
            + New Feature
          </button>
        )}
      </div>

      {/* Form */}
      {showForm && (
        <FeatureForm
          form={form}
          editingId={editingId}
          saving={saving}
          setField={setField}
          addParam={addParam}
          removeParam={removeParam}
          updateParam={updateParam}
          onSave={saveFeature}
          onCancel={() => setShowForm(false)}
        />
      )}

      {/* List */}
      {!showForm && (
        loading ? (
          <div style={{ color: 'var(--text-3)', fontSize: 13 }}>Loading…</div>
        ) : features.length === 0 ? (
          <EmptyState onCreate={openCreate} />
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {features.map(f => (
              <FeatureCard
                key={f.id}
                feature={f}
                testOpen={testOpen === f.id}
                onToggleTest={() => setTestOpen(t => t === f.id ? null : f.id)}
                onEdit={() => openEdit(f)}
                onDelete={() => deleteFeature(f.id)}
                deleting={deleting === f.id}
              />
            ))}
          </div>
        )
      )}
    </div>
  )
}

// ── Feature form ──────────────────────────────────────────────────────────────

function FeatureForm({ form, editingId, saving, setField, addParam, removeParam, updateParam, onSave, onCancel }) {
  const [hfQuery,     setHfQuery]     = useState('')
  const [hfModels,    setHfModels]    = useState([])
  const [hfSearching, setHfSearching] = useState(false)
  const hfTimer = useRef(null)

  function onHfQueryChange(q) {
    setHfQuery(q)
    clearTimeout(hfTimer.current)
    if (!q.trim()) { setHfModels([]); return }
    hfTimer.current = setTimeout(() => searchHfModels(q), 400)
  }

  async function searchHfModels(q) {
    setHfSearching(true)
    try {
      const r    = await fetch(`${API}/v1/features/hf/models?q=${encodeURIComponent(q)}&limit=15`, { headers: authHeaders() })
      const data = await r.json()
      setHfModels(data.models || [])
    } catch {
      setHfModels([])
    } finally {
      setHfSearching(false)
    }
  }

  function onProviderChange(val) {
    setField('model_provider', val)
    setField('model_name', val === 'groq' ? 'llama-3.1-8b-instant' : '')
    setHfQuery(''); setHfModels([])
  }

  return (
    <div style={{
      border: '1px solid rgba(99,102,241,0.3)', borderRadius: 14,
      padding: '24px 28px', background: 'rgba(99,102,241,0.03)', marginBottom: 28,
    }}>
      <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-1)', marginBottom: 22 }}>
        {editingId ? 'Edit API Feature' : 'New API Feature'}
      </div>

      {/* Row 1 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
        <Field label={<>FEATURE CODE <Required /> <Hint>e.g. CUSTOM_001</Hint></>}>
          <input
            value={form.feature_code}
            onChange={e => setField('feature_code', e.target.value.toUpperCase().replace(/\s/g, '_'))}
            placeholder="CUSTOM_001"
            style={s.input}
          />
        </Field>
        <Field label={<>FEATURE NAME <Required /></>}>
          <input
            value={form.feature_name}
            onChange={e => setField('feature_name', e.target.value)}
            placeholder="e.g. Custom Lead Scorer"
            style={s.input}
          />
        </Field>
      </div>

      {/* Row 2 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
        <Field label={<>MODULE <Hint>e.g. TARO / LIO / CUSTOM</Hint></>}>
          <select value={form.module} onChange={e => setField('module', e.target.value)} style={s.select}>
            {MODULES.map(m => <option key={m} value={m}>{m}</option>)}
          </select>
        </Field>
        <Field label={<>ENDPOINT SLUG <Required /> <Hint>→ /api/v1/features/run/your-slug</Hint></>}>
          <input
            value={form.endpoint_slug}
            onChange={e => setField('endpoint_slug', e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, '-'))}
            placeholder="e.g. lead-scorer"
            style={s.input}
          />
        </Field>
      </div>

      {/* Row 3 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
        <Field label="TASK TYPE">
          <select value={form.task_type} onChange={e => setField('task_type', e.target.value)} style={s.select}>
            {TASK_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="DESCRIPTION">
          <input
            value={form.description}
            onChange={e => setField('description', e.target.value)}
            placeholder="Brief description of what this feature does"
            style={s.input}
          />
        </Field>
      </div>

      {/* Row 4: Model Provider + Model */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
        <Field label="MODEL PROVIDER">
          <select value={form.model_provider} onChange={e => onProviderChange(e.target.value)} style={s.select}>
            {PROVIDERS.map(p => <option key={p.value} value={p.value}>{p.label}</option>)}
          </select>
        </Field>
        <Field label={form.model_provider === 'huggingface' ? 'HUGGINGFACE MODEL ID' : 'MODEL NAME'}>
          {form.model_provider === 'groq' ? (
            <select value={form.model_name} onChange={e => setField('model_name', e.target.value)} style={s.select}>
              {GROQ_MODELS.map(m => <option key={m} value={m}>{m}</option>)}
            </select>
          ) : form.model_provider === 'huggingface' ? (
            <HFModelSearch
              value={form.model_name}
              query={hfQuery}
              models={hfModels}
              searching={hfSearching}
              onQueryChange={onHfQueryChange}
              onSelect={m => { setField('model_name', m); setHfQuery(''); setHfModels([]) }}
            />
          ) : (
            <input
              value={form.model_name}
              onChange={e => setField('model_name', e.target.value)}
              placeholder="e.g. llama-3.1-8b-instant"
              style={s.input}
            />
          )}
        </Field>
      </div>

      {/* Temperature */}
      <div style={{ marginBottom: 16 }}>
        <label style={s.label}>
          TEMPERATURE &nbsp;
          <span style={{ color: '#a5b4fc', fontWeight: 800, fontSize: 12, textTransform: 'none' }}>
            {form.temperature.toFixed(2)}
          </span>
        </label>
        <input
          type="range" min="0" max="1" step="0.05"
          value={form.temperature}
          onChange={e => setField('temperature', parseFloat(e.target.value))}
          style={{ width: '100%', accentColor: '#6366f1', cursor: 'pointer', marginBottom: 4 }}
        />
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: 'var(--text-3)' }}>
          <span>Precise (0.0)</span><span>Balanced (0.5)</span><span>Creative (1.0)</span>
        </div>
      </div>

      {/* System Prompt */}
      <div style={{ marginBottom: 16 }}>
        <label style={s.label}>SYSTEM PROMPT</label>
        <textarea
          value={form.system_prompt}
          onChange={e => setField('system_prompt', e.target.value)}
          placeholder="You are an expert AI… Use {param_key} to inject input values into this prompt."
          style={{ ...s.input, height: 130, resize: 'vertical', fontFamily: 'monospace', fontSize: 12 }}
        />
        <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 4 }}>
          Use <code style={{ color: '#a5b4fc' }}>{'{param_key}'}</code> to inject input values. Return ONLY valid JSON.
        </div>
      </div>

      {/* Input Params */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
          <label style={{ ...s.label, margin: 0 }}>INPUT PARAMETERS</label>
          <button onClick={addParam} style={btnOutline}>+ Add Param</button>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {form.input_params.map((param, i) => (
            <div key={i} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 110px 1fr 32px', gap: 8, alignItems: 'center' }}>
              <input
                value={param.key}
                onChange={e => updateParam(i, 'key', e.target.value.toLowerCase().replace(/\s/g, '_'))}
                placeholder="key"
                style={{ ...s.input, margin: 0 }}
              />
              <input
                value={param.label}
                onChange={e => updateParam(i, 'label', e.target.value)}
                placeholder="Label"
                style={{ ...s.input, margin: 0 }}
              />
              <select
                value={param.type}
                onChange={e => updateParam(i, 'type', e.target.value)}
                style={{ ...s.select, margin: 0 }}
              >
                <option value="text">text</option>
                <option value="textarea">textarea</option>
                <option value="number">number</option>
                <option value="url">url</option>
                <option value="email">email</option>
                <option value="json">json</option>
              </select>
              <input
                value={param.placeholder}
                onChange={e => updateParam(i, 'placeholder', e.target.value)}
                placeholder="Placeholder"
                style={{ ...s.input, margin: 0 }}
              />
              <button
                onClick={() => removeParam(i)}
                style={{
                  background: 'transparent', border: 'none', color: '#f87171',
                  cursor: 'pointer', fontSize: 18, padding: 0,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  width: 32, height: 32,
                }}
              >×</button>
            </div>
          ))}
        </div>
      </div>

      {/* Actions */}
      <div style={{ display: 'flex', gap: 12 }}>
        <button onClick={onSave} disabled={saving} style={saving ? btnDisabled : btnPrimary}>
          {saving ? 'Saving…' : (editingId ? 'Update Feature' : 'Create Feature')}
        </button>
        <button onClick={onCancel} style={btnGhost}>Cancel</button>
      </div>
    </div>
  )
}

// ── Feature card ──────────────────────────────────────────────────────────────

function FeatureCard({ feature, testOpen, onToggleTest, onEdit, onDelete, deleting }) {
  const params = typeof feature.input_params === 'string'
    ? JSON.parse(feature.input_params || '[]')
    : (feature.input_params || [])

  const providerBadge = {
    groq:        { bg: 'rgba(16,185,129,0.12)',  color: '#34d399' },
    huggingface: { bg: 'rgba(245,158,11,0.12)',  color: '#fbbf24' },
    worksbuddy:  { bg: 'rgba(99,102,241,0.12)',  color: '#a5b4fc' },
  }[feature.model_provider] || { bg: 'rgba(107,114,128,0.12)', color: '#9ca3af' }

  return (
    <div style={{
      background: 'var(--bg-card,#111)',
      border: '1px solid var(--border-1)', borderRadius: 12, overflow: 'hidden',
    }}>
      <div style={{ padding: '16px 20px', display: 'flex', gap: 16, alignItems: 'flex-start' }}>
        {/* Info */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 8, marginBottom: 6 }}>
            <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)' }}>
              {feature.feature_name}
            </span>
            <Badge bg="rgba(99,102,241,0.15)" color="#a5b4fc">{feature.module}</Badge>
            <Badge bg="rgba(16,185,129,0.12)" color="#34d399">{feature.task_type}</Badge>
            {!feature.is_active && <Badge bg="rgba(239,68,68,0.12)" color="#f87171">INACTIVE</Badge>}
          </div>
          <div style={{ fontFamily: 'monospace', fontSize: 11, color: '#a5b4fc', marginBottom: 6 }}>
            POST /api/v1/features/run/{feature.endpoint_slug}
          </div>
          {feature.description && (
            <div style={{ fontSize: 12, color: 'var(--text-3)', marginBottom: 8 }}>
              {feature.description}
            </div>
          )}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 14, fontSize: 11, color: 'var(--text-3)' }}>
            <span>Code: <b style={{ color: 'var(--text-2)' }}>{feature.feature_code}</b></span>
            <span style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
              Model:
              <span style={{ fontSize: 9, padding: '2px 7px', borderRadius: 4, fontWeight: 700, ...providerBadge }}>
                {(feature.model_provider || 'groq').toUpperCase()}
              </span>
              {feature.model_name && (
                <b style={{ color: 'var(--text-2)', fontFamily: 'monospace', fontSize: 10 }}>
                  {feature.model_name}
                </b>
              )}
            </span>
            {params.length > 0 && (
              <span>{params.length} param{params.length !== 1 ? 's' : ''}</span>
            )}
            <span>temp: {feature.temperature ?? 0.3}</span>
          </div>
        </div>

        {/* Actions */}
        <div style={{ display: 'flex', gap: 8, flexShrink: 0 }}>
          <button onClick={onToggleTest} style={btnOutline}>
            {testOpen ? 'Close Test' : '▶ Test'}
          </button>
          <button onClick={onEdit} style={btnOutline}>Edit</button>
          <button onClick={onDelete} disabled={deleting} style={btnDanger}>
            {deleting ? '…' : 'Delete'}
          </button>
        </div>
      </div>

      {/* Test panel */}
      {testOpen && <TestPanel feature={feature} params={params} />}
    </div>
  )
}

// ── Test panel ────────────────────────────────────────────────────────────────

// Recursively parse any string values that look like JSON (model sometimes double-encodes)
function deepParseStrings(val) {
  if (typeof val === 'string') {
    const s = val.trim()
    if (s.startsWith('{') || s.startsWith('[')) {
      try { return deepParseStrings(JSON.parse(s)) } catch { /* keep as string */ }
    }
    return val
  }
  if (Array.isArray(val)) return val.map(deepParseStrings)
  if (val && typeof val === 'object') {
    return Object.fromEntries(Object.entries(val).map(([k, v]) => [k, deepParseStrings(v)]))
  }
  return val
}

function buildUserMessage(params, inputs) {
  const lines = []
  const defined = new Set(params.map(p => p.key))
  for (const p of params) {
    if (inputs[p.key]) lines.push(`${p.label || p.key}: ${inputs[p.key]}`)
  }
  for (const [k, v] of Object.entries(inputs)) {
    if (!defined.has(k) && v) lines.push(`${k}: ${v}`)
  }
  return lines.join('\n') || JSON.stringify(inputs)
}

function fillTemplate(tpl, inputs) {
  let r = tpl || ''
  for (const [k, v] of Object.entries(inputs)) r = r.replaceAll(`{${k}}`, String(v))
  return r
}

function TestPanel({ feature, params }) {
  const [inputs,       setInputs]       = useState(() => Object.fromEntries(params.map(p => [p.key, ''])))
  const [sysPrompt,    setSysPrompt]    = useState(feature.system_prompt || '')
  const [activeTab,    setActiveTab]    = useState('params')   // 'params' | 'system' | 'user' | 'result'
  const [result,       setResult]       = useState(null)
  const [running,      setRunning]      = useState(false)

  // Live previews
  const resolvedSystem = fillTemplate(sysPrompt, inputs)
  const resolvedUser   = buildUserMessage(params, inputs)

  async function run() {
    setRunning(true)
    setResult(null)
    try {
      const body = {
        inputs,
        system_prompt_override: sysPrompt !== feature.system_prompt ? sysPrompt : undefined,
      }
      const r    = await fetch(
        `${API}/v1/features/run/${feature.endpoint_slug}`,
        { method: 'POST', headers: authHeaders(), body: JSON.stringify(body) },
      )
      const data = await r.json()
      if (!r.ok) throw new Error(data.detail || 'Run failed')
      setResult(data)
      setActiveTab('result')
    } catch (e) {
      toast.error(e.message || 'Run failed')
    } finally {
      setRunning(false)
    }
  }

  const tabs = [
    { id: 'params', label: `Params (${params.length})` },
    { id: 'system', label: 'System Prompt' },
    { id: 'user',   label: 'User Prompt' },
    ...(result ? [{ id: 'result', label: 'Result' }] : []),
  ]

  return (
    <div style={{ borderTop: '1px solid var(--border-1)', background: 'rgba(0,0,0,0.25)' }}>
      {/* Tab bar */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 0, borderBottom: '1px solid var(--border-1)', paddingLeft: 20 }}>
        {tabs.map(t => (
          <button
            key={t.id}
            onClick={() => setActiveTab(t.id)}
            style={{
              background: 'transparent', border: 'none', cursor: 'pointer',
              padding: '9px 14px', fontSize: 11, fontWeight: 600,
              color: activeTab === t.id ? '#a5b4fc' : 'var(--text-3)',
              borderBottom: activeTab === t.id ? '2px solid #6366f1' : '2px solid transparent',
              marginBottom: -1,
            }}
          >{t.label}</button>
        ))}
        <div style={{ flex: 1 }} />
        <button
          onClick={run}
          disabled={running}
          style={{
            ...(running ? btnDisabled : btnPrimary),
            margin: '6px 16px 6px 0', padding: '6px 16px', fontSize: 12,
          }}
        >
          {running ? 'Running…' : '▶ Run'}
        </button>
      </div>

      <div style={{ padding: '16px 20px' }}>

        {/* Params tab */}
        {activeTab === 'params' && (
          params.length > 0 ? (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(240px, 1fr))', gap: 12 }}>
              {params.map(p => (
                <div key={p.key}>
                  <label style={{ ...s.label, marginBottom: 5 }}>
                    {p.label || p.key}
                    <span style={{ fontWeight: 400, textTransform: 'none', letterSpacing: 0, color: '#6366f1', marginLeft: 6, fontFamily: 'monospace', fontSize: 9 }}>
                      {'{'}{ p.key }{'}'}
                    </span>
                  </label>
                  {p.type === 'textarea' ? (
                    <textarea
                      value={inputs[p.key] || ''}
                      onChange={e => setInputs(i => ({ ...i, [p.key]: e.target.value }))}
                      placeholder={p.placeholder}
                      style={{ ...s.input, height: 80, resize: 'vertical', fontSize: 12, fontFamily: 'inherit' }}
                    />
                  ) : (
                    <input
                      type={p.type === 'number' ? 'number' : 'text'}
                      value={inputs[p.key] || ''}
                      onChange={e => setInputs(i => ({ ...i, [p.key]: e.target.value }))}
                      placeholder={p.placeholder}
                      style={s.input}
                    />
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div style={{ fontSize: 12, color: 'var(--text-3)' }}>
              No input params defined — feature will run with empty inputs.
            </div>
          )
        )}

        {/* System Prompt tab — editable */}
        {activeTab === 'system' && (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
              <div style={{ fontSize: 11, color: 'var(--text-3)' }}>
                Edits here only apply to this test run, they do NOT save to the feature.
              </div>
              {sysPrompt !== feature.system_prompt && (
                <button
                  onClick={() => setSysPrompt(feature.system_prompt || '')}
                  style={{ ...btnOutline, fontSize: 10 }}
                >Reset</button>
              )}
            </div>
            <textarea
              value={sysPrompt}
              onChange={e => setSysPrompt(e.target.value)}
              style={{ ...s.input, height: 180, resize: 'vertical', fontFamily: 'monospace', fontSize: 12 }}
              placeholder="System prompt…"
            />
            {Object.values(inputs).some(Boolean) && (
              <div style={{ marginTop: 12 }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
                  Preview (with inputs filled in)
                </div>
                <pre style={preStyle}>{resolvedSystem}</pre>
              </div>
            )}
          </div>
        )}

        {/* User Prompt tab — read-only preview */}
        {activeTab === 'user' && (
          <div>
            <div style={{ fontSize: 11, color: 'var(--text-3)', marginBottom: 8 }}>
              This is the user message sent to the model, built from your input param values.
            </div>
            <pre style={preStyle}>{resolvedUser || '(fill in params to see preview)'}</pre>
          </div>
        )}

        {/* Result tab */}
        {activeTab === 'result' && result && (
          <div>
            {result.token_usage && (
              <div style={{ fontSize: 10, color: 'var(--text-3)', marginBottom: 8, display: 'flex', gap: 12 }}>
                <span>provider: <b style={{ color: 'var(--text-2)' }}>{result.token_usage.provider}</b></span>
                <span>model: <b style={{ color: 'var(--text-2)' }}>{result.token_usage.model}</b></span>
                <span>tokens: <b style={{ color: 'var(--text-2)' }}>{result.token_usage.total_tokens}</b></span>
              </div>
            )}
            <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
              Result
            </div>
            <pre style={{ ...preStyle, color: '#6ee7b7', borderColor: 'rgba(16,185,129,0.25)' }}>
              {JSON.stringify(deepParseStrings(result.result), null, 2)}
            </pre>
          </div>
        )}

      </div>
    </div>
  )
}

const preStyle = {
  background: 'rgba(0,0,0,0.35)', border: '1px solid var(--border-1)',
  borderRadius: 8, padding: '12px 14px', fontSize: 11,
  color: 'var(--text-2)', overflowX: 'auto', whiteSpace: 'pre-wrap',
  wordBreak: 'break-word', maxHeight: 300, overflowY: 'auto',
  margin: 0, fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
}

// ── HuggingFace model search ──────────────────────────────────────────────────

function HFModelSearch({ value, query, models, searching, onQueryChange, onSelect }) {
  const [open, setOpen] = useState(false)

  return (
    <div style={{ position: 'relative' }}>
      <input
        value={query || (open ? '' : value)}
        onChange={e => { onQueryChange(e.target.value); setOpen(true) }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 200)}
        placeholder="Search HuggingFace models…"
        style={s.input}
      />
      {value && !query && (
        <div style={{ fontSize: 10, color: '#a5b4fc', marginTop: 4, fontFamily: 'monospace' }}>
          Selected: {value}
        </div>
      )}
      {searching && (
        <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 4 }}>Searching…</div>
      )}
      {open && models.length > 0 && (
        <div style={{
          position: 'absolute', top: 'calc(100% + 4px)', left: 0, right: 0, zIndex: 200,
          background: '#13131e', border: '1px solid var(--border-1)',
          borderRadius: 8, maxHeight: 220, overflowY: 'auto', boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
        }}>
          {models.map(m => (
            <div
              key={m.id}
              onMouseDown={() => onSelect(m.id)}
              style={{
                padding: '9px 14px', cursor: 'pointer', fontSize: 12,
                borderBottom: '1px solid var(--border-1)',
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                transition: 'background 0.1s',
              }}
              onMouseEnter={e => e.currentTarget.style.background = 'rgba(99,102,241,0.1)'}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
            >
              <span style={{ color: 'var(--text-1)', fontFamily: 'monospace' }}>{m.id}</span>
              <span style={{ color: 'var(--text-3)', fontSize: 10 }}>
                ↓{m.downloads > 1000 ? `${(m.downloads / 1000).toFixed(0)}k` : m.downloads}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Empty state ───────────────────────────────────────────────────────────────

function EmptyState({ onCreate }) {
  return (
    <div style={{
      border: '1px dashed var(--border-1)', borderRadius: 14,
      padding: '56px 32px', textAlign: 'center',
    }}>
      <div style={{ fontSize: 36, marginBottom: 14 }}>⚡</div>
      <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-1)', marginBottom: 8 }}>
        No custom features yet
      </div>
      <div style={{ fontSize: 13, color: 'var(--text-3)', marginBottom: 24 }}>
        Create your first AI feature endpoint with a custom prompt and model
      </div>
      <button onClick={onCreate} style={btnPrimary}>+ New Feature</button>
    </div>
  )
}

// ── Small helpers ─────────────────────────────────────────────────────────────

function Field({ label, children }) {
  return (
    <div>
      <label style={s.label}>{label}</label>
      {children}
    </div>
  )
}

function Badge({ bg, color, children }) {
  return (
    <span style={{
      fontSize: 9, padding: '2px 7px', borderRadius: 4, fontWeight: 700,
      background: bg, color,
    }}>
      {children}
    </span>
  )
}

function Required() {
  return <span style={{ color: '#f87171' }}>*</span>
}

function Hint({ children }) {
  return (
    <span style={{ fontWeight: 400, textTransform: 'none', letterSpacing: 0, color: 'var(--text-3)', fontSize: 10 }}>
      {' '}{children}
    </span>
  )
}

// ── Button styles ─────────────────────────────────────────────────────────────

const btnPrimary = {
  background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
  color: '#fff', border: 'none', borderRadius: 9,
  padding: '9px 20px', fontSize: 13, fontWeight: 600, cursor: 'pointer',
}
const btnGhost = {
  background: 'transparent', border: '1px solid var(--border-1)',
  color: 'var(--text-2)', borderRadius: 9,
  padding: '9px 18px', fontSize: 13, fontWeight: 600, cursor: 'pointer',
}
const btnOutline = {
  background: 'transparent', border: '1px solid var(--border-1)',
  color: 'var(--text-3)', cursor: 'pointer', borderRadius: 7,
  padding: '5px 12px', fontSize: 11, fontWeight: 600,
}
const btnDanger = {
  background: 'transparent', border: '1px solid rgba(239,68,68,0.3)',
  color: '#f87171', cursor: 'pointer', borderRadius: 7,
  padding: '5px 12px', fontSize: 11, fontWeight: 600,
}
const btnDisabled = {
  background: '#374151', color: '#9ca3af', border: 'none',
  borderRadius: 9, padding: '9px 20px', fontSize: 13,
  fontWeight: 600, cursor: 'not-allowed',
}
