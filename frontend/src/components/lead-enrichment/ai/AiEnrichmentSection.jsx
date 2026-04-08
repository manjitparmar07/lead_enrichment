import { useState, useEffect } from 'react'
import { Sparkles, Check, AlertCircle, RefreshCw, ChevronDown, Loader, Zap } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { BACKEND, jsonHdr } from '../../../utils/api'
import { AI_MODULES } from '../../../utils/leadConfig.jsx'
import {
  AiNluIdentity, AiNluContact, AiNluScores, AiNluIcpMatch,
  AiNluBehavioural, AiNluPitch, AiNluActivity, AiNluTags,
  AiNluOutreach, AiNluPersona,
} from './nlu'

const AI_NLU_MAP = {
  identity:            AiNluIdentity,
  contact:             AiNluContact,
  scores:              AiNluScores,
  icp_match:           AiNluIcpMatch,
  behavioural_signals: AiNluBehavioural,
  pitch_intelligence:  AiNluPitch,
  activity:            AiNluActivity,
  tags:                AiNluTags,
  outreach:            AiNluOutreach,
  persona_analysis:    AiNluPersona,
}

export default function AiEnrichmentSection({ lead }) {
  const [results,    setResults]    = useState({})   // { moduleId: parsedObject }
  const [statuses,   setStatuses]   = useState({})   // { moduleId: 'idle'|'running'|'done'|'error' }
  const [errors,     setErrors]     = useState({})   // { moduleId: msg }
  const [open,       setOpen]       = useState(new Set())
  const [viewMode,   setViewMode]   = useState({})   // { moduleId: 'nlu'|'json' }
  const [runningAll, setRunningAll] = useState(false)
  const [rawProfile, setRawProfile] = useState(null)

  // Fetch full lead (includes raw_profile) when lead changes
  useEffect(() => {
    if (!lead?.id) return
    setResults({}); setStatuses({}); setErrors({}); setRawProfile(null)
    fetch(`${BACKEND}/leads/${lead.id}`, { headers: jsonHdr() })
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.raw_profile) setRawProfile(d.raw_profile) })
      .catch(() => {})
  }, [lead?.id])

  const getProfile = () => {
    if (!rawProfile) return null
    const p = typeof rawProfile === 'string' ? JSON.parse(rawProfile) : { ...rawProfile }
    delete p._activity_full
    return p
  }

  const runModule = async (moduleId) => {
    const profile = getProfile()
    if (!profile) { toast.error('Profile data not loaded yet'); return null }
    const mod = AI_MODULES.find(m => m.id === moduleId)
    setStatuses(p => ({ ...p, [moduleId]: 'running' }))
    setErrors(p => ({ ...p, [moduleId]: null }))
    setOpen(p => new Set([...p, moduleId]))
    setViewMode(p => ({ ...p, [moduleId]: 'nlu' }))
    try {
      const r = await fetch(`${BACKEND}/v1/ai/${mod.route}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile }),
      })
      if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`) }
      const data = await r.json()
      setResults(p => ({ ...p, [moduleId]: data }))
      setStatuses(p => ({ ...p, [moduleId]: 'done' }))
      return data
    } catch (e) {
      setStatuses(p => ({ ...p, [moduleId]: 'error' }))
      setErrors(p => ({ ...p, [moduleId]: e.message }))
      return null
    }
  }

  const runAll = async () => {
    const profile = getProfile()
    if (!profile) { toast.error('Profile data not loaded yet'); return }
    setRunningAll(true)
    setResults({}); setStatuses({}); setErrors({})
    // Mark all as running
    const running = {}
    AI_MODULES.forEach(m => { running[m.id] = 'running' })
    setStatuses(running)
    setOpen(new Set(AI_MODULES.map(m => m.id)))
    try {
      const r = await fetch(`${BACKEND}/v1/ai/full-enrichment`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile }),
      })
      if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`) }
      const data = await r.json()
      const newStatuses = {}
      const newResults  = {}
      AI_MODULES.forEach(m => {
        if (data[m.id]) {
          newResults[m.id]  = data[m.id]
          newStatuses[m.id] = data[m.id].error ? 'error' : 'done'
        } else {
          newStatuses[m.id] = 'idle'
        }
      })
      setResults(newResults)
      setStatuses(newStatuses)
      setViewMode(Object.fromEntries(AI_MODULES.map(m => [m.id, 'nlu'])))
    } catch (e) {
      toast.error('Full enrichment failed: ' + e.message)
      const errStatuses = {}
      AI_MODULES.forEach(m => { errStatuses[m.id] = 'idle' })
      setStatuses(errStatuses)
    } finally {
      setRunningAll(false)
    }
  }

  const anyRunning = runningAll || Object.values(statuses).includes('running')
  const doneCount  = AI_MODULES.filter(m => statuses[m.id] === 'done').length

  const renderModule = (mod) => {
    const status  = statuses[mod.id] || 'idle'
    const result  = results[mod.id]
    const err     = errors[mod.id]
    const isOpen  = open.has(mod.id)
    const mode    = viewMode[mod.id] || 'nlu'
    const NluComp = AI_NLU_MAP[mod.id]
    const tu      = result?.token_usage

    const statusDot = {
      idle:    <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'var(--bg-card)', border: '1px solid var(--border-1)', display: 'inline-block' }} />,
      running: <span style={{ width: 7, height: 7, borderRadius: '50%', background: '#f59e0b', display: 'inline-block', animation: 'pulse 1s ease-in-out infinite' }} />,
      done:    <Check size={10} color={mod.color} strokeWidth={3} />,
      error:   <AlertCircle size={10} color="#ef4444" />,
    }[status]

    return (
      <div key={mod.id} style={{ borderRadius: 10, border: `1px solid ${status === 'done' ? mod.color + '33' : status === 'error' ? 'rgba(239,68,68,0.3)' : 'var(--border-1)'}`, overflow: 'hidden', transition: 'border-color 0.2s' }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '9px 12px', background: 'var(--bg-card)', cursor: 'pointer' }}
          onClick={() => setOpen(p => { const s = new Set(p); isOpen ? s.delete(mod.id) : s.add(mod.id); return s })}>
          <div style={{ width: 22, height: 22, borderRadius: 6, background: mod.color + '22', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, flexShrink: 0 }}>{mod.icon}</div>
          {statusDot}
          <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)', flex: 1 }}>{mod.name}</span>
          {status === 'done' && <span style={{ fontSize: 9, fontWeight: 700, color: mod.color, letterSpacing: '0.06em', textTransform: 'uppercase' }}>DONE</span>}
          {status === 'running' && <span style={{ fontSize: 9, color: '#f59e0b', fontWeight: 600 }}>Analyzing…</span>}
          {tu && <span style={{ fontSize: 9, color: 'var(--text-3)' }}>{tu.total_tokens}t</span>}
          {status !== 'running' && (
            <button onClick={e => { e.stopPropagation(); runModule(mod.id) }}
              style={{ padding: '3px 9px', borderRadius: 6, border: `1px solid ${mod.color}44`, background: 'transparent', color: mod.color, fontSize: 10, fontWeight: 600, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
              <RefreshCw size={9} /> {status === 'done' ? 'Re-run' : 'Run'}
            </button>
          )}
          <ChevronDown size={12} color="var(--text-3)" style={{ transform: isOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s', flexShrink: 0 }} />
        </div>

        {/* Body */}
        {isOpen && (
          <div style={{ borderTop: '1px solid var(--border-1)', padding: '12px 14px', background: 'var(--bg-elevated)' }}>
            {err && <div style={{ padding: '8px 10px', borderRadius: 7, background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)', fontSize: 12, color: '#fca5a5', marginBottom: 8 }}>{err}</div>}
            {status === 'running' && <div style={{ fontSize: 12, color: 'var(--text-3)', textAlign: 'center', padding: '16px 0' }}>Analyzing with Groq…</div>}
            {result && status !== 'running' && (
              <>
                {/* View toggle */}
                <div style={{ display: 'flex', gap: 4, marginBottom: 10 }}>
                  {['nlu', 'json'].map(v => (
                    <button key={v} onClick={() => setViewMode(p => ({ ...p, [mod.id]: v }))}
                      style={{ padding: '3px 10px', borderRadius: 6, border: `1px solid ${mode === v ? mod.color : 'var(--border-1)'}`, background: mode === v ? mod.color + '18' : 'transparent', color: mode === v ? mod.color : 'var(--text-3)', fontSize: 10, fontWeight: 700, cursor: 'pointer', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                      {v === 'nlu' ? '✦ NLU' : '{ } JSON'}
                    </button>
                  ))}
                  {tu && <span style={{ marginLeft: 'auto', fontSize: 10, color: 'var(--text-3)', alignSelf: 'center' }}>
                    {tu.prompt_tokens}+{tu.completion_tokens} tokens · {tu.model}
                  </span>}
                </div>
                {mode === 'nlu'
                  ? <NluComp d={result} />
                  : <pre style={{ margin: 0, fontSize: 11, lineHeight: 1.6, color: 'var(--text-1)', whiteSpace: 'pre-wrap', wordBreak: 'break-all', fontFamily: "'JetBrains Mono','Fira Code',monospace", maxHeight: 400, overflowY: 'auto' }}>
                      {JSON.stringify(result, null, 2)}
                    </pre>
                }
              </>
            )}
            {!result && status === 'idle' && (
              <div style={{ textAlign: 'center', padding: '16px 0', fontSize: 12, color: 'var(--text-3)' }}>Click Run to analyse</div>
            )}
          </div>
        )}
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', borderRadius: 10, background: 'var(--bg-card)', border: '1px solid var(--border-1)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Sparkles size={14} color="#6366f1" />
          <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-1)' }}>AI Enrichment</span>
          <span style={{ fontSize: 10, color: 'var(--text-3)' }}>· 10 modules · Groq</span>
          {doneCount > 0 && <span style={{ fontSize: 10, fontWeight: 700, color: '#10b981' }}>{doneCount}/{AI_MODULES.length} done</span>}
        </div>
        <button onClick={runAll} disabled={anyRunning || !rawProfile}
          style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '6px 14px', borderRadius: 8, border: 'none', background: anyRunning ? 'rgba(99,102,241,0.3)' : 'rgba(99,102,241,0.85)', color: '#fff', fontSize: 12, fontWeight: 700, cursor: anyRunning || !rawProfile ? 'not-allowed' : 'pointer', opacity: !rawProfile ? 0.5 : 1 }}>
          {anyRunning ? <><Loader size={11} style={{ animation: 'spin 1s linear infinite' }} /> Running all…</> : <><Zap size={11} /> Run All Modules</>}
        </button>
      </div>

      {!rawProfile && <div style={{ textAlign: 'center', padding: '20px', fontSize: 12, color: 'var(--text-3)' }}>Loading profile data…</div>}

      {/* Core group */}
      {rawProfile && (
        <>
          <div style={{ fontSize: 9, fontWeight: 800, color: 'var(--text-3)', letterSpacing: '0.1em', textTransform: 'uppercase', paddingLeft: 2 }}>Core</div>
          {AI_MODULES.filter(m => m.group === 'core').map(renderModule)}

          <div style={{ fontSize: 9, fontWeight: 800, color: 'var(--text-3)', letterSpacing: '0.1em', textTransform: 'uppercase', paddingLeft: 2, marginTop: 4 }}>Intelligence</div>
          {AI_MODULES.filter(m => m.group === 'intel').map(renderModule)}

          <div style={{ fontSize: 9, fontWeight: 800, color: 'var(--text-3)', letterSpacing: '0.1em', textTransform: 'uppercase', paddingLeft: 2, marginTop: 4 }}>Output</div>
          {AI_MODULES.filter(m => m.group === 'output').map(renderModule)}
        </>
      )}
    </div>
  )
}
