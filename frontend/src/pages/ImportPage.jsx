// ImportPage.jsx — Single-file wizard + Folder batch import + History
import { useState, useRef, useCallback, useEffect } from 'react'
import toast from 'react-hot-toast'
import { saveJob, updateJob, removeJob, getActiveJobs } from '../utils/importStore'

const API_BASE = `${import.meta.env.VITE_BACKEND_URL || 'https://leadenrichment-production-5b78.up.railway.app'}/api`
const MAX_FILE_BYTES = 500 * 1024 * 1024  // 500 MB
const IMPORT_STORE_EVENT = 'wb-import-store-updated'

const IMPORTABLE_FIELDS = [
  { value: '', label: '— skip —' },
  { value: 'linkedin_url', label: 'LinkedIn URL' },
  { value: 'name', label: 'Full Name' },
  { value: 'first_name', label: 'First Name' },
  { value: 'last_name', label: 'Last Name' },
  { value: 'work_email', label: 'Work Email' },
  { value: 'personal_email', label: 'Personal Email' },
  { value: 'direct_phone', label: 'Phone' },
  { value: 'twitter', label: 'Twitter' },
  { value: 'title', label: 'Job Title' },
  { value: 'seniority_level', label: 'Seniority Level' },
  { value: 'department', label: 'Department' },
  { value: 'city', label: 'City' },
  { value: 'country', label: 'Country' },
  { value: 'timezone', label: 'Timezone' },
  { value: 'company', label: 'Company' },
  { value: 'industry', label: 'Industry' },
  { value: 'company_website', label: 'Company Website' },
  { value: 'employee_count', label: 'Employee Count' },
  { value: 'hq_location', label: 'HQ Location' },
  { value: 'founded_year', label: 'Founded Year' },
  { value: 'funding_stage', label: 'Funding Stage' },
  { value: 'total_funding', label: 'Total Funding' },
  { value: 'annual_revenue', label: 'Annual Revenue' },
  { value: 'tags', label: 'Tags' },
  { value: 'about', label: 'About / Bio' },
  { value: 'email_source', label: 'Email Source' },
  { value: 'email_confidence', label: 'Email Confidence' },
  { value: 'connections', label: 'LinkedIn Connections' },
  { value: 'followers', label: 'Followers' },
]

const ACCEPTED_EXTS = ['csv', 'tsv', 'xls', 'xlsx']

function getOrgId() {
  try {
    const token = localStorage.getItem('wb_ai_token')
    if (!token) return 'default'
    const payload = JSON.parse(atob(token.split('.')[1]))
    return payload.org_id || payload.organization_id || 'default'
  } catch { return 'default' }
}

function fmtSize(bytes) {
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}

function fmtDate(iso) {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  } catch { return iso }
}

function isAccepted(filename) {
  const ext = filename.split('.').pop().toLowerCase()
  return ACCEPTED_EXTS.includes(ext)
}

// ── Styles ─────────────────────────────────────────────────────────────────

const card = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border-1)',
  borderRadius: 14,
  padding: '28px 32px',
}

const btn = (v = 'primary', disabled = false) => ({
  padding: '9px 20px', borderRadius: 9, fontSize: 13, fontWeight: 600,
  cursor: disabled ? 'not-allowed' : 'pointer',
  border: v === 'primary' ? 'none' : '1px solid var(--border-1)',
  background: v === 'primary' ? (disabled ? '#4b4e8a' : '#6366f1') : 'transparent',
  color: v === 'primary' ? '#fff' : 'var(--text-2)',
  opacity: disabled ? 0.7 : 1,
  transition: 'all 0.15s',
})

// ── Resume Panel ────────────────────────────────────────────────────────────
// Shown at top of page when user navigates back and there are active jobs.
// Lets them see live progress without losing their place in the wizard.

function ResumePanel() {
  const [jobs, setJobs] = useState(() => getActiveJobs())
  const [dismissed, setDismissed] = useState(false)

  useEffect(() => {
    const refresh = () => setJobs(getActiveJobs())
    window.addEventListener(IMPORT_STORE_EVENT, refresh)
    return () => window.removeEventListener(IMPORT_STORE_EVENT, refresh)
  }, [])

  // Also poll directly while on this page (2s — faster than the global 8s)
  useEffect(() => {
    if (!jobs.length) return
    const iv = setInterval(async () => {
      const active = getActiveJobs()
      for (const job of active) {
        try {
          const res = await fetch(`${API_BASE}/import/status/${job.jobId}`)
          if (!res.ok) continue
          const d = await res.json()
          updateJob(job.jobId, { status: d.status, processed: d.processed, pct: d.pct, total: d.total, new_count: d.new_count, updated_count: d.updated_count, skipped: d.skipped })
          if (d.status === 'completed' || d.status === 'failed') {
            setTimeout(() => removeJob(job.jobId), 3000)
          }
        } catch {}
      }
      setJobs(getActiveJobs())
    }, 2000)
    return () => clearInterval(iv)
  }, [jobs.length])

  if (!jobs.length || dismissed) return null

  return (
    <div style={{
      background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.25)',
      borderRadius: 12, padding: '16px 20px', marginBottom: 24,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#f59e0b', display: 'inline-block', animation: 'pulse 1.5s ease-in-out infinite' }} />
          <span style={{ fontSize: 13, fontWeight: 700, color: '#f59e0b' }}>
            {jobs.length} import{jobs.length > 1 ? 's' : ''} running in background
          </span>
        </div>
        <button
          onClick={() => setDismissed(true)}
          style={{ background: 'none', border: 'none', color: 'var(--text-3)', cursor: 'pointer', fontSize: 16, padding: '0 4px' }}
          title="Dismiss (job keeps running)"
        >×</button>
      </div>
      {jobs.map(j => {
        const pct = j.pct || 0
        const statusColor = { running: '#f59e0b', completed: '#10b981', failed: '#ef4444', pending: '#6b7280' }
        return (
          <div key={j.jobId} style={{ marginBottom: 10 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
              <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }}>
                {j.filename}
              </span>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
                {j.processed > 0 && (
                  <span style={{ fontSize: 11, color: 'var(--text-3)' }}>
                    {(j.processed || 0).toLocaleString()} / {(j.total || 0).toLocaleString()}
                  </span>
                )}
                <span style={{ fontSize: 11, fontWeight: 700, color: statusColor[j.status] || '#6b7280', textTransform: 'capitalize' }}>
                  {j.status} {pct > 0 && j.status === 'running' ? `· ${pct}%` : ''}
                </span>
              </div>
            </div>
            <div style={{ height: 5, background: 'rgba(245,158,11,0.15)', borderRadius: 3, overflow: 'hidden' }}>
              <div style={{
                height: '100%', borderRadius: 3, transition: 'width 0.5s',
                background: j.status === 'failed' ? '#ef4444' : j.status === 'completed' ? '#10b981' : '#f59e0b',
                width: `${j.status === 'completed' ? 100 : pct}%`,
              }} />
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Tab bar ────────────────────────────────────────────────────────────────

function TabBar({ active, onChange }) {
  const tabs = [
    { id: 'single', label: 'Single File', icon: '📄' },
    { id: 'folder', label: 'Folder Import', icon: '📁' },
    { id: 'history', label: 'History', icon: '🕓' },
  ]
  return (
    <div style={{ display: 'flex', gap: 4, marginBottom: 28, background: 'var(--bg-sidebar)', borderRadius: 10, padding: 4 }}>
      {tabs.map(t => (
        <button
          key={t.id}
          onClick={() => onChange(t.id)}
          style={{
            flex: 1, padding: '8px 16px', borderRadius: 8, border: 'none',
            fontSize: 13, fontWeight: 600, cursor: 'pointer',
            background: active === t.id ? 'var(--bg-card)' : 'transparent',
            color: active === t.id ? 'var(--text-1)' : 'var(--text-3)',
            boxShadow: active === t.id ? '0 1px 4px rgba(0,0,0,0.2)' : 'none',
            transition: 'all 0.15s',
          }}
        >
          {t.icon} {t.label}
        </button>
      ))}
    </div>
  )
}

// ── Step bar (single-file flow) ────────────────────────────────────────────

function StepBar({ step }) {
  const steps = ['Upload', 'Map Columns', 'Import']
  return (
    <div style={{ display: 'flex', alignItems: 'center', marginBottom: 28 }}>
      {steps.map((label, i) => {
        const num = i + 1; const active = num === step; const done = num < step
        return (
          <div key={label} style={{ display: 'flex', alignItems: 'center', flex: i < steps.length - 1 ? 1 : undefined }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
              <div style={{
                width: 26, height: 26, borderRadius: '50%',
                background: done ? '#6366f1' : active ? 'rgba(99,102,241,0.15)' : 'var(--bg-card)',
                border: done ? 'none' : active ? '2px solid #6366f1' : '1px solid var(--border-1)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 11, fontWeight: 700,
                color: done ? '#fff' : active ? '#a5b4fc' : 'var(--text-3)',
              }}>{done ? '✓' : num}</div>
              <span style={{ fontSize: 12, fontWeight: 600, color: active ? 'var(--text-1)' : done ? '#a5b4fc' : 'var(--text-3)' }}>{label}</span>
            </div>
            {i < steps.length - 1 && <div style={{ flex: 1, height: 1, background: done ? '#6366f1' : 'var(--border-1)', margin: '0 12px' }} />}
          </div>
        )
      })}
    </div>
  )
}

// ── Drop zone (shared) ──────────────────────────────────────────────────────

function DropZone({ label, hint, multiple, folder, onFiles, loading }) {
  const [dragging, setDragging] = useState(false)
  const inputRef = useRef(null)

  const handleDrop = (e) => {
    e.preventDefault(); setDragging(false)
    if (!loading) onFiles(Array.from(e.dataTransfer.files))
  }

  return (
    <div
      onClick={() => !loading && inputRef.current?.click()}
      onDragOver={e => { e.preventDefault(); setDragging(true) }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      style={{
        border: `2px dashed ${dragging ? '#6366f1' : 'var(--border-1)'}`,
        borderRadius: 14, padding: '56px 40px', textAlign: 'center',
        cursor: loading ? 'default' : 'pointer',
        background: dragging ? 'rgba(99,102,241,0.06)' : 'transparent',
        transition: 'all 0.15s',
      }}
    >
      <div style={{ fontSize: 38, marginBottom: 12 }}>{loading ? '⏳' : '📂'}</div>
      <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)', marginBottom: 6 }}>
        {loading ? 'Reading…' : label}
      </div>
      <div style={{ fontSize: 12, color: 'var(--text-3)' }}>{loading ? '' : hint}</div>
      <input
        ref={inputRef} type="file"
        accept=".csv,.tsv,.xls,.xlsx"
        multiple={multiple || folder}
        {...(folder ? { webkitdirectory: '', directory: '' } : {})}
        style={{ display: 'none' }}
        onChange={e => { if (!loading) onFiles(Array.from(e.target.files)) }}
      />
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════
// SINGLE / MULTI FILE FLOW
// ═══════════════════════════════════════════════════════════════════════════

// Step 1 — pick files (one or many)
function FilesStep({ onPreview }) {
  const [loading, setLoading] = useState(false)
  const [pending, setPending] = useState([])  // files chosen but not yet previewed

  const handleFiles = useCallback(async (chosen) => {
    const valid = chosen.filter(f => isAccepted(f.name))
    const oversized = chosen.filter(f => f.size > MAX_FILE_BYTES)
    if (!valid.length) { toast.error('No supported files. Use CSV, TSV, XLS, or XLSX.'); return }
    if (oversized.length) toast.error(`${oversized.length} file(s) exceed 500 MB and were skipped.`)
    const accepted = valid.filter(f => f.size <= MAX_FILE_BYTES)
    if (!accepted.length) return
    setPending(accepted)
    setLoading(true)
    try {
      // Preview the first file for column mapping
      const form = new FormData(); form.append('file', accepted[0])
      const res = await fetch(`${API_BASE}/import/preview`, { method: 'POST', body: form })
      if (!res.ok) throw new Error((await res.json()).detail || res.statusText)
      onPreview(accepted, await res.json())
    } catch (err) { toast.error(`Preview failed: ${err.message}`); setPending([]) }
    finally { setLoading(false) }
  }, [onPreview])

  return (
    <>
      <DropZone
        label="Drop files here"
        hint="or click to browse · select one or multiple files · CSV / TSV / XLS / XLSX · max 500 MB each"
        multiple={true}
        onFiles={handleFiles}
        loading={loading}
      />
      {pending.length > 1 && !loading && (
        <div style={{ marginTop: 10, fontSize: 12, color: '#a5b4fc', fontWeight: 600 }}>
          {pending.length} files selected
        </div>
      )}
      <div style={{ marginTop: 16, display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        <span style={{ fontSize: 11, color: 'var(--text-3)' }}>Auto-detects:</span>
        {['Apollo', 'HubSpot', 'LinkedIn', 'Salesforce'].map(s => (
          <span key={s} style={{ fontSize: 11, padding: '2px 8px', borderRadius: 5, background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', fontWeight: 600 }}>{s}</span>
        ))}
      </div>
    </>
  )
}

// Step 2 — map columns (based on first file, applied to all)
function MappingStep({ files, preview, onStart, onBack }) {
  const [mapping, setMapping] = useState(() => {
    const suggested = preview.suggested_mapping || {}
    return Object.fromEntries(preview.columns.map(c => [c, suggested[c] || '']))
  })
  const [loading, setLoading] = useState(false)

  const usedFields = Object.values(mapping).filter(Boolean)
  const autoMapped = preview.columns.filter(c => (preview.suggested_mapping || {})[c]).length
  const firstFile = files[0]

  const handleStart = async () => {
    const hasKey = Object.values(mapping).some(v => ['linkedin_url','work_email','personal_email'].includes(v))
    if (!hasKey) { toast.error('Map at least one of: LinkedIn URL, Work Email, or Personal Email'); return }
    setLoading(true)
    const org_id = getOrgId()
    const jobs = []
    try {
      // Start all files in parallel with the same mapping
      await Promise.all(files.map(async (file) => {
        const form = new FormData()
        form.append('file', file)
        form.append('mapping', JSON.stringify(mapping))
        form.append('org_id', org_id)
        const res = await fetch(`${API_BASE}/import/start`, { method: 'POST', body: form })
        if (!res.ok) throw new Error((await res.json()).detail || res.statusText)
        const { job_id } = await res.json()
        saveJob(job_id, file.name, preview.total_rows)
        jobs.push({ jobId: job_id, filename: file.name, totalRows: preview.total_rows })
      }))
      onStart(jobs)
    } catch (err) { toast.error(`Failed to start: ${err.message}`); setLoading(false) }
  }

  return (
    <div>
      <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>
          {files.length > 1 ? `${files.length} files selected` : firstFile.name}
        </span>
        <span style={{ fontSize: 12, color: 'var(--text-3)' }}>
          {preview.total_rows.toLocaleString()} rows · {preview.file_type.toUpperCase()}
          {files.length > 1 && ' (preview from first file)'}
        </span>
        {autoMapped > 0 && <span style={{ fontSize: 12, color: '#10b981', fontWeight: 600 }}>✓ {autoMapped} auto-mapped</span>}
        {files.length > 1 && (
          <span style={{ fontSize: 11, background: 'rgba(99,102,241,0.1)', color: '#a5b4fc', borderRadius: 5, padding: '2px 8px', fontWeight: 600 }}>
            mapping applies to all {files.length} files
          </span>
        )}
      </div>

      {/* File list when multi-file */}
      {files.length > 1 && (
        <div style={{ border: '1px solid var(--border-1)', borderRadius: 10, marginBottom: 16, maxHeight: 130, overflowY: 'auto' }}>
          {files.map((f, i) => (
            <div key={f.name} style={{
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              padding: '7px 14px', borderTop: i === 0 ? 'none' : '1px solid var(--border-1)',
              fontSize: 12,
            }}>
              <span style={{ color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
                {i === 0 ? '📄 ' : '📄 '}{f.name}
                {i === 0 && <span style={{ fontSize: 10, color: '#a5b4fc', marginLeft: 6 }}>(preview)</span>}
              </span>
              <span style={{ color: 'var(--text-3)', flexShrink: 0, marginLeft: 12 }}>{fmtSize(f.size)}</span>
            </div>
          ))}
        </div>
      )}

      <div style={{ border: '1px solid var(--border-1)', borderRadius: 10, overflow: 'hidden', maxHeight: 340, overflowY: 'auto', marginBottom: 20 }}>
        <div style={{
          display: 'grid', gridTemplateColumns: '1fr 1fr 1.5fr',
          background: 'var(--bg-sidebar)', padding: '8px 16px',
          fontSize: 11, fontWeight: 700, color: 'var(--text-3)',
          letterSpacing: '0.06em', textTransform: 'uppercase',
          position: 'sticky', top: 0, borderBottom: '1px solid var(--border-1)',
        }}>
          <span>Source Column</span><span>DB Field</span><span>Sample</span>
        </div>
        {preview.columns.map(col => {
          const val = mapping[col] || ''
          const isDup = val && usedFields.filter(v => v === val).length > 1
          const isAuto = !!(preview.suggested_mapping || {})[col]
          const samples = preview.rows.map(r => r[col]).filter(Boolean).slice(0, 3)
          return (
            <div key={col} style={{
              display: 'grid', gridTemplateColumns: '1fr 1fr 1.5fr',
              padding: '8px 16px', borderTop: '1px solid var(--border-1)', alignItems: 'center',
              background: isDup ? 'rgba(239,68,68,0.04)' : undefined,
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{col}</span>
                {isAuto && val && <span style={{ fontSize: 9, background: 'rgba(16,185,129,0.12)', color: '#10b981', borderRadius: 4, padding: '1px 5px', fontWeight: 700, flexShrink: 0 }}>AUTO</span>}
                {!val && <span style={{ fontSize: 9, background: 'rgba(245,158,11,0.12)', color: '#f59e0b', borderRadius: 4, padding: '1px 5px', fontWeight: 700, flexShrink: 0 }}>UNMAPPED→SAVED</span>}
              </div>
              <select
                value={val}
                onChange={e => setMapping(m => ({ ...m, [col]: e.target.value }))}
                style={{
                  background: 'var(--bg-base)', borderRadius: 7, fontSize: 12, padding: '5px 8px',
                  border: `1px solid ${isDup ? '#ef4444' : val ? 'rgba(99,102,241,0.4)' : 'var(--border-1)'}`,
                  color: val ? 'var(--text-1)' : 'var(--text-3)', width: '92%', cursor: 'pointer',
                }}
              >
                {IMPORTABLE_FIELDS.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
              </select>
              <span style={{ fontSize: 11, color: 'var(--text-3)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {samples.join(' · ') || '—'}
              </span>
            </div>
          )
        })}
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: 12, color: 'var(--text-3)' }}>
          Unmapped columns are saved to Storage → Unmapped Fields
        </span>
        <div style={{ display: 'flex', gap: 10 }}>
          <button onClick={onBack} style={btn('ghost')}>Back</button>
          <button onClick={handleStart} disabled={loading} style={btn('primary', loading)}>
            {loading ? 'Starting…' : files.length > 1
              ? `Import ${files.length} files →`
              : `Import ${preview.total_rows.toLocaleString()} rows →`}
          </button>
        </div>
      </div>
    </div>
  )
}

// Step 3 — progress (one or multiple jobs)
function ProgressStep({ jobs, onReset }) {
  // jobs: [{jobId, filename, totalRows}]
  const [jobStates, setJobStates] = useState(() =>
    Object.fromEntries(jobs.map(j => [j.jobId, {
      status: 'pending', processed: 0, total: j.totalRows, pct: 0,
      new_count: 0, updated_count: 0, skipped: 0, error: null,
    }]))
  )
  const intervalRef = useRef(null)

  useEffect(() => {
    const poll = async () => {
      await Promise.all(jobs.map(async ({ jobId }) => {
        try {
          const res = await fetch(`${API_BASE}/import/status/${jobId}`)
          if (!res.ok) return
          const d = await res.json()
          setJobStates(prev => ({ ...prev, [jobId]: d }))
          updateJob(jobId, { status: d.status, processed: d.processed, pct: d.pct, total: d.total })
        } catch {}
      }))
    }
    poll(); intervalRef.current = setInterval(poll, 2000)
    return () => clearInterval(intervalRef.current)
  }, [])

  useEffect(() => {
    const allDone = Object.values(jobStates).every(s => s.status === 'completed' || s.status === 'failed')
    if (allDone && intervalRef.current) clearInterval(intervalRef.current)
  }, [jobStates])

  const states = Object.values(jobStates)
  const allDone = states.every(s => s.status === 'completed' || s.status === 'failed')
  const totalNew     = states.reduce((s, j) => s + (j.new_count     || 0), 0)
  const totalUpdated = states.reduce((s, j) => s + (j.updated_count || 0), 0)
  const totalSkipped = states.reduce((s, j) => s + (j.skipped       || 0), 0)
  const statusColor  = { pending: '#6b7280', running: '#f59e0b', completed: '#10b981', failed: '#ef4444' }

  return (
    <div>
      {/* Per-file progress rows */}
      <div style={{ border: '1px solid var(--border-1)', borderRadius: 10, overflow: 'hidden', marginBottom: 20 }}>
        {jobs.map(({ jobId, filename }) => {
          const s = jobStates[jobId] || {}
          const pct = s.pct || 0
          const status = s.status || 'pending'
          const done = status === 'completed' || status === 'failed'
          return (
            <div key={jobId} style={{ padding: '12px 16px', borderTop: jobs[0].jobId === jobId ? 'none' : '1px solid var(--border-1)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0, flex: 1 }}>
                  <span style={{ fontSize: 10, width: 8, height: 8, borderRadius: '50%', background: statusColor[status], display: 'inline-block', flexShrink: 0 }} />
                  <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{filename}</span>
                </div>
                <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexShrink: 0, marginLeft: 12 }}>
                  {!done && s.processed > 0 && (
                    <span style={{ fontSize: 11, color: 'var(--text-3)' }}>
                      {(s.processed || 0).toLocaleString()} / {(s.total || 0).toLocaleString()}
                    </span>
                  )}
                  <span style={{ fontSize: 11, fontWeight: 700, color: statusColor[status], textTransform: 'capitalize', minWidth: 60, textAlign: 'right' }}>
                    {status === 'completed' ? '✓ Done' : status === 'failed' ? '✗ Failed' : status === 'running' ? `${pct}%` : 'Pending'}
                  </span>
                </div>
              </div>
              <div style={{ height: 5, background: 'rgba(99,102,241,0.1)', borderRadius: 3, overflow: 'hidden' }}>
                <div style={{
                  height: '100%', borderRadius: 3, transition: 'width 0.4s',
                  background: status === 'failed' ? '#ef4444' : status === 'completed' ? '#10b981' : 'linear-gradient(90deg,#6366f1,#8b5cf6)',
                  width: `${status === 'completed' ? 100 : pct}%`,
                }} />
              </div>
              {s.error && <div style={{ fontSize: 11, color: '#ef4444', marginTop: 4, fontFamily: 'monospace' }}>{s.error}</div>}
            </div>
          )
        })}
      </div>

      {/* Summary counts */}
      {(totalNew > 0 || totalUpdated > 0 || totalSkipped > 0) && (
        <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
          {[
            { label: 'New leads', value: totalNew, color: '#10b981' },
            { label: 'Updated (duplicates)', value: totalUpdated, color: '#6366f1' },
            { label: 'Skipped (no key)', value: totalSkipped, color: '#f59e0b' },
          ].map(s => (
            <div key={s.label} style={{
              flex: 1, minWidth: 100, background: 'var(--bg-sidebar)', borderRadius: 9,
              padding: '10px 14px', border: '1px solid var(--border-1)', textAlign: 'center',
            }}>
              <div style={{ fontSize: 18, fontWeight: 800, color: s.color }}>{s.value.toLocaleString()}</div>
              <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2 }}>{s.label}</div>
            </div>
          ))}
        </div>
      )}

      {!allDone && (
        <div style={{ background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.15)', borderRadius: 10, padding: '12px 16px', marginBottom: 20 }}>
          <div style={{ fontSize: 12, color: '#a5b4fc', fontWeight: 600, marginBottom: 3 }}>Processing in background</div>
          <div style={{ fontSize: 11, color: 'var(--text-3)' }}>You can navigate away — imports keep running. Progress is saved and restored when you return.</div>
        </div>
      )}

      {allDone && (
        <div style={{ display: 'flex', gap: 10 }}>
          <button onClick={onReset} style={btn('primary')}>Import more files</button>
          <a href="/storage" style={{ ...btn('ghost'), textDecoration: 'none', display: 'inline-flex', alignItems: 'center' }}>View in Storage →</a>
        </div>
      )}
    </div>
  )
}

function SingleFileFlow() {
  const [step, setStep] = useState(1)
  const [files, setFiles] = useState([])
  const [preview, setPreview] = useState(null)
  const [jobs, setJobs] = useState([])

  const reset = () => { setStep(1); setFiles([]); setPreview(null); setJobs([]) }

  return (
    <>
      <StepBar step={step} />
      {step === 1 && (
        <FilesStep onPreview={(fs, p) => { setFiles(fs); setPreview(p); setStep(2) }} />
      )}
      {step === 2 && preview && (
        <MappingStep files={files} preview={preview}
          onStart={(js) => { setJobs(js); setStep(3) }}
          onBack={reset}
        />
      )}
      {step === 3 && jobs.length > 0 && <ProgressStep jobs={jobs} onReset={reset} />}
    </>
  )
}

// ═══════════════════════════════════════════════════════════════════════════
// FOLDER IMPORT FLOW
// ═══════════════════════════════════════════════════════════════════════════

function FileStatusIcon({ status }) {
  if (status === 'pending') return <span style={{ fontSize: 14, color: 'var(--text-3)' }}>⏳</span>
  if (status === 'running') return <span style={{ fontSize: 14, color: '#f59e0b' }}>🔄</span>
  if (status === 'completed') return <span style={{ fontSize: 14, color: '#10b981' }}>✅</span>
  return <span style={{ fontSize: 14, color: '#ef4444' }}>❌</span>
}

// Generate a UUID v4 without external lib
function genUUID() {
  return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>
    (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16))
}

const FOLDER_BATCH = 4  // max files started concurrently

function FolderImportFlow() {
  // files: [{name, size, file, status, jobId, result, error, pct, alreadyImported}]
  const [files, setFiles]               = useState([])
  const [running, setRunning]           = useState(false)
  const [folderId, setFolderId]         = useState(null)
  const [importedNames, setImportedNames] = useState(new Set())
  const [skipDuplicates, setSkipDuplicates] = useState(true)
  const pollRef = useRef(null)

  // Load already-imported filenames once on mount
  useEffect(() => {
    const org_id = getOrgId()
    fetch(`${API_BASE}/import/imported-files?org_id=${encodeURIComponent(org_id)}`)
      .then(r => r.ok ? r.json() : { filenames: [] })
      .then(d => setImportedNames(new Set(d.filenames || [])))
      .catch(() => {})
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [])

  const handleFolderSelect = useCallback((selectedFiles) => {
    const accepted = selectedFiles
      .filter(f => isAccepted(f.name))
      .map(f => ({
        name: f.name, size: f.size, file: f,
        status: 'pending', jobId: null, result: null, error: null, pct: 0,
        alreadyImported: importedNames.has(f.name),
      }))
    if (!accepted.length) { toast.error('No supported files found in the selected folder.'); return }
    setFiles(accepted)
    setRunning(false)
    setFolderId(null)
    if (pollRef.current) clearInterval(pollRef.current)
  }, [importedNames])

  const setFileByName = (name, patch) =>
    setFiles(prev => prev.map(f => f.name === name ? { ...f, ...patch } : f))

  // ── Poll folder status (single request covers all files) ─────────────────
  const startFolderPoll = useCallback((fid, org_id) => {
    if (pollRef.current) clearInterval(pollRef.current)
    pollRef.current = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE}/import/folder-status/${fid}?org_id=${encodeURIComponent(org_id)}`)
        if (!res.ok) return
        const data = await res.json()

        // Update each file row from the folder status response
        setFiles(prev => prev.map(f => {
          const job = data.jobs.find(j => j.filename === f.name)
          if (!job) return f
          const status = job.status === 'completed' ? 'completed'
                       : job.status === 'failed'    ? 'error'
                       : 'running'
          return {
            ...f,
            status,
            pct:    job.pct    || 0,
            jobId:  job.id,
            result: job.status === 'completed' ? job : f.result,
            error:  job.status === 'failed'    ? (job.error || 'Import failed') : f.error,
          }
        }))

        // Update import store for sidebar badges
        data.jobs.forEach(job => {
          if (job.id) updateJob(job.id, { status: job.status, processed: job.processed, pct: job.pct, total: job.total })
        })

        if (data.done) {
          clearInterval(pollRef.current)
          setRunning(false)
          const newCount     = data.total_new     || 0
          const updatedCount = data.total_updated || 0
          const skipped      = data.total_skipped || 0
          toast.success(
            `Folder import complete — ${newCount.toLocaleString()} new, ` +
            `${updatedCount.toLocaleString()} updated, ${skipped.toLocaleString()} skipped`
          )
          // Mark all completed files as imported
          setImportedNames(prev => {
            const next = new Set(prev)
            data.jobs.filter(j => j.status === 'completed').forEach(j => next.add(j.filename))
            return next
          })
        }
      } catch {}
    }, 2000)
  }, [])

  // ── Start import: concurrent batches of FOLDER_BATCH files ───────────────
  const startImport = async () => {
    setRunning(true)
    const org_id = getOrgId()
    const newFolderId = genUUID()
    setFolderId(newFolderId)

    const toProcess = files.filter(f => {
      if (f.status === 'completed') return false
      if (skipDuplicates && f.alreadyImported) return false
      return true
    })

    // Mark all as 'running' immediately so the UI responds
    setFiles(prev => prev.map(f => {
      const willRun = toProcess.some(t => t.name === f.name)
      return willRun ? { ...f, status: 'running', pct: 0 } : f
    }))

    // Fire files in batches of FOLDER_BATCH (prevents overwhelming the server)
    const startFile = async (fileEntry) => {
      try {
        // 1. Preview → get auto-mapping
        const form1 = new FormData()
        form1.append('file', fileEntry.file)
        const r1 = await fetch(`${API_BASE}/import/preview`, { method: 'POST', body: form1 })
        if (!r1.ok) throw new Error((await r1.json()).detail || r1.statusText)
        const preview = await r1.json()

        // 2. Start job (attach folder_id so all files are grouped)
        const form2 = new FormData()
        form2.append('file', fileEntry.file)
        form2.append('mapping', JSON.stringify(preview.suggested_mapping || {}))
        form2.append('org_id', org_id)
        form2.append('folder_id', newFolderId)
        const r2 = await fetch(`${API_BASE}/import/start`, { method: 'POST', body: form2 })
        if (!r2.ok) throw new Error((await r2.json()).detail || r2.statusText)
        const { job_id } = await r2.json()

        saveJob(job_id, fileEntry.name, preview.total_rows)
        setFileByName(fileEntry.name, { jobId: job_id })
      } catch (err) {
        setFileByName(fileEntry.name, { status: 'error', error: err.message })
      }
    }

    for (let i = 0; i < toProcess.length; i += FOLDER_BATCH) {
      await Promise.all(toProcess.slice(i, i + FOLDER_BATCH).map(startFile))
    }

    // All jobs fired — start single folder-level poll
    startFolderPoll(newFolderId, org_id)
  }

  const completedFiles = files.filter(f => f.status === 'completed')
  const errorFiles     = files.filter(f => f.status === 'error')
  const pendingFiles   = files.filter(f => f.status === 'pending')
  const duplicateFiles = files.filter(f => f.alreadyImported)
  const totalImported  = completedFiles.reduce((s, f) => s + (f.result?.processed || 0), 0)

  const filesToRun = files.filter(f => {
    if (f.status === 'completed') return false
    if (skipDuplicates && f.alreadyImported) return false
    return true
  })

  return (
    <div>
      {files.length === 0 ? (
        <>
          <DropZone
            label="Select a folder"
            hint="All CSV / TSV / XLS / XLSX files inside will be listed"
            folder={true}
            onFiles={handleFolderSelect}
            loading={false}
          />
          <div style={{ marginTop: 14, fontSize: 12, color: 'var(--text-3)' }}>
            Already-imported files are detected automatically and can be skipped.
          </div>
        </>
      ) : (
        <>
          {/* Duplicate warning */}
          {duplicateFiles.length > 0 && (
            <div style={{
              background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.2)',
              borderRadius: 10, padding: '12px 16px', marginBottom: 16,
              display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 10,
            }}>
              <span style={{ fontSize: 12, color: '#f59e0b', fontWeight: 600 }}>
                ⚠️ {duplicateFiles.length} file{duplicateFiles.length > 1 ? 's' : ''} already imported previously
              </span>
              <label style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer', fontSize: 12, color: 'var(--text-2)' }}>
                <input
                  type="checkbox"
                  checked={skipDuplicates}
                  onChange={e => setSkipDuplicates(e.target.checked)}
                  style={{ cursor: 'pointer' }}
                />
                Skip already-imported files
              </label>
            </div>
          )}

          {/* Summary bar */}
          <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
            {[
              { label: 'Total files', value: files.length, color: 'var(--text-1)' },
              { label: 'Completed', value: completedFiles.length, color: '#10b981' },
              { label: 'Errors', value: errorFiles.length, color: errorFiles.length ? '#ef4444' : 'var(--text-3)' },
              { label: 'Pending', value: pendingFiles.length, color: '#f59e0b' },
              { label: 'Duplicates', value: duplicateFiles.length, color: duplicateFiles.length ? '#f59e0b' : 'var(--text-3)' },
              { label: 'Rows imported', value: totalImported.toLocaleString(), color: '#a5b4fc' },
            ].map(s => (
              <div key={s.label} style={{
                background: 'var(--bg-sidebar)', borderRadius: 10, padding: '10px 16px',
                border: '1px solid var(--border-1)', textAlign: 'center', flex: '1 1 80px',
              }}>
                <div style={{ fontSize: 18, fontWeight: 800, color: s.color }}>{s.value}</div>
                <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2 }}>{s.label}</div>
              </div>
            ))}
          </div>

          {/* File list */}
          <div style={{ border: '1px solid var(--border-1)', borderRadius: 10, overflow: 'hidden', maxHeight: 380, overflowY: 'auto', marginBottom: 20 }}>
            {completedFiles.length > 0 && (
              <div style={{ background: 'rgba(16,185,129,0.05)', borderBottom: '1px solid var(--border-1)', padding: '6px 16px' }}>
                <span style={{ fontSize: 11, fontWeight: 700, color: '#10b981', textTransform: 'uppercase', letterSpacing: '0.06em' }}>✅ Completed</span>
              </div>
            )}
            {completedFiles.map(f => <FileRow key={f.name} f={f} />)}

            {[...files.filter(f => f.status === 'running'), ...pendingFiles].map(f => <FileRow key={f.name} f={f} skipDuplicate={skipDuplicates && f.alreadyImported} />)}

            {errorFiles.length > 0 && (
              <div style={{ background: 'rgba(239,68,68,0.05)', borderTop: '1px solid var(--border-1)', padding: '6px 16px' }}>
                <span style={{ fontSize: 11, fontWeight: 700, color: '#ef4444', textTransform: 'uppercase', letterSpacing: '0.06em' }}>❌ Failed Files</span>
              </div>
            )}
            {errorFiles.map(f => <FileRow key={f.name} f={f} />)}
          </div>

          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
            <button
              onClick={startImport}
              disabled={running || filesToRun.length === 0}
              style={btn('primary', running || filesToRun.length === 0)}
            >
              {running
                ? 'Importing…'
                : filesToRun.length === 0
                  ? 'Nothing to import'
                  : `Import ${filesToRun.length} file${filesToRun.length > 1 ? 's' : ''}`}
            </button>
            <button onClick={() => { setFiles([]); setRunning(false) }} style={btn('ghost')}>Clear</button>
            <a href="/storage" style={{ ...btn('ghost'), textDecoration: 'none', display: 'inline-flex', alignItems: 'center', marginLeft: 'auto' }}>
              View in Storage →
            </a>
          </div>
        </>
      )}
    </div>
  )
}

function FileRow({ f, skipDuplicate }) {
  return (
    <div style={{
      borderTop: '1px solid var(--border-1)', padding: '10px 16px',
      opacity: skipDuplicate ? 0.45 : 1,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <FileStatusIcon status={f.status} />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {f.name}
            </span>
            {f.alreadyImported && (
              <span style={{ fontSize: 9, background: 'rgba(245,158,11,0.12)', color: '#f59e0b', borderRadius: 4, padding: '1px 5px', fontWeight: 700, flexShrink: 0 }}>
                ALREADY IMPORTED
              </span>
            )}
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
            {fmtSize(f.size)}
            {f.status === 'completed' && f.result && (
              <>
                {` · ${f.result.processed?.toLocaleString() || 0} rows`}
                {f.result.new_count > 0 && <span style={{ color: '#10b981' }}> · {f.result.new_count.toLocaleString()} new</span>}
                {f.result.updated_count > 0 && <span style={{ color: '#6366f1' }}> · {f.result.updated_count.toLocaleString()} updated</span>}
                {f.result.skipped > 0 && <span style={{ color: '#f59e0b' }}> · {f.result.skipped.toLocaleString()} skipped</span>}
              </>
            )}
            {f.status === 'error' && <span style={{ color: '#ef4444' }}> · {f.error}</span>}
            {skipDuplicate && <span style={{ color: 'var(--text-3)' }}> · will be skipped</span>}
          </div>
        </div>
        {f.status === 'running' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
            <div style={{ width: 100, height: 6, background: 'var(--bg-sidebar)', borderRadius: 4, overflow: 'hidden' }}>
              <div style={{ height: '100%', width: `${f.pct}%`, background: 'linear-gradient(90deg,#6366f1,#8b5cf6)', borderRadius: 4, transition: 'width 0.4s' }} />
            </div>
            <span style={{ fontSize: 11, color: '#a5b4fc', fontWeight: 700, minWidth: 30 }}>{f.pct}%</span>
          </div>
        )}
        {f.status === 'completed' && (
          <span style={{ fontSize: 11, color: '#10b981', fontWeight: 700, flexShrink: 0 }}>Done</span>
        )}
      </div>
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════
// HISTORY TAB
// ═══════════════════════════════════════════════════════════════════════════

const STATUS_COLOR = { completed: '#10b981', failed: '#ef4444', running: '#f59e0b', pending: '#6b7280' }
const STATUS_BG    = { completed: 'rgba(16,185,129,0.1)', failed: 'rgba(239,68,68,0.1)', running: 'rgba(245,158,11,0.1)', pending: 'rgba(107,114,128,0.1)' }

function HistoryTab() {
  const [history, setHistory] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const LIMIT = 20

  const load = async (p = 1) => {
    setLoading(true)
    try {
      const org_id = getOrgId()
      const res = await fetch(`${API_BASE}/import/history?org_id=${encodeURIComponent(org_id)}&page=${p}&limit=${LIMIT}`)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setHistory(data.history || [])
      setTotal(data.total || 0)
      setPage(p)
    } catch (err) {
      toast.error(`Could not load history: ${err.message}`)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load(1) }, [])

  const totalPages = Math.ceil(total / LIMIT)

  if (loading && !history.length) {
    return <div style={{ textAlign: 'center', padding: '40px 0', color: 'var(--text-3)', fontSize: 13 }}>Loading history…</div>
  }

  if (!history.length) {
    return (
      <div style={{ textAlign: 'center', padding: '60px 0' }}>
        <div style={{ fontSize: 36, marginBottom: 12 }}>📋</div>
        <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-1)', marginBottom: 6 }}>No imports yet</div>
        <div style={{ fontSize: 12, color: 'var(--text-3)' }}>Your import history will appear here after you run your first import.</div>
      </div>
    )
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <span style={{ fontSize: 13, color: 'var(--text-3)' }}>{total.toLocaleString()} import{total !== 1 ? 's' : ''} total</span>
        <button onClick={() => load(page)} style={btn('ghost')}>↻ Refresh</button>
      </div>

      <div style={{ border: '1px solid var(--border-1)', borderRadius: 10, overflow: 'hidden' }}>
        {/* Header */}
        <div style={{
          display: 'grid', gridTemplateColumns: '2fr 90px 80px 80px 80px 80px 100px',
          background: 'var(--bg-sidebar)', padding: '8px 16px',
          fontSize: 10, fontWeight: 700, color: 'var(--text-3)',
          letterSpacing: '0.06em', textTransform: 'uppercase',
          borderBottom: '1px solid var(--border-1)',
        }}>
          <span>File</span>
          <span>Status</span>
          <span style={{ textAlign: 'right' }}>Total</span>
          <span style={{ textAlign: 'right' }}>New</span>
          <span style={{ textAlign: 'right' }}>Updated</span>
          <span style={{ textAlign: 'right' }}>Skipped</span>
          <span style={{ textAlign: 'right' }}>Date</span>
        </div>

        {history.map((row, i) => (
          <div key={row.id} style={{
            display: 'grid', gridTemplateColumns: '2fr 90px 80px 80px 80px 80px 100px',
            padding: '10px 16px', alignItems: 'center',
            borderTop: i === 0 ? 'none' : '1px solid var(--border-1)',
            background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)',
          }}>
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {row.filename || '—'}
              </div>
              {row.error && (
                <div style={{ fontSize: 10, color: '#ef4444', marginTop: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {row.error}
                </div>
              )}
            </div>
            <div>
              <span style={{
                fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 5,
                color: STATUS_COLOR[row.status] || '#6b7280',
                background: STATUS_BG[row.status] || 'rgba(107,114,128,0.1)',
                textTransform: 'capitalize',
              }}>
                {row.status}
              </span>
            </div>
            <div style={{ textAlign: 'right', fontSize: 12, color: 'var(--text-2)' }}>
              {(row.total || 0).toLocaleString()}
            </div>
            <div style={{ textAlign: 'right', fontSize: 12, color: '#10b981', fontWeight: 600 }}>
              {(row.new_count || 0).toLocaleString()}
            </div>
            <div style={{ textAlign: 'right', fontSize: 12, color: '#6366f1', fontWeight: 600 }}>
              {(row.updated_count || 0).toLocaleString()}
            </div>
            <div style={{ textAlign: 'right', fontSize: 12, color: '#f59e0b', fontWeight: 600 }}>
              {(row.skipped || 0).toLocaleString()}
            </div>
            <div style={{ textAlign: 'right', fontSize: 10, color: 'var(--text-3)' }}>
              {fmtDate(row.created_at)}
            </div>
          </div>
        ))}
      </div>

      {totalPages > 1 && (
        <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginTop: 16 }}>
          <button onClick={() => load(page - 1)} disabled={page === 1} style={btn('ghost', page === 1)}>← Prev</button>
          <span style={{ fontSize: 12, color: 'var(--text-3)', padding: '9px 4px' }}>
            Page {page} / {totalPages}
          </span>
          <button onClick={() => load(page + 1)} disabled={page >= totalPages} style={btn('ghost', page >= totalPages)}>Next →</button>
        </div>
      )}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════
// Main page
// ═══════════════════════════════════════════════════════════════════════════

export default function ImportPage() {
  const [tab, setTab] = useState('single')

  return (
    <div style={{ padding: '32px 36px', maxWidth: 980, margin: '0 auto' }}>
      <div style={{ marginBottom: 28 }}>
        <h1 style={{ fontSize: 22, fontWeight: 800, color: 'var(--text-1)', margin: 0 }}>Import Leads</h1>
        <p style={{ fontSize: 13, color: 'var(--text-3)', marginTop: 6 }}>
          Bulk import CSV / TSV / XLS / XLSX. Apollo, HubSpot, LinkedIn exports auto-mapped.
          Duplicates updated in place. Every lead gets a unique Lead ID.
        </p>
      </div>

      {/* Resume panel — shown when active jobs exist from a previous session / navigation */}
      <ResumePanel />

      <div style={card}>
        <TabBar active={tab} onChange={t => setTab(t)} />
        {tab === 'single'  && <SingleFileFlow key="single" />}
        {tab === 'folder'  && <FolderImportFlow key="folder" />}
        {tab === 'history' && <HistoryTab key="history" />}
      </div>
    </div>
  )
}
