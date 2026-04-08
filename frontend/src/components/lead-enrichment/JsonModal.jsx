import { useState, useEffect } from 'react'
import { Copy, Check, X, Code2 } from 'lucide-react'
import { toast } from 'react-hot-toast'

function JsonHighlight({ json }) {
  const html = json
    .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g, (match) => {
      let cls = '#0369a1' // number — blue
      if (/^"/.test(match)) {
        if (/:$/.test(match)) cls = '#111827' // key — near black
        else cls = '#15803d' // string value — dark green
      } else if (/true|false/.test(match)) cls = '#b45309' // amber
      else if (/null/.test(match)) cls = '#9ca3af' // gray
      return `<span style="color:${cls}">${match}</span>`
    })
  return <span dangerouslySetInnerHTML={{ __html: html }} />
}

export default function JsonModal({ data, onClose }) {
  const formatted = JSON.stringify(data, null, 2)
  const [copied, setCopied] = useState(false)
  const copy = () => {
    navigator.clipboard.writeText(formatted)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
    toast.success('JSON copied to clipboard')
  }
  useEffect(() => {
    const esc = (e) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', esc)
    return () => window.removeEventListener('keydown', esc)
  }, [onClose])

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 1000,
      background: 'rgba(0,0,0,0.65)', backdropFilter: 'blur(4px)',
      display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 20,
    }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{
        width: '100%', maxWidth: 900, maxHeight: '88vh',
        borderRadius: 14, border: '1px solid var(--border-1)',
        background: 'var(--bg-base)', display: 'flex', flexDirection: 'column',
        overflow: 'hidden', boxShadow: '0 25px 60px rgba(0,0,0,0.4)',
      }}>
        {/* Header */}
        <div style={{
          padding: '14px 18px', borderBottom: '1px solid var(--border-1)',
          display: 'flex', alignItems: 'center', gap: 10, flexShrink: 0,
        }}>
          <Code2 size={16} color="#6366f1" />
          <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-1)', flex: 1 }}>
            Full JSON — {data?.name || 'Lead'}
            <span style={{ fontSize: 10, fontWeight: 400, color: 'var(--text-3)', marginLeft: 8 }}>
              raw_profile + full_data + all fields
            </span>
          </span>
          <button onClick={copy} style={{
            display: 'flex', alignItems: 'center', gap: 5, padding: '6px 12px',
            borderRadius: 7, border: '1px solid var(--border-1)', background: 'transparent',
            color: copied ? '#10b981' : 'var(--text-2)', cursor: 'pointer', fontSize: 12, fontWeight: 500,
          }}>
            {copied ? <Check size={12} /> : <Copy size={12} />}
            {copied ? 'Copied!' : 'Copy JSON'}
          </button>
          <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-3)', padding: 4 }}>
            <X size={16} />
          </button>
        </div>
        {/* Body */}
        <div style={{ flex: 1, overflow: 'auto', padding: '14px 18px', background: '#ffffff' }}>
          <pre style={{
            margin: 0, fontSize: 12, lineHeight: 1.6,
            fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
            color: '#111827', whiteSpace: 'pre-wrap', wordBreak: 'break-word',
          }}>
            <JsonHighlight json={formatted} />
          </pre>
        </div>
      </div>
    </div>
  )
}
