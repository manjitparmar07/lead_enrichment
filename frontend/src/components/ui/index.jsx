// ui/index.jsx — all micro UI components
import { useState, useEffect } from 'react'
import {
  Copy, Check, AlertCircle, CheckCircle, RefreshCw, Building2, User,
  Clock,
} from 'lucide-react'
import { TIER } from '../../utils/leadConfig.jsx'

export function Grid({ children }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(220px,1fr))', gap: 8 }}>
      {children}
    </div>
  )
}

export function Field({ icon, label, value, badge, link, copyable, wide }) {
  const [copied, setCopied] = useState(false)
  const isEmpty = !value || value === '—' || (typeof value === 'string' && !value.trim())
  const copy = () => {
    if (typeof value === 'string') {
      navigator.clipboard.writeText(value)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    }
  }
  return (
    <div style={{
      padding: '9px 12px', borderRadius: 8,
      background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
      gridColumn: wide ? '1/-1' : undefined,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 4 }}>
        <span style={{ color: 'var(--text-3)', display: 'flex' }}>{icon}</span>
        <span style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, minHeight: 20 }}>
        {link && !isEmpty ? (
          <a href={link.startsWith('http') ? link : `https://${link}`} target="_blank" rel="noreferrer"
            style={{ fontSize: 12, color: '#6366f1', wordBreak: 'break-all', flex: 1, textDecoration: 'none' }}>
            {value}
          </a>
        ) : (
          <span style={{ fontSize: 12, color: isEmpty ? 'var(--text-3)' : 'var(--text-1)', flex: 1, wordBreak: 'break-word', fontStyle: isEmpty ? 'italic' : 'normal' }}>
            {isEmpty ? '—' : (typeof value === 'string' ? value : value)}
          </span>
        )}
        {badge}
        {copyable && !isEmpty && typeof value === 'string' && (
          <button onClick={copy} style={{ background: 'none', border: 'none', cursor: 'pointer', color: copied ? '#10b981' : 'var(--text-3)', padding: 1, flexShrink: 0 }}>
            {copied ? <Check size={11} /> : <Copy size={11} />}
          </button>
        )}
      </div>
    </div>
  )
}

export function ScoreBar({ label, score, max, color }) {
  return (
    <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontSize: 11, color: 'var(--text-3)', fontWeight: 500 }}>{label}</span>
        <span style={{ fontSize: 12, fontWeight: 700, color }}>{score}<span style={{ fontSize: 10, fontWeight: 400, color: 'var(--text-3)' }}>/{max}</span></span>
      </div>
      <div style={{ height: 5, borderRadius: 3, background: 'var(--bg-base)', overflow: 'hidden' }}>
        <div style={{ height: '100%', borderRadius: 3, background: color, width: `${Math.min((score/max)*100,100)}%`, transition: 'width 0.5s' }} />
      </div>
    </div>
  )
}

export function ScoreCircle({ score, tier }) {
  const cfg = TIER[tier] || TIER.cold
  return (
    <div style={{
      width: 36, height: 36, borderRadius: '50%', flexShrink: 0,
      border: `2px solid ${cfg.border}`, background: cfg.bg,
      display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
    }}>
      <span style={{ fontSize: 11, fontWeight: 800, color: cfg.color, lineHeight: 1 }}>{score}</span>
    </div>
  )
}

export function TierBadge({ tier }) {
  const cfg = TIER[tier] || TIER.cold
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3, padding: '2px 8px', borderRadius: 10,
      fontSize: 10, fontWeight: 700, background: cfg.bg, border: `1px solid ${cfg.border}`, color: cfg.color }}>
      {cfg.icon} {cfg.label}
    </span>
  )
}

export function Avatar({ name, tier, src, size = 34 }) {
  const cfg = TIER[tier] || TIER.cold
  const [imgErr, setImgErr] = useState(false)
  if (src && !imgErr) {
    return (
      <img src={src} alt={name || ''} onError={() => setImgErr(true)}
        referrerPolicy="no-referrer" crossOrigin="anonymous"
        style={{ width: size, height: size, borderRadius: '50%', objectFit: 'cover', flexShrink: 0,
          border: `2px solid ${cfg.border}` }} />
    )
  }
  return (
    <div style={{
      width: size, height: size, borderRadius: '50%', flexShrink: 0,
      background: `linear-gradient(135deg,${cfg.color}25,${cfg.color}50)`,
      border: `2px solid ${cfg.border}`,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontSize: Math.floor(size * 0.38), fontWeight: 700, color: cfg.color,
    }}>
      {(name || '?')[0].toUpperCase()}
    </div>
  )
}

export function CompanyLogo({ src, name, size = 28 }) {
  const [imgErr, setImgErr] = useState(false)
  if (src && !imgErr) {
    return (
      <img src={src} alt={name || ''} onError={() => setImgErr(true)}
        style={{ width: size, height: size, borderRadius: 6, objectFit: 'contain', flexShrink: 0,
          background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', padding: 2 }} />
    )
  }
  return (
    <div style={{
      width: size, height: size, borderRadius: 6, flexShrink: 0,
      background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
    }}>
      <Building2 size={Math.floor(size * 0.55)} color="#6366f1" />
    </div>
  )
}

export function TagBadge({ text, size }) {
  return (
    <span style={{
      fontSize: size === 'md' ? 11 : 9, fontWeight: 600, padding: size === 'md' ? '3px 9px' : '1px 6px',
      borderRadius: 10, background: 'rgba(99,102,241,0.1)', color: '#a5b4fc',
      border: '1px solid rgba(99,102,241,0.2)',
    }}>{text}</span>
  )
}

export function TagList({ items, color }) {
  if (!items || !items.length) return <span style={{ color: 'var(--text-3)', fontStyle: 'italic', fontSize: 12 }}>—</span>
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
      {items.map(item => (
        <span key={item} style={{ fontSize: 10, padding: '1px 7px', borderRadius: 10, fontWeight: 600,
          background: `${color}15`, color, border: `1px solid ${color}30` }}>{item}</span>
      ))}
    </div>
  )
}

export function ConfBadge({ c, src }) {
  const color = c === 'high' ? '#10b981' : c === 'medium' ? '#f97316' : '#6b7280'
  return (
    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 10, fontWeight: 700,
      background: `${color}15`, color, border: `1px solid ${color}35`, whiteSpace: 'nowrap' }}>
      {c} · {src}
    </span>
  )
}

export function FundingBadge({ stage }) {
  const colors = {
    'series b': '#8b5cf6', 'series c': '#6366f1', 'series a': '#f97316',
    seed: '#10b981', 'pre-seed': '#3b82f6', angel: '#fbbf24',
  }
  const color = colors[(stage || '').toLowerCase()] || '#6b7280'
  return (
    <span style={{ fontSize: 9, padding: '1px 7px', borderRadius: 10, fontWeight: 700,
      background: `${color}18`, color, border: `1px solid ${color}35` }}>{stage}</span>
  )
}

export function EmailStatusBadge({ status }) {
  if (!status) return null
  const isVerified = status.toLowerCase().includes('valid') || status.toLowerCase().includes('verified')
  const color = isVerified ? '#10b981' : '#f97316'
  return (
    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 10, fontWeight: 700,
      background: `${color}15`, color, border: `1px solid ${color}35` }}>
      {isVerified ? '✓ ' : '⚠ '}{status}
    </span>
  )
}

export function ActiveBadge() {
  return (
    <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 10, fontWeight: 700,
      background: 'rgba(16,185,129,0.12)', color: '#10b981', border: '1px solid rgba(16,185,129,0.3)' }}>
      Active
    </span>
  )
}

export function StatusBadge({ status }) {
  const map = {
    running:               { color: '#6366f1', icon: <RefreshSpin size={10} />, label: 'Running' },
    pending:               { color: '#f97316', icon: <Clock size={10} />,       label: 'Pending' },
    completed:             { color: '#10b981', icon: <CheckCircle size={10} />, label: 'Completed' },
    completed_with_errors: { color: '#f97316', icon: <AlertCircle size={10} />, label: 'Completed (errors)' },
    failed:                { color: '#ef4444', icon: <AlertCircle size={10} />, label: 'Failed' },
    fallback:              { color: '#8b5cf6', icon: <RefreshSpin size={10} />, label: 'Processing' },
    stale:                 { color: '#6b7280', icon: <AlertCircle size={10} />, label: 'Stale' },
  }
  const cfg = map[status] || { color: '#6b7280', icon: null, label: status }
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px',
      borderRadius: 10, fontSize: 11, fontWeight: 600,
      background: `${cfg.color}18`, color: cfg.color, border: `1px solid ${cfg.color}35` }}>
      {cfg.icon} {cfg.label}
    </span>
  )
}

export function CopyBlock({ text, multiline }) {
  const [copied, setCopied] = useState(false)
  const copy = () => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1800) }
  return (
    <div style={{ position: 'relative' }}>
      <div style={{
        padding: '10px 40px 10px 12px', borderRadius: 8, border: '1px solid var(--border-1)',
        background: 'var(--bg-elevated)', fontSize: 12, color: 'var(--text-1)', lineHeight: 1.6,
        whiteSpace: multiline ? 'pre-wrap' : 'normal',
      }}>{text}</div>
      <button onClick={copy} style={{ position: 'absolute', top: 8, right: 8,
        background: 'none', border: 'none', cursor: 'pointer', color: copied ? '#10b981' : 'var(--text-3)' }}>
        {copied ? <Check size={13} /> : <Copy size={13} />}
      </button>
    </div>
  )
}

export function Card({ children, style }) {
  return <div style={{ padding: '16px 18px', borderRadius: 10, background: 'var(--bg-card)', border: '1px solid var(--border-1)', ...style }}>{children}</div>
}

export function Lbl({ children, style }) {
  return <span style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', ...style }}>{children}</span>
}

export function Btn({ children, onClick, loading, disabled, style }) {
  return (
    <button onClick={onClick} disabled={loading || disabled} style={{
      display: 'flex', alignItems: 'center', gap: 7,
      padding: '10px 20px', borderRadius: 8, border: 'none',
      background: loading || disabled ? 'var(--bg-elevated)' : 'linear-gradient(135deg,#6366f1,#8b5cf6)',
      color: loading || disabled ? 'var(--text-3)' : '#fff',
      cursor: loading || disabled ? 'not-allowed' : 'pointer',
      fontSize: 13, fontWeight: 600, transition: 'all 0.15s', ...style,
    }}>{children}</button>
  )
}

export function GhostBtn({ children, onClick, style }) {
  return (
    <button onClick={onClick} style={{
      display: 'flex', alignItems: 'center', gap: 5,
      padding: '5px 10px', borderRadius: 7, fontSize: 12, fontWeight: 500, cursor: 'pointer',
      border: '1px solid var(--border-1)', background: 'transparent', color: 'var(--text-2)',
      ...style,
    }}>{children}</button>
  )
}

export function ErrorBox({ msg }) {
  return (
    <div style={{ padding: '11px 14px', borderRadius: 8, marginBottom: 14,
      background: 'rgba(239,68,68,0.07)', border: '1px solid rgba(239,68,68,0.25)',
      color: '#ef4444', fontSize: 13, display: 'flex', gap: 8, alignItems: 'flex-start' }}>
      <AlertCircle size={14} style={{ flexShrink: 0, marginTop: 1 }} />{msg}
    </div>
  )
}

export function LoadingCard() {
  const steps = [
    'Connecting to Bright Data…',
    'Scraping LinkedIn profile…',
    'Running email waterfall (Hunter → Apollo)…',
    'Building 7-category intelligence report…',
    'Generating AI outreach sequences…',
    'Calculating lead score & ICP match…',
  ]
  const [step, setStep] = useState(0)
  useEffect(() => {
    const t = setInterval(() => setStep(s => (s + 1) % steps.length), 2000)
    return () => clearInterval(t)
  }, [])
  return (
    <Card>
      <div style={{ padding: '20px 0', textAlign: 'center' }}>
        <div style={{ width: 40, height: 40, borderRadius: '50%', margin: '0 auto 14px',
          border: '3px solid var(--border-1)', borderTop: '3px solid #6366f1',
          animation: 'spin 0.8s linear infinite' }} />
        <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 4 }}>{steps[step]}</div>
        <div style={{ fontSize: 11, color: 'var(--text-3)' }}>30–60 seconds · 7 enrichment categories</div>
      </div>
    </Card>
  )
}

export function RefreshSpin({ size = 14 }) {
  return <RefreshCw size={size} style={{ animation: 'spin 1s linear infinite' }} />
}

export const inputStyle = {
  padding: '9px 13px', borderRadius: 8,
  border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
  color: 'var(--text-1)', fontSize: 13, outline: 'none', flex: 1,
}

export const selStyle = {
  padding: '7px 11px', borderRadius: 7, fontSize: 12,
  border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
  color: 'var(--text-1)', cursor: 'pointer', outline: 'none',
}
