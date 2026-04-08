import { CheckCircle, AlertCircle } from 'lucide-react'
import { STREAM_STAGES } from '../../../utils/leadConfig.jsx'

export function StageDot({ status }) {
  if (status === 'loading') return (
    <div style={{ width: 16, height: 16, borderRadius: '50%', border: '2px solid #6366f1', borderTopColor: 'transparent', animation: 'spin 0.7s linear infinite', flexShrink: 0 }} />
  )
  if (status === 'done')    return <CheckCircle size={16} color="#10b981" style={{ flexShrink: 0 }} />
  if (status === 'error')   return <AlertCircle  size={16} color="#ef4444" style={{ flexShrink: 0 }} />
  return <div style={{ width: 16, height: 16, borderRadius: '50%', border: '2px solid #374151', flexShrink: 0 }} />
}

export function FieldReveal({ index = 0, children, style }) {
  return (
    <div style={{ animation: 'fieldReveal 0.32s ease forwards', animationDelay: `${index * 85}ms`, opacity: 0, ...style }}>
      {children}
    </div>
  )
}

export function StreamSection({ title, stageId, stages, icon, skeleton, children }) {
  const st = stages[stageId] || { status: 'waiting' }
  if (st.status === 'waiting') return null

  return (
    <div style={{
      borderRadius: 10, border: `1px solid ${st.status === 'error' ? 'rgba(239,68,68,0.3)' : 'var(--border-1)'}`,
      background: 'var(--bg-main)', overflow: 'hidden',
    }}>
      <div style={{
        padding: '9px 14px', borderBottom: '1px solid var(--border-1)',
        display: 'flex', alignItems: 'center', gap: 7,
        background: st.status === 'loading' ? 'rgba(99,102,241,0.04)' : 'transparent',
      }}>
        <span style={{ color: st.status === 'done' ? '#10b981' : st.status === 'error' ? '#ef4444' : '#6366f1' }}>{icon}</span>
        <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)' }}>{title}</span>
        {st.status === 'loading' && <span style={{ fontSize: 10, color: '#a5b4fc', marginLeft: 2, display: 'flex', alignItems: 'center', gap: 4 }}>
          <div style={{ width: 5, height: 5, borderRadius: '50%', background: '#6366f1', animation: 'pulse 0.9s ease-in-out infinite' }} /> Fetching…
        </span>}
        {st.status === 'error' && <span style={{ fontSize: 10, color: '#ef4444', marginLeft: 2 }}>Failed</span>}
      </div>
      <div style={{ padding: '12px 14px' }}>
        {st.status === 'loading' && (skeleton || <SkeletonLines />)}
        {st.status === 'done'    && st.data && children(st.data)}
        {st.status === 'error'   && <span style={{ fontSize: 12, color: '#ef4444' }}>Stage failed — enrichment continues</span>}
      </div>
    </div>
  )
}

const SK = ({ w = '100%', h = 10, r = 5, delay = 0, style: s }) => (
  <div style={{ width: w, height: h, borderRadius: r, background: 'var(--bg-elevated)', animation: `pulse 1.5s ease-in-out ${delay}s infinite`, ...s }} />
)

export function SkeletonProfile() {
  return (
    <div style={{ display: 'flex', gap: 14 }}>
      <SK w={54} h={54} r={10} style={{ flexShrink: 0 }} />
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8, paddingTop: 4 }}>
        <SK w="55%" h={14} r={4} delay={0} />
        <SK w="70%" h={10} r={4} delay={0.1} />
        <div style={{ display: 'flex', gap: 7 }}>
          <SK w={90} h={20} r={10} delay={0.15} />
          <SK w={110} h={20} r={10} delay={0.2} />
          <SK w={85} h={20} r={10} delay={0.25} />
        </div>
        <SK w="90%" h={9} r={4} delay={0.3} />
        <SK w="75%" h={9} r={4} delay={0.35} />
      </div>
    </div>
  )
}

export function SkeletonCompany() {
  return (
    <div style={{ display: 'flex', gap: 14 }}>
      <SK w={44} h={44} r={8} style={{ flexShrink: 0 }} />
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8 }}>
        <SK w="50%" h={11} r={4} delay={0} />
        <div style={{ display: 'flex', gap: 7 }}>
          <SK w={80} h={20} r={10} delay={0.1} />
          <SK w={120} h={20} r={10} delay={0.15} />
          <SK w={100} h={20} r={10} delay={0.2} />
        </div>
        <div style={{ display: 'flex', gap: 7 }}>
          <SK w={70} h={20} r={10} delay={0.25} />
          <SK w={90} h={20} r={10} delay={0.3} />
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          {[55,65,50,70,60].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.35 + i * 0.05} />)}
        </div>
      </div>
    </div>
  )
}

export function SkeletonContact() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 10px', borderRadius: 7, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
        <SK w={14} h={14} r={7} />
        <SK w="50%" h={10} r={4} delay={0.05} />
        <SK w={60} h={18} r={9} delay={0.1} style={{ marginLeft: 'auto' }} />
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 10px', borderRadius: 7, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
        <SK w={14} h={14} r={7} delay={0.1} />
        <SK w="35%" h={10} r={4} delay={0.15} />
      </div>
    </div>
  )
}

export function SkeletonWebsite() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ padding: '10px 12px', borderRadius: 8, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', display: 'flex', flexDirection: 'column', gap: 6 }}>
        <SK w="90%" h={9} r={4} delay={0} />
        <SK w="75%" h={9} r={4} delay={0.1} />
      </div>
      <div style={{ display: 'flex', gap: 7 }}>
        {[70,90,85,80].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.15 + i * 0.05} />)}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <SK w={110} h={9} r={4} delay={0.35} />
        <div style={{ display: 'flex', gap: 6 }}>
          {[80,100,70,90].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.4 + i * 0.05} />)}
        </div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <SK w={110} h={9} r={4} delay={0.6} />
        <div style={{ display: 'flex', gap: 6 }}>
          {[90,75,85].map((w, i) => <SK key={i} w={w} h={20} r={10} delay={0.65 + i * 0.05} />)}
        </div>
      </div>
      <SK w="80%" h={9} r={4} delay={0.8} />
    </div>
  )
}

export function SkeletonScoring() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
        <SK w={48} h={48} r={24} delay={0} />
        <SK w={70} h={22} r={11} delay={0.1} />
        <SK w={120} h={11} r={4} delay={0.15} />
      </div>
      <div style={{ display: 'flex', gap: 6 }}>
        {[1,2,3].map(i => (
          <div key={i} style={{ flex: 1, padding: '8px 10px', borderRadius: 6, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6 }}>
            <SK w={28} h={22} r={4} delay={i * 0.1} />
            <SK w="70%" h={8} r={4} delay={i * 0.1 + 0.05} />
          </div>
        ))}
      </div>
      <SK w="100%" h={36} r={6} delay={0.35} />
      <div style={{ display: 'flex', gap: 7 }}>
        <SK w={80} h={22} r={11} delay={0.4} />
        <SK w="55%" h={11} r={4} delay={0.45} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
        <SK w={70} h={9} r={4} delay={0.5} />
        <SK w="100%" h={70} r={6} delay={0.55} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
        <SK w={80} h={9} r={4} delay={0.7} />
        <SK w="100%" h={40} r={6} delay={0.75} />
      </div>
    </div>
  )
}

export function SkeletonLines() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {[80, 55, 65].map((w, i) => (
        <SK key={i} w={`${w}%`} h={10} r={5} delay={i * 0.12} />
      ))}
    </div>
  )
}

export const tagStyle = (color) => ({
  display: 'inline-flex', alignItems: 'center', gap: 4,
  fontSize: 10, fontWeight: 500, padding: '2px 8px', borderRadius: 5,
  background: `${color}18`, color, border: `1px solid ${color}30`,
})

export const contactRow = {
  display: 'flex', alignItems: 'center', gap: 7,
  padding: '6px 10px', borderRadius: 7,
  background: 'var(--bg-elevated)', border: '1px solid var(--border-1)',
}
