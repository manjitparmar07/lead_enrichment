// App.jsx — WorksBuddy Lead Enrichment
import { useState, lazy, Suspense } from 'react'
import { Toaster } from 'react-hot-toast'
import { BrowserRouter, Routes, Route, NavLink, Navigate } from 'react-router-dom'

// ── Lazy page imports ──────────────────────────────────────────────────────
// Each page is a separate chunk — only downloaded when first visited.
// Initial bundle drops from ~338 KB → ~120 KB (react + router + hot-toast).
const LoginPage               = lazy(() => import('./pages/LoginPage'))
const LeadEnrichmentPage      = lazy(() => import('./pages/LeadEnrichmentPage'))
const LeadEnrichmentConfigPage = lazy(() => import('./pages/LeadEnrichmentConfigPage'))
const ApiDocPage              = lazy(() => import('./pages/ApiDocPage'))
const LioConfigPage           = lazy(() => import('./pages/LioConfigPage'))
const SystemPromptGeneratorPage = lazy(() => import('./pages/SystemPromptGeneratorPage'))
const LinkedInFinderPage        = lazy(() => import('./pages/LinkedInFinderPage'))
const CustomFeaturesPage        = lazy(() => import('./pages/CustomFeaturesPage'))
const ApiUsagePage              = lazy(() => import('./pages/ApiUsagePage'))
const BrightDataFilterPage      = lazy(() => import('./pages/BrightDataFilterPage'))

// ── Suspense fallback ──────────────────────────────────────────────────────

function PageLoader() {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      height: '100%', gap: 10, color: 'var(--text-3)', fontSize: 13,
    }}>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
        style={{ animation: 'spin 0.8s linear infinite' }}>
        <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round"/>
      </svg>
      Loading…
      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
    </div>
  )
}

// ── Auth helpers ───────────────────────────────────────────────────────────

function getStoredAuth() {
  try {
    const token = localStorage.getItem('wb_ai_token')
    if (!token) return null
    const payload = JSON.parse(atob(token.split('.')[1]))
    if (payload.exp && payload.exp * 1000 < Date.now()) {
      localStorage.removeItem('wb_ai_token')
      localStorage.removeItem('wb_ai_user')
      return null
    }
    return { token, user: JSON.parse(localStorage.getItem('wb_ai_user') || '{}') }
  } catch {
    return null
  }
}

// ── App root ───────────────────────────────────────────────────────────────

export default function App() {
  const [auth, setAuth] = useState(() => getStoredAuth())

  if (!auth) {
    return (
      <>
        <ToasterSetup />
        <Suspense fallback={null}>
          <LoginPage onLogin={(user) => {
            setAuth({ token: localStorage.getItem('wb_ai_token'), user })
          }} />
        </Suspense>
      </>
    )
  }

  const handleLogout = () => {
    localStorage.removeItem('wb_ai_token')
    localStorage.removeItem('wb_ai_user')
    setAuth(null)
  }

  return (
    <BrowserRouter>
      <ToasterSetup />
      <AppShell user={auth.user} onLogout={handleLogout} />
    </BrowserRouter>
  )
}

function ToasterSetup() {
  return (
    <Toaster
      position="top-right"
      toastOptions={{
        style: {
          background: '#1e1e1e', color: '#e8e8e8',
          border: '1px solid #2a2a2a', borderRadius: '10px',
          fontSize: '13px', fontFamily: "'Inter', system-ui, sans-serif",
        },
        error:   { iconTheme: { primary: '#ef4444', secondary: '#fff' } },
        success: { iconTheme: { primary: '#10b981', secondary: '#fff' } },
      }}
    />
  )
}

// ── Nav config ─────────────────────────────────────────────────────────────

const NAV_ITEMS = [
  {
    to: '/leads', label: 'Lead Enrichment', badge: 'LIVE',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/><line x1="19" y1="8" x2="23" y2="12"/><line x1="23" y1="8" x2="19" y2="12"/></svg>,
  },
  {
    to: '/config', label: 'Tool Configuration',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/><path d="M12 2v2M12 20v2M2 12h2M20 12h2"/></svg>,
  },
  {
    to: '/lio-config', label: 'LIO Config',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>,
  },
  {
    to: '/api-doc', label: 'API Doc',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>,
  },
  {
    to: '/prompt-generator', label: 'Prompt Generator',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg>,
  },
  {
    to: '/linkedin-finder', label: 'LinkedIn Finder',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>,
  },
  {
    to: '/custom-features', label: 'Custom Features',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>,
  },
  {
    to: '/api-usage', label: 'API Usage',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/><line x1="2" y1="20" x2="22" y2="20"/></svg>,
  },
  {
    to: '/bd-filter', label: 'BrightData Filter',
    icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/></svg>,
  },
]

// ── App shell ──────────────────────────────────────────────────────────────

function AppShell({ user, onLogout }) {
  return (
    <div style={{ display: 'flex', height: '100vh', overflow: 'hidden', background: 'var(--bg-base)' }}>

      {/* Left nav */}
      <nav style={{
        width: 220, minWidth: 220, height: '100vh', flexShrink: 0,
        background: 'var(--bg-sidebar)', borderRight: '1px solid var(--border-1)',
        display: 'flex', flexDirection: 'column', zIndex: 50,
      }}>
        {/* Logo */}
        <div style={{
          padding: '16px 18px', borderBottom: '1px solid var(--border-1)',
          display: 'flex', alignItems: 'center', gap: 10, minHeight: 56,
        }}>
          <div style={{
            width: 28, height: 28, borderRadius: 8,
            background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
          }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><path d="M11 8v6M8 11h6"/>
            </svg>
          </div>
          <div>
            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-1)' }}>WorksBuddy</div>
            <div style={{ fontSize: 10, color: 'var(--text-3)' }}>Lead Enrichment</div>
          </div>
        </div>

        {/* Nav items */}
        <div style={{ flex: 1, padding: '10px 8px', overflowY: 'auto' }}>
          {NAV_ITEMS.map(item => (
            <NavLink
              key={item.to}
              to={item.to}
              style={({ isActive }) => ({
                width: '100%', display: 'flex', alignItems: 'center', gap: 10,
                padding: '8px 12px', borderRadius: 9, marginBottom: 4,
                background: isActive ? 'rgba(99,102,241,0.12)' : 'transparent',
                border: isActive ? '1px solid rgba(99,102,241,0.25)' : '1px solid transparent',
                color: isActive ? '#a5b4fc' : 'var(--text-3)',
                fontSize: 12, fontWeight: 600, cursor: 'pointer',
                transition: 'all 0.15s', textDecoration: 'none',
              })}
            >
              {item.icon}
              {item.label}
              {item.badge && (
                <span style={{
                  marginLeft: 'auto', fontSize: 8, padding: '1px 5px', borderRadius: 4,
                  background: 'rgba(99,102,241,0.2)', color: '#a5b4fc', fontWeight: 700,
                }}>{item.badge}</span>
              )}
            </NavLink>
          ))}
        </div>

        {/* User + logout */}
        <div style={{
          padding: '12px 14px', borderTop: '1px solid var(--border-1)',
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          <div style={{
            width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
            background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 11, fontWeight: 700, color: '#fff',
          }}>
            {(user?.name || user?.email || 'U')[0].toUpperCase()}
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {user?.name || user?.email || 'User'}
            </div>
            <div style={{ fontSize: 10, color: 'var(--text-3)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {user?.email || ''}
            </div>
          </div>
          <button
            onClick={onLogout}
            title="Log out"
            style={{
              background: 'transparent', border: '1px solid var(--border-1)',
              color: 'var(--text-3)', cursor: 'pointer', borderRadius: 7,
              padding: 5, display: 'flex', alignItems: 'center', flexShrink: 0,
            }}
          >
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/>
              <polyline points="16 17 21 12 16 7"/>
              <line x1="21" y1="12" x2="9" y2="12"/>
            </svg>
          </button>
        </div>
      </nav>

      {/* Main content — each page lazy-loaded on first visit */}
      <main style={{ flex: 1, height: '100vh', overflowY: 'auto', background: 'var(--bg-base)' }}>
        <Suspense fallback={<PageLoader />}>
          <Routes>
            <Route path="/" element={<Navigate to="/leads" replace />} />
            <Route path="/leads"    element={<LeadEnrichmentPage />} />
            <Route path="/config"   element={<LeadEnrichmentConfigPage />} />
            <Route path="/lio-config" element={<LioConfigPage />} />
<Route path="/api-doc"          element={<ApiDocPage />} />
            <Route path="/prompt-generator"  element={<SystemPromptGeneratorPage />} />
            <Route path="/linkedin-finder"    element={<LinkedInFinderPage />} />
            <Route path="/custom-features"  element={<CustomFeaturesPage />} />
            <Route path="/api-usage"        element={<ApiUsagePage />} />
            <Route path="/bd-filter"        element={<BrightDataFilterPage />} />
            <Route path="*"                 element={<Navigate to="/leads" replace />} />
          </Routes>
        </Suspense>
      </main>

      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
    </div>
  )
}

