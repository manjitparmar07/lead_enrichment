// LoginPage.jsx — WorksBuddy SSO login gate (admin-only)

import { useState } from 'react'
import { LogIn, Mail, Lock, Eye, EyeOff, AlertCircle } from 'lucide-react'

const BACKEND = `${import.meta.env.VITE_BACKEND_URL || ''}/api`

export default function LoginPage({ onLogin }) {
  const [email, setEmail]       = useState('')
  const [password, setPassword] = useState('')
  const [showPwd, setShowPwd]   = useState(false)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState('')

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!email.trim() || !password.trim()) {
      setError('Email and password are required.')
      return
    }
    setError('')
    setLoading(true)
    try {
      const res = await fetch(`${BACKEND}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: email.trim(), password }),
      })
      const data = await res.json()
      if (!res.ok) {
        setError(data.detail || 'Login failed. Please try again.')
        return
      }
      // Store token + user info
      localStorage.setItem('wb_ai_token', data.access_token)
      localStorage.setItem('wb_ai_user', JSON.stringify(data.user))
      onLogin(data.user)
    } catch {
      setError('Cannot reach the server. Please check your connection.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{
      minHeight: '100vh',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      background: 'var(--bg-base)',
      fontFamily: "'Inter', system-ui, sans-serif",
    }}>

      {/* Card */}
      <div style={{
        width: '100%',
        maxWidth: 400,
        margin: '0 16px',
        background: 'var(--bg-sidebar)',
        border: '1px solid var(--border-1)',
        borderRadius: 16,
        padding: '36px 32px',
        boxShadow: '0 8px 40px rgba(0,0,0,0.18)',
      }}>

        {/* Logo area */}
        <div style={{ textAlign: 'center', marginBottom: 28 }}>
          <div style={{
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
            width: 52, height: 52, borderRadius: 14,
            background: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
            marginBottom: 14,
            boxShadow: '0 4px 16px rgba(99,102,241,0.35)',
          }}>
            <LogIn size={24} color="#fff" />
          </div>
          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--text-1)', marginBottom: 4 }}>
            WorksBuddy AI Brain
          </div>
          <div style={{ fontSize: 13, color: 'var(--text-3)' }}>
            Sign in with your WorksBuddy admin account
          </div>
        </div>

        {/* Error banner */}
        {error && (
          <div style={{
            display: 'flex', alignItems: 'flex-start', gap: 8,
            padding: '10px 12px', borderRadius: 8, marginBottom: 18,
            background: 'rgba(239,68,68,0.1)',
            border: '1px solid rgba(239,68,68,0.3)',
            color: '#f87171', fontSize: 13,
          }}>
            <AlertCircle size={15} style={{ flexShrink: 0, marginTop: 1 }} />
            <span>{error}</span>
          </div>
        )}

        {/* Form */}
        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

          {/* Email */}
          <div>
            <label style={{ fontSize: 12, fontWeight: 500, color: 'var(--text-2)', display: 'block', marginBottom: 6 }}>
              Email address
            </label>
            <div style={{ position: 'relative' }}>
              <Mail size={15} style={{
                position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)',
                color: 'var(--text-3)',
              }} />
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="admin@worksbuddy.ai"
                autoComplete="email"
                style={{
                  width: '100%', boxSizing: 'border-box',
                  padding: '10px 12px 10px 36px',
                  borderRadius: 9, border: '1px solid var(--border-1)',
                  background: 'var(--bg-elevated)', color: 'var(--text-1)',
                  fontSize: 14, outline: 'none',
                  transition: 'border-color 0.15s',
                }}
                onFocus={(e) => e.target.style.borderColor = '#6366f1'}
                onBlur={(e) => e.target.style.borderColor = 'var(--border-1)'}
              />
            </div>
          </div>

          {/* Password */}
          <div>
            <label style={{ fontSize: 12, fontWeight: 500, color: 'var(--text-2)', display: 'block', marginBottom: 6 }}>
              Password
            </label>
            <div style={{ position: 'relative' }}>
              <Lock size={15} style={{
                position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)',
                color: 'var(--text-3)',
              }} />
              <input
                type={showPwd ? 'text' : 'password'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                autoComplete="current-password"
                style={{
                  width: '100%', boxSizing: 'border-box',
                  padding: '10px 40px 10px 36px',
                  borderRadius: 9, border: '1px solid var(--border-1)',
                  background: 'var(--bg-elevated)', color: 'var(--text-1)',
                  fontSize: 14, outline: 'none',
                  transition: 'border-color 0.15s',
                }}
                onFocus={(e) => e.target.style.borderColor = '#6366f1'}
                onBlur={(e) => e.target.style.borderColor = 'var(--border-1)'}
              />
              <button
                type="button"
                onClick={() => setShowPwd((v) => !v)}
                style={{
                  position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                  background: 'transparent', border: 'none', cursor: 'pointer',
                  color: 'var(--text-3)', padding: 4, display: 'flex',
                }}
              >
                {showPwd ? <EyeOff size={15} /> : <Eye size={15} />}
              </button>
            </div>
          </div>

          {/* Submit */}
          <button
            type="submit"
            disabled={loading}
            style={{
              marginTop: 6,
              padding: '11px 16px',
              borderRadius: 9,
              border: 'none',
              background: loading ? 'rgba(99,102,241,0.5)' : 'linear-gradient(135deg, #6366f1, #8b5cf6)',
              color: '#fff',
              fontSize: 14,
              fontWeight: 600,
              cursor: loading ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 8,
              boxShadow: loading ? 'none' : '0 2px 12px rgba(99,102,241,0.3)',
              transition: 'all 0.15s',
            }}
          >
            {loading ? (
              <>
                <span style={{
                  width: 16, height: 16, borderRadius: '50%',
                  border: '2px solid rgba(255,255,255,0.3)',
                  borderTopColor: '#fff',
                  animation: 'spin 0.7s linear infinite',
                  display: 'inline-block',
                }} />
                Signing in...
              </>
            ) : (
              <>
                <LogIn size={15} />
                Sign in
              </>
            )}
          </button>
        </form>

        {/* Footer note */}
        <div style={{ marginTop: 24, textAlign: 'center', fontSize: 11, color: 'var(--text-3)', lineHeight: 1.5 }}>
          Access restricted to WorksBuddy organization admins only.
        </div>
      </div>

      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        input:-webkit-autofill {
          -webkit-box-shadow: 0 0 0 1000px var(--bg-elevated) inset !important;
          -webkit-text-fill-color: var(--text-1) !important;
        }
      `}</style>
    </div>
  )
}
