// api.js — shared backend utilities

export const BACKEND = `${import.meta.env.VITE_BACKEND_URL || 'https://leadenrichment-production-5b78.up.railway.app'}/api`

export const getToken = () => localStorage.getItem('wb_ai_token') || ''
export const jsonHdr  = () => ({ 'Content-Type': 'application/json' })

export function getOrgId() {
  try {
    const t = getToken()
    if (!t) return 'default'
    const payload = JSON.parse(atob(t.split('.')[1].replace(/-/g,'+').replace(/_/g,'/')))
    return payload.organization_id || payload.org_id || 'default'
  } catch { return 'default' }
}
