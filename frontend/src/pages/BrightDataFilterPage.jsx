// BrightDataFilterPage.jsx — LinkedIn people search via BrightData or Apollo
import { useState, useCallback, useEffect } from 'react'
import toast from 'react-hot-toast'

const BACKEND = `${import.meta.env.VITE_BACKEND_URL || 'https://api-lead-enrichment-worksbuddy.lbmdemo.com'}/api`

// ─────────────────────────────────────────────────────────────────────────────
// BrightData — field & operator definitions
// ─────────────────────────────────────────────────────────────────────────────

const BD_FIELDS = [
  // ── Identity ──────────────────────────────────────────────────────────────
  { value: 'position',                      label: 'Position / Title',        type: 'text'   },
  { value: 'name',                          label: 'Full Name',               type: 'text'   },
  { value: 'first_name',                    label: 'First Name',              type: 'text'   },
  { value: 'last_name',                     label: 'Last Name',               type: 'text'   },
  // ── Location ──────────────────────────────────────────────────────────────
  { value: 'city',                          label: 'City',                    type: 'text'   },
  { value: 'country_code',                  label: 'Country Code (US/GB…)',   type: 'text'   },
  { value: 'location',                      label: 'Location (full string)',   type: 'text'   },
  // ── Current company ───────────────────────────────────────────────────────
  { value: 'current_company_name',          label: 'Company Name',            type: 'text'   },
  { value: 'current_company_industry',      label: 'Company Industry',        type: 'text'   },
  { value: 'current_company_employee_count',label: 'Company Employees',       type: 'number' },
  { value: 'current_company_link',          label: 'Company LinkedIn URL',    type: 'text'   },
  // ── Profile content ───────────────────────────────────────────────────────
  { value: 'about',                         label: 'About / Summary',         type: 'text'   },
  { value: 'skills',                        label: 'Skills',                  type: 'text'   },
  { value: 'industry',                      label: 'Industry',                type: 'text'   },
  { value: 'language',                      label: 'Language',                type: 'text'   },
  // ── Education ─────────────────────────────────────────────────────────────
  { value: 'school',                        label: 'School / University',     type: 'text'   },
  { value: 'degree_name',                   label: 'Degree',                  type: 'text'   },
  { value: 'field_of_study',                label: 'Field of Study',          type: 'text'   },
  { value: 'certification',                 label: 'Certification',           type: 'text'   },
  // ── Social stats ──────────────────────────────────────────────────────────
  { value: 'followers',                     label: 'Followers',               type: 'number' },
  { value: 'connections',                   label: 'Connections',             type: 'number' },
  // ── URL ───────────────────────────────────────────────────────────────────
  { value: 'url',                           label: 'LinkedIn URL',            type: 'text'   },
]

const TEXT_OPS = [
  { value: 'includes',     label: 'includes' },
  { value: '=',            label: '= equals' },
  { value: '!=',           label: '≠ not equals' },
  { value: 'starts_with',  label: 'starts with' },
  { value: 'ends_with',    label: 'ends with' },
  { value: 'is_not_null',  label: 'is not empty', noValue: true },
]

const NUM_OPS = [
  { value: '>',           label: '> greater than' },
  { value: '<',           label: '< less than' },
  { value: '=',           label: '= equals' },
  { value: '!=',          label: '≠ not equals' },
  { value: 'is_not_null', label: 'is not empty', noValue: true },
]

function getBDOps(fieldValue) {
  const f = BD_FIELDS.find(f => f.value === fieldValue)
  return f?.type === 'number' ? NUM_OPS : TEXT_OPS
}

function newBDRule() {
  return { id: Math.random().toString(36).slice(2), field: 'position', operator: 'includes', value: '' }
}

function buildBDFilter(rules, globalOp) {
  const valid = rules.filter(r => {
    const op = getBDOps(r.field).find(o => o.value === r.operator)
    return op?.noValue ? true : r.value.trim() !== ''
  })
  if (!valid.length) return null
  const toFilter = r => {
    const op = getBDOps(r.field).find(o => o.value === r.operator)
    return op?.noValue ? { name: r.field, operator: r.operator }
      : { name: r.field, operator: r.operator, value: r.value.trim() }
  }
  if (valid.length === 1) return toFilter(valid[0])
  return { operator: globalOp, filters: valid.map(toFilter) }
}

// ─────────────────────────────────────────────────────────────────────────────
// Apollo — field & operator definitions (same pattern as BrightData)
// ─────────────────────────────────────────────────────────────────────────────

const APOLLO_FIELDS = [
  // ── Person ────────────────────────────────────────────────────────────────
  { value: 'person_titles',                   label: 'Job Title',               type: 'text'       },
  { value: 'person_locations',                label: 'Person Location',         type: 'text'       },
  { value: 'person_seniorities',              label: 'Seniority',               type: 'select'     },
  { value: 'person_departments',              label: 'Department',              type: 'deptselect' },
  { value: 'person_functions',                label: 'Job Function',            type: 'text'       },
  // ── Company ───────────────────────────────────────────────────────────────
  { value: 'organization_names',              label: 'Company Name',            type: 'text'       },
  { value: 'organization_locations',          label: 'Company HQ Location',     type: 'text'       },
  { value: 'organization_industries',         label: 'Company Industry',        type: 'text'       },
  { value: 'organization_num_employees_ranges', label: 'Employee Count Range',  type: 'empselect'  },
  { value: 'organization_keywords',           label: 'Company Keywords',        type: 'text'       },
  // ── Contact ───────────────────────────────────────────────────────────────
  { value: 'contact_email_status',            label: 'Email Status',            type: 'emailselect'},
  // ── Search ────────────────────────────────────────────────────────────────
  { value: 'q_keywords',                      label: 'Keywords (free text)',    type: 'text'       },
]

const DEPARTMENT_OPTS = [
  { value: 'sales',              label: 'Sales'              },
  { value: 'engineering',        label: 'Engineering'        },
  { value: 'marketing',          label: 'Marketing'          },
  { value: 'product_management', label: 'Product Management' },
  { value: 'finance',            label: 'Finance'            },
  { value: 'operations',         label: 'Operations'         },
  { value: 'human_resources',    label: 'Human Resources'    },
  { value: 'it',                 label: 'IT'                 },
  { value: 'legal',              label: 'Legal'              },
  { value: 'customer_success',   label: 'Customer Success'   },
  { value: 'design',             label: 'Design'             },
  { value: 'data_science',       label: 'Data Science'       },
  { value: 'business_development', label: 'Business Dev'     },
  { value: 'consulting',         label: 'Consulting'         },
]

const EMPLOYEE_RANGE_OPTS = [
  { value: '1,10',       label: '1 – 10'         },
  { value: '11,20',      label: '11 – 20'        },
  { value: '21,50',      label: '21 – 50'        },
  { value: '51,100',     label: '51 – 100'       },
  { value: '101,200',    label: '101 – 200'      },
  { value: '201,500',    label: '201 – 500'      },
  { value: '501,1000',   label: '501 – 1,000'    },
  { value: '1001,2000',  label: '1,001 – 2,000'  },
  { value: '2001,5000',  label: '2,001 – 5,000'  },
  { value: '5001,10000', label: '5,001 – 10,000' },
  { value: '10001,',     label: '10,001+'         },
]

const EMAIL_STATUS_OPTS = [
  { value: 'verified',     label: 'Verified'      },
  { value: 'unverified',   label: 'Unverified'    },
  { value: 'likely_to_engage', label: 'Likely to engage' },
]

// Apollo operators — same full set as BrightData
const APOLLO_TEXT_OPS = [
  { value: 'includes',    label: 'includes'          },
  { value: 'is',          label: '= equals'          },
  { value: 'is_not',      label: '≠ not equals'      },
  { value: 'starts_with', label: 'starts with'       },
  { value: 'ends_with',   label: 'ends with'         },
  { value: 'is_not_null', label: 'is not empty', noValue: true },
]
const APOLLO_SEL_OPS = [
  { value: 'is',          label: 'is'                },
  { value: 'is_not',      label: '≠ not equals'      },
  { value: 'is_not_null', label: 'is not empty', noValue: true },
]

const APOLLO_SELECT_TYPES = ['select', 'deptselect', 'empselect', 'emailselect']

function getAPOps(fieldValue) {
  const type = APOLLO_FIELDS.find(f => f.value === fieldValue)?.type
  return APOLLO_SELECT_TYPES.includes(type) ? APOLLO_SEL_OPS : APOLLO_TEXT_OPS
}

function newAPRule() {
  return { id: Math.random().toString(36).slice(2), field: 'person_titles', operator: 'includes', value: '' }
}

// Group rules into Apollo API params.
// Operator semantics:
//   includes / is / starts_with / ends_with → add value to the field array
//   is_not_null                             → field must be non-empty (send sentinel "")
//   is_not                                  → Apollo has no native NOT; skip from API call
function buildApolloFilters(rules) {
  const out = {}
  for (const r of rules) {
    const ops  = getAPOps(r.field)
    const opDef = ops.find(o => o.value === r.operator)

    if (opDef?.noValue) {
      // is_not_null — just ensure the field key exists with at least one entry
      if (r.field !== 'q_keywords') {
        if (!out[r.field]) out[r.field] = []
        // send empty string as wildcard — Apollo returns any non-null value
      }
      continue
    }

    if (!r.value.trim()) continue
    if (r.operator === 'is_not') continue   // no native NOT in Apollo — omit

    if (r.field === 'q_keywords') {
      out.q_keywords = r.value.trim()
    } else {
      if (!out[r.field]) out[r.field] = []
      out[r.field].push(r.value.trim())
    }
  }
  return out
}

const SENIORITY_OPTS = [
  { value: 'owner',    label: 'Owner'       },
  { value: 'founder',  label: 'Founder'     },
  { value: 'c_suite',  label: 'C-Suite'     },
  { value: 'partner',  label: 'Partner'     },
  { value: 'vp',       label: 'VP'          },
  { value: 'head',     label: 'Head'        },
  { value: 'director', label: 'Director'    },
  { value: 'manager',  label: 'Manager'     },
  { value: 'senior',   label: 'Senior'      },
  { value: 'entry',    label: 'Entry Level' },
  { value: 'intern',   label: 'Intern'      },
]

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

function getToken() { return localStorage.getItem('wb_ai_token') || '' }

// ─────────────────────────────────────────────────────────────────────────────
// Shared styles
// ─────────────────────────────────────────────────────────────────────────────

const card = { background: 'var(--bg-card)', border: '1px solid var(--border-1)', borderRadius: 12, padding: '20px 24px' }
const inputSt = { background: 'var(--bg-input)', border: '1px solid var(--border-1)', borderRadius: 8, padding: '7px 12px', fontSize: 13, color: 'var(--text-1)', outline: 'none', width: '100%' }
const selectSt = { ...inputSt, width: 'auto', cursor: 'pointer' }
const btnPrimary = { background: 'var(--accent)', color: '#fff', border: 'none', borderRadius: 8, padding: '8px 20px', fontSize: 13, fontWeight: 600, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6 }
const btnGhost = { background: 'transparent', border: '1px solid var(--border-1)', color: 'var(--text-2)', borderRadius: 8, padding: '7px 14px', fontSize: 12, fontWeight: 500, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 5 }

function Spinner({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
      style={{ animation: 'spin 0.8s linear infinite', flexShrink: 0 }}>
      <path d="M21 12a9 9 0 1 1-6.219-8.56" strokeLinecap="round" />
    </svg>
  )
}

function Avatar({ name = '', url = '', size = 34 }) {
  const initials = (name || '?').split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase()
  const colors = ['#6366f1', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#3b82f6']
  const bg = colors[(name.charCodeAt(0) || 0) % colors.length]
  if (url) return <img src={url} alt={name} style={{ width: size, height: size, borderRadius: '50%', objectFit: 'cover', flexShrink: 0 }} />
  return <div style={{ width: size, height: size, borderRadius: '50%', background: bg, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: size * 0.35, fontWeight: 700, color: '#fff', flexShrink: 0 }}>{initials}</div>
}

// ─────────────────────────────────────────────────────────────────────────────
// Source selector pill
// ─────────────────────────────────────────────────────────────────────────────

function SourceToggle({ source, setSource }) {
  const opts = [
    {
      id: 'brightdata', label: 'BrightData',
      badge: '620M+',
      color: '#0073b1',
      icon: (
        <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor">
          <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 0 1-2.063-2.065 2.064 2.064 0 1 1 2.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/>
        </svg>
      ),
    },
    {
      id: 'apollo', label: 'Apollo',
      badge: '275M+',
      color: '#7c3aed',
      icon: (
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="4"/><line x1="4.93" y1="4.93" x2="9.17" y2="9.17"/><line x1="14.83" y1="14.83" x2="19.07" y2="19.07"/><line x1="14.83" y1="9.17" x2="19.07" y2="4.93"/><line x1="4.93" y1="19.07" x2="9.17" y2="14.83"/>
        </svg>
      ),
    },
  ]
  return (
    <div style={{ display: 'flex', gap: 8 }}>
      {opts.map(o => {
        const active = source === o.id
        return (
          <button key={o.id} onClick={() => setSource(o.id)} style={{
            display: 'flex', alignItems: 'center', gap: 8,
            padding: '9px 18px', borderRadius: 10, cursor: 'pointer',
            border: active ? `1.5px solid ${o.color}` : '1.5px solid var(--border-1)',
            background: active ? `${o.color}15` : 'var(--bg-card)',
            color: active ? o.color : 'var(--text-3)',
            fontWeight: 600, fontSize: 13, transition: 'all 0.15s',
          }}>
            {o.icon}
            {o.label}
            <span style={{
              fontSize: 10, padding: '1px 6px', borderRadius: 4, fontWeight: 700,
              background: active ? `${o.color}25` : 'var(--bg-elevated)',
              color: active ? o.color : 'var(--text-3)',
              border: `1px solid ${active ? o.color + '40' : 'var(--border-1)'}`,
            }}>{o.badge}</span>
          </button>
        )
      })}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Country autocomplete
// ─────────────────────────────────────────────────────────────────────────────

const COUNTRIES = [
  { code: 'AF', name: 'Afghanistan' }, { code: 'AL', name: 'Albania' }, { code: 'DZ', name: 'Algeria' },
  { code: 'AR', name: 'Argentina' }, { code: 'AM', name: 'Armenia' }, { code: 'AU', name: 'Australia' },
  { code: 'AT', name: 'Austria' }, { code: 'AZ', name: 'Azerbaijan' }, { code: 'BH', name: 'Bahrain' },
  { code: 'BD', name: 'Bangladesh' }, { code: 'BY', name: 'Belarus' }, { code: 'BE', name: 'Belgium' },
  { code: 'BO', name: 'Bolivia' }, { code: 'BA', name: 'Bosnia and Herzegovina' }, { code: 'BR', name: 'Brazil' },
  { code: 'BG', name: 'Bulgaria' }, { code: 'KH', name: 'Cambodia' }, { code: 'CA', name: 'Canada' },
  { code: 'CL', name: 'Chile' }, { code: 'CN', name: 'China' }, { code: 'CO', name: 'Colombia' },
  { code: 'HR', name: 'Croatia' }, { code: 'CY', name: 'Cyprus' }, { code: 'CZ', name: 'Czech Republic' },
  { code: 'DK', name: 'Denmark' }, { code: 'EC', name: 'Ecuador' }, { code: 'EG', name: 'Egypt' },
  { code: 'EE', name: 'Estonia' }, { code: 'ET', name: 'Ethiopia' }, { code: 'FI', name: 'Finland' },
  { code: 'FR', name: 'France' }, { code: 'GE', name: 'Georgia' }, { code: 'DE', name: 'Germany' },
  { code: 'GH', name: 'Ghana' }, { code: 'GR', name: 'Greece' }, { code: 'GT', name: 'Guatemala' },
  { code: 'HK', name: 'Hong Kong' }, { code: 'HU', name: 'Hungary' }, { code: 'IS', name: 'Iceland' },
  { code: 'IN', name: 'India' }, { code: 'ID', name: 'Indonesia' }, { code: 'IE', name: 'Ireland' },
  { code: 'IL', name: 'Israel' }, { code: 'IT', name: 'Italy' }, { code: 'JM', name: 'Jamaica' },
  { code: 'JP', name: 'Japan' }, { code: 'JO', name: 'Jordan' }, { code: 'KZ', name: 'Kazakhstan' },
  { code: 'KE', name: 'Kenya' }, { code: 'KW', name: 'Kuwait' }, { code: 'LV', name: 'Latvia' },
  { code: 'LB', name: 'Lebanon' }, { code: 'LT', name: 'Lithuania' }, { code: 'LU', name: 'Luxembourg' },
  { code: 'MY', name: 'Malaysia' }, { code: 'MT', name: 'Malta' }, { code: 'MX', name: 'Mexico' },
  { code: 'MD', name: 'Moldova' }, { code: 'MA', name: 'Morocco' }, { code: 'MM', name: 'Myanmar' },
  { code: 'NP', name: 'Nepal' }, { code: 'NL', name: 'Netherlands' }, { code: 'NZ', name: 'New Zealand' },
  { code: 'NG', name: 'Nigeria' }, { code: 'MK', name: 'North Macedonia' }, { code: 'NO', name: 'Norway' },
  { code: 'OM', name: 'Oman' }, { code: 'PK', name: 'Pakistan' }, { code: 'PS', name: 'Palestine' },
  { code: 'PA', name: 'Panama' }, { code: 'PY', name: 'Paraguay' }, { code: 'PE', name: 'Peru' },
  { code: 'PH', name: 'Philippines' }, { code: 'PL', name: 'Poland' }, { code: 'PT', name: 'Portugal' },
  { code: 'QA', name: 'Qatar' }, { code: 'RO', name: 'Romania' }, { code: 'RU', name: 'Russia' },
  { code: 'SA', name: 'Saudi Arabia' }, { code: 'RS', name: 'Serbia' }, { code: 'SG', name: 'Singapore' },
  { code: 'SK', name: 'Slovakia' }, { code: 'SI', name: 'Slovenia' }, { code: 'ZA', name: 'South Africa' },
  { code: 'KR', name: 'South Korea' }, { code: 'ES', name: 'Spain' }, { code: 'LK', name: 'Sri Lanka' },
  { code: 'SE', name: 'Sweden' }, { code: 'CH', name: 'Switzerland' }, { code: 'TW', name: 'Taiwan' },
  { code: 'TZ', name: 'Tanzania' }, { code: 'TH', name: 'Thailand' }, { code: 'TN', name: 'Tunisia' },
  { code: 'TR', name: 'Turkey' }, { code: 'UG', name: 'Uganda' }, { code: 'UA', name: 'Ukraine' },
  { code: 'AE', name: 'United Arab Emirates' }, { code: 'GB', name: 'United Kingdom' },
  { code: 'US', name: 'United States' }, { code: 'UY', name: 'Uruguay' }, { code: 'UZ', name: 'Uzbekistan' },
  { code: 'VE', name: 'Venezuela' }, { code: 'VN', name: 'Vietnam' }, { code: 'ZM', name: 'Zambia' },
  { code: 'ZW', name: 'Zimbabwe' },
]

function CountryAutocomplete({ value, onChange, placeholder, style }) {
  const [open, setOpen]   = useState(false)
  const [query, setQuery] = useState(value || '')

  const matches = query.length === 0 ? [] : COUNTRIES.filter(c =>
    c.name.toLowerCase().startsWith(query.toLowerCase()) ||
    c.code.toLowerCase().startsWith(query.toLowerCase())
  ).slice(0, 8)

  const select = c => {
    setQuery(c.name)
    onChange(c.code)
    setOpen(false)
  }

  return (
    <div style={{ position: 'relative', flex: 1 }}>
      <input
        value={query}
        onChange={e => { setQuery(e.target.value); onChange(e.target.value); setOpen(true) }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        placeholder={placeholder}
        style={style}
      />
      {open && matches.length > 0 && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 999,
          background: 'var(--bg-card)', border: '1px solid var(--border-1)',
          borderRadius: 8, marginTop: 4, boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
          overflow: 'hidden',
        }}>
          {matches.map(c => (
            <div
              key={c.code}
              onMouseDown={() => select(c)}
              style={{
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '8px 12px', cursor: 'pointer', fontSize: 13,
                borderBottom: '1px solid var(--border-2)',
              }}
              onMouseEnter={e => e.currentTarget.style.background = 'var(--bg-elevated)'}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
            >
              <span style={{ fontWeight: 700, color: 'var(--text-1)', minWidth: 28, fontSize: 12, fontFamily: 'monospace' }}>{c.code}</span>
              <span style={{ color: 'var(--text-2)' }}>{c.name}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// BrightData filter builder
// ─────────────────────────────────────────────────────────────────────────────

function BDFilterBuilder({ rules, setRules, globalOp, setGlobalOp, limit, setLimit, timeout, setTimeout_, onSearch, loading }) {
  const addRule    = () => setRules(r => [...r, newBDRule()])
  const removeRule = id => setRules(r => r.length > 1 ? r.filter(x => x.id !== id) : r)
  const updateRule = (id, k, v) => setRules(r => r.map(x => x.id === id ? { ...x, [k]: v } : x))

  return (
    <>
      <div style={{ ...card, marginBottom: 16 }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 18 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>Advanced Filters</span>
            <span style={{ fontSize: 11, color: 'var(--text-3)' }}>{rules.length} rule{rules.length !== 1 ? 's' : ''}</span>
          </div>
          {rules.length > 1 && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ fontSize: 11, color: 'var(--text-3)' }}>Match</span>
              {['and', 'or'].map(op => (
                <button key={op} onClick={() => setGlobalOp(op)} style={{
                  padding: '3px 10px', borderRadius: 6, fontSize: 11, fontWeight: 700,
                  cursor: 'pointer', textTransform: 'uppercase',
                  background: globalOp === op ? 'var(--accent)' : 'var(--bg-elevated)',
                  color: globalOp === op ? '#fff' : 'var(--text-3)',
                  border: globalOp === op ? 'none' : '1px solid var(--border-1)',
                }}>{op}</button>
              ))}
              <span style={{ fontSize: 11, color: 'var(--text-3)' }}>rules</span>
            </div>
          )}
        </div>

        {/* Column headers */}
        <div style={{ display: 'grid', gridTemplateColumns: '200px 170px 1fr 32px', gap: 8, marginBottom: 6, padding: '0 2px' }}>
          {['Field', 'Operator', 'Value', ''].map(h => (
            <span key={h} style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{h}</span>
          ))}
        </div>

        {/* Rules */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {rules.map((rule, idx) => {
            const ops = getBDOps(rule.field)
            const activeOp = ops.find(o => o.value === rule.operator) || ops[0]
            return (
              <div key={rule.id}>
                {idx > 0 && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                    <div style={{ flex: 1, height: 1, background: 'var(--border-2)' }} />
                    <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>{globalOp}</span>
                    <div style={{ flex: 1, height: 1, background: 'var(--border-2)' }} />
                  </div>
                )}
                <div style={{ display: 'grid', gridTemplateColumns: '200px 170px 1fr 32px', gap: 8, alignItems: 'center' }}>
                  <select value={rule.field}
                    onChange={e => {
                      const f = e.target.value
                      const newOps = getBDOps(f)
                      updateRule(rule.id, 'field', f)
                      if (!newOps.find(o => o.value === rule.operator))
                        updateRule(rule.id, 'operator', newOps[0].value)
                    }}
                    style={{ ...selectSt, width: '100%' }}>
                    {BD_FIELDS.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
                  </select>

                  <select value={rule.operator}
                    onChange={e => updateRule(rule.id, 'operator', e.target.value)}
                    style={{ ...selectSt, width: '100%' }}>
                    {ops.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                  </select>

                  {activeOp?.noValue
                    ? <div style={{ height: 34, display: 'flex', alignItems: 'center', padding: '0 12px', background: 'var(--bg-elevated)', borderRadius: 8, border: '1px dashed var(--border-1)' }}>
                        <span style={{ fontSize: 12, color: 'var(--text-3)', fontStyle: 'italic' }}>no value needed</span>
                      </div>
                    : rule.field === 'country_code'
                      ? <CountryAutocomplete
                          value={rule.value}
                          onChange={v => updateRule(rule.id, 'value', v)}
                          placeholder="Type country name or code…"
                          style={inputSt}
                        />
                      : <input type={BD_FIELDS.find(f => f.value === rule.field)?.type === 'number' ? 'number' : 'text'}
                          value={rule.value}
                          onChange={e => updateRule(rule.id, 'value', e.target.value)}
                          placeholder={`Enter ${BD_FIELDS.find(f => f.value === rule.field)?.label || 'value'}…`}
                          style={inputSt}
                          onKeyDown={e => e.key === 'Enter' && rules.length === 1 && onSearch()}
                        />
                  }

                  <button onClick={() => removeRule(rule.id)} disabled={rules.length === 1}
                    style={{ width: 32, height: 32, borderRadius: 7, border: '1px solid var(--border-1)', background: 'transparent', color: 'var(--text-3)', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', opacity: rules.length === 1 ? 0.3 : 1 }}>
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                  </button>
                </div>
              </div>
            )
          })}
        </div>

        <button onClick={addRule} style={{ marginTop: 14, display: 'flex', alignItems: 'center', gap: 6, background: 'transparent', border: '1px dashed var(--border-1)', borderRadius: 8, padding: '7px 14px', fontSize: 12, fontWeight: 500, color: 'var(--accent)', cursor: 'pointer', width: '100%', justifyContent: 'center' }}>
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          Add rule
        </button>
      </div>

      {/* Options */}
      <div style={{ ...card, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 24, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-2)', whiteSpace: 'nowrap' }}>Max records</label>
          <input
            type="number" min={1} value={limit}
            onChange={e => setLimit(Math.max(1, parseInt(e.target.value) || 1))}
            style={{ ...selectSt, width: 80, textAlign: 'center' }}
          />
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-2)', whiteSpace: 'nowrap' }}>Snapshot timeout</label>
          <select value={timeout} onChange={e => setTimeout_(Number(e.target.value))} style={{ ...selectSt, width: 110 }}>
            {[{ v: 60, l: '1 min' }, { v: 120, l: '2 min' }, { v: 300, l: '5 min' }, { v: 600, l: '10 min' }].map(o => (
              <option key={o.v} value={o.v}>{o.l}</option>
            ))}
          </select>
        </div>
        <div style={{ flex: 1 }} />
        <button onClick={onSearch} disabled={loading} style={{ ...btnPrimary, opacity: loading ? 0.7 : 1 }}>
          {loading ? <><Spinner />Processing…</> : <><SearchIcon />Apply Filter</>}
        </button>
      </div>
    </>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Apollo filter builder
// ─────────────────────────────────────────────────────────────────────────────

function ApolloFilterBuilder({ rules, setRules, perPage, setPerPage, enrich, setEnrich, onSearch, loading }) {
  const addRule    = () => setRules(r => [...r, newAPRule()])
  const removeRule = id  => setRules(r => r.length > 1 ? r.filter(x => x.id !== id) : r)
  const updateRule = (id, k, v) => setRules(r => r.map(x => x.id === id ? { ...x, [k]: v } : x))

  return (
    <>
      <div style={{ ...card, marginBottom: 16 }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 18 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>Advanced Filters</span>
            <span style={{ fontSize: 11, color: 'var(--text-3)' }}>{rules.length} rule{rules.length !== 1 ? 's' : ''}</span>
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-3)', background: 'var(--bg-elevated)', padding: '3px 10px', borderRadius: 6, border: '1px solid var(--border-1)' }}>
            Same field = <span style={{ color: '#7c3aed', fontWeight: 700 }}>OR</span>
            &nbsp;·&nbsp;
            Different fields = <span style={{ color: 'var(--text-2)', fontWeight: 700 }}>AND</span>
          </div>
        </div>

        {/* Column headers */}
        <div style={{ display: 'grid', gridTemplateColumns: '220px 110px 1fr 32px', gap: 8, marginBottom: 6, padding: '0 2px' }}>
          {['Field', 'Operator', 'Value', ''].map(h => (
            <span key={h} style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{h}</span>
          ))}
        </div>

        {/* Rule rows */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {rules.map((rule, idx) => {
            const fieldDef  = APOLLO_FIELDS.find(f => f.value === rule.field) || APOLLO_FIELDS[0]
            const prevField = idx > 0 ? rules[idx - 1].field : null
            // Between rules: OR if same field, AND if different
            const sepLabel  = prevField === rule.field ? 'OR' : 'AND'
            const sepColor  = sepLabel === 'OR' ? '#7c3aed' : 'var(--text-3)'

            return (
              <div key={rule.id}>
                {idx > 0 && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                    <div style={{ flex: 1, height: 1, background: 'var(--border-2)' }} />
                    <span style={{ fontSize: 10, fontWeight: 700, color: sepColor, textTransform: 'uppercase', letterSpacing: '0.08em' }}>{sepLabel}</span>
                    <div style={{ flex: 1, height: 1, background: 'var(--border-2)' }} />
                  </div>
                )}
                <div style={{ display: 'grid', gridTemplateColumns: '220px 110px 1fr 32px', gap: 8, alignItems: 'center' }}>

                  {/* Field */}
                  <select value={rule.field}
                    onChange={e => {
                      const f = e.target.value
                      updateRule(rule.id, 'field', f)
                      updateRule(rule.id, 'operator', getAPOps(f)[0].value)
                      updateRule(rule.id, 'value', '')
                    }}
                    style={{ ...selectSt, width: '100%' }}>
                    {APOLLO_FIELDS.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
                  </select>

                  {/* Operator — selectable dropdown */}
                  <select value={rule.operator}
                    onChange={e => updateRule(rule.id, 'operator', e.target.value)}
                    style={{ ...selectSt, width: '100%' }}>
                    {getAPOps(rule.field).map(o => (
                      <option key={o.value} value={o.value}>{o.label}</option>
                    ))}
                  </select>

                  {/* Value — no value / select variants / text input */}
                  {(() => {
                    const noVal = getAPOps(rule.field).find(o => o.value === rule.operator)?.noValue
                    if (noVal) return (
                      <div style={{ height: 34, display: 'flex', alignItems: 'center', padding: '0 12px', background: 'var(--bg-elevated)', borderRadius: 8, border: '1px dashed var(--border-1)' }}>
                        <span style={{ fontSize: 12, color: 'var(--text-3)', fontStyle: 'italic' }}>no value needed</span>
                      </div>
                    )
                    const selOpts =
                      fieldDef.type === 'select'     ? { placeholder: 'Select seniority…',       opts: SENIORITY_OPTS      } :
                      fieldDef.type === 'deptselect'  ? { placeholder: 'Select department…',      opts: DEPARTMENT_OPTS     } :
                      fieldDef.type === 'empselect'   ? { placeholder: 'Select employee range…',  opts: EMPLOYEE_RANGE_OPTS } :
                      fieldDef.type === 'emailselect' ? { placeholder: 'Select email status…',    opts: EMAIL_STATUS_OPTS   } :
                      null
                    if (selOpts) return (
                      <select value={rule.value}
                        onChange={e => updateRule(rule.id, 'value', e.target.value)}
                        style={{ ...selectSt, width: '100%' }}>
                        <option value="">{selOpts.placeholder}</option>
                        {selOpts.opts.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                      </select>
                    )
                    return (
                      <input type="text" value={rule.value}
                        onChange={e => updateRule(rule.id, 'value', e.target.value)}
                        placeholder={`Enter ${fieldDef.label}…`}
                        style={inputSt}
                        onKeyDown={e => e.key === 'Enter' && rules.length === 1 && onSearch()}
                      />
                    )
                  })()}

                  {/* Remove */}
                  <button onClick={() => removeRule(rule.id)} disabled={rules.length === 1}
                    style={{ width: 32, height: 32, borderRadius: 7, border: '1px solid var(--border-1)', background: 'transparent', color: 'var(--text-3)', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', opacity: rules.length === 1 ? 0.3 : 1 }}>
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                  </button>
                </div>
              </div>
            )
          })}
        </div>

        {/* Add rule */}
        <button onClick={addRule} style={{
          marginTop: 14, display: 'flex', alignItems: 'center', gap: 6,
          background: 'transparent', border: '1px dashed rgba(124,58,237,0.4)',
          borderRadius: 8, padding: '7px 14px', fontSize: 12, fontWeight: 500,
          color: '#7c3aed', cursor: 'pointer', width: '100%', justifyContent: 'center',
        }}>
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          Add rule
        </button>
      </div>

      {/* Options row */}
      <div style={{ ...card, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 24, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-2)', whiteSpace: 'nowrap' }}>
            {enrich ? 'Enrich limit' : 'Results per page'}
          </label>
          {enrich
            ? <input
                type="number" min={1} max={100} value={perPage}
                onChange={e => setPerPage(Math.max(1, Math.min(100, Number(e.target.value) || 1)))}
                placeholder="1–100"
                style={{ ...inputSt, width: 80 }}
              />
            : <select value={perPage} onChange={e => setPerPage(Number(e.target.value))} style={{ ...selectSt, width: 80 }}>
                {[10, 20, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
              </select>
          }
          {enrich && (
            <span style={{ fontSize: 11, color: 'var(--text-3)' }}>people (1–100)</span>
          )}
        </div>

        {/* Free / Credits toggle */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-2)', whiteSpace: 'nowrap' }}>Mode</label>
          <div style={{ display: 'flex', background: 'var(--bg-elevated)', borderRadius: 8, border: '1px solid var(--border-1)', overflow: 'hidden' }}>
            {[
              { val: false, label: 'Free',    hint: 'No credits — limited data' },
              { val: true,  label: 'Credits', hint: 'Full name, email & phone' },
            ].map(opt => (
              <button
                key={String(opt.val)}
                title={opt.hint}
                onClick={() => setEnrich(opt.val)}
                style={{
                  padding: '5px 14px', border: 'none', cursor: 'pointer', fontSize: 12, fontWeight: 600,
                  background: enrich === opt.val ? '#7c3aed' : 'transparent',
                  color:      enrich === opt.val ? '#fff'    : 'var(--text-3)',
                  transition: 'all 0.15s',
                }}
              >
                {opt.label}
              </button>
            ))}
          </div>
          {enrich && (
            <span style={{ fontSize: 11, color: '#f59e0b', fontWeight: 600 }}>
              ⚠ Uses credits
            </span>
          )}
        </div>

        <div style={{ flex: 1 }} />
        <button onClick={onSearch} disabled={loading} style={{ ...btnPrimary, background: '#7c3aed', opacity: loading ? 0.7 : 1 }}>
          {loading ? <><Spinner />Searching…</> : <><SearchIcon />Search Apollo</>}
        </button>
      </div>
    </>
  )
}

function SearchIcon() {
  return <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
}

// ─────────────────────────────────────────────────────────────────────────────
// Stat card row
// ─────────────────────────────────────────────────────────────────────────────

function StatCards({ stats }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: `repeat(${stats.length}, 1fr)`, gap: 12, marginBottom: 16 }}>
      {stats.map(s => (
        <div key={s.label} style={{ ...card, padding: '14px 18px' }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>{s.label}</div>
          <div style={{ fontSize: 24, fontWeight: 700, color: s.color }}>{s.value?.toLocaleString() ?? '—'}</div>
        </div>
      ))}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Profile table (shared for BD + Apollo)
// ─────────────────────────────────────────────────────────────────────────────

function ProfileTable({ profiles, accentColor = 'var(--accent)' }) {
  const dash = <span style={{ color: 'var(--text-3)', fontWeight: 400 }}>—</span>
  const tv   = v => (v !== null && v !== undefined && v !== '' && v !== 0) ? v : dash

  const HEADERS = ['Name', 'Title', 'Company', 'LinkedIn URL', 'Location', 'Email', 'Phone', 'Industry', 'Emp.', 'Followers', 'Skills', 'Education', 'Saved']
  const COLS    = '180px 200px 160px 240px 130px 200px 140px 130px 80px 90px 180px 160px 100px'

  return (
    <div style={{ width: '100%', overflowX: 'auto' }}>

      {/* Header */}
      <div style={{ display: 'grid', gridTemplateColumns: COLS, background: 'var(--bg-elevated)', borderBottom: '2px solid var(--border-1)', borderRadius: '6px 6px 0 0' }}>
        {HEADERS.map((h, i) => (
          <div key={h} style={{
            fontSize: 10, fontWeight: 700, color: 'var(--text-3)',
            textTransform: 'uppercase', letterSpacing: '0.06em',
            padding: '8px 10px',
            borderRight: i < HEADERS.length - 1 ? '1px solid var(--border-2)' : 'none',
            whiteSpace: 'nowrap',
          }}>{h}</div>
        ))}
      </div>

      {/* Rows */}
      {profiles.map((p, i) => {
        const name       = p.name || `${p.first_name || ''} ${p.last_name || ''}`.trim() || 'Unknown'
        const loc        = [p.city, p.country].filter(Boolean).join(', ')
        const isApollo   = p.status === 'apollo_search' || p.status === 'apollo_enriched'
        const isEnriched = p.status === 'apollo_enriched'

        let skills = []
        try { skills = JSON.parse(p.top_skills || '[]') } catch {}
        let edu = []
        try { edu = JSON.parse(p.education || '[]') } catch {}

        const C = ({ children, idx, wrap }) => (
          <div style={{
            padding: '9px 10px', fontSize: 12, color: 'var(--text-2)',
            borderRight: idx < 12 ? '1px solid var(--border-2)' : 'none',
            overflow: 'hidden',
            ...(wrap ? {} : { whiteSpace: 'nowrap', textOverflow: 'ellipsis' }),
          }}>
            {children}
          </div>
        )

        return (
          <div key={p.id || i} style={{
            display: 'grid', gridTemplateColumns: COLS,
            borderBottom: '1px solid var(--border-2)',
            background: i % 2 !== 0 ? 'rgba(255,255,255,0.018)' : 'transparent',
            alignItems: 'center',
          }}>

            {/* Name */}
            <C idx={0}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
                <Avatar name={name} url={p.avatar_url} size={26} />
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-1)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={name}>{name}</div>
                  <span style={{
                    fontSize: 9, fontWeight: 700, padding: '1px 5px', borderRadius: 3,
                    background: isApollo ? 'rgba(124,58,237,0.12)' : 'rgba(0,115,177,0.12)',
                    color: isApollo ? '#7c3aed' : '#0073b1',
                  }}>{isEnriched ? 'Apollo+' : isApollo ? 'Apollo' : 'BrightData'}</span>
                </div>
              </div>
            </C>

            {/* Title */}
            <C idx={1}><span title={p.title} style={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{tv(p.title)}</span></C>

            {/* Company */}
            <C idx={2}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 5, overflow: 'hidden' }}>
                {p.company_logo && <img src={p.company_logo} alt="" style={{ width: 14, height: 14, borderRadius: 3, flexShrink: 0 }} />}
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={p.company}>{tv(p.company)}</span>
              </div>
            </C>

            {/* LinkedIn URL */}
            <C idx={3}>
              {p.linkedin_url
                ? <a href={p.linkedin_url} target="_blank" rel="noreferrer"
                    title={p.linkedin_url}
                    style={{ fontSize: 11, color: isApollo ? '#7c3aed' : '#0073b1', textDecoration: 'none', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'block' }}>
                    {p.linkedin_url.replace('https://', '')}
                  </a>
                : dash
              }
            </C>

            {/* Location */}
            <C idx={4}>{tv(loc)}</C>

            {/* Email */}
            <C idx={5}><span title={p.work_email} style={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{tv(p.work_email)}</span></C>

            {/* Phone */}
            <C idx={6}>{tv(p.direct_phone)}</C>

            {/* Industry */}
            <C idx={7}><span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'block' }}>{tv(p.industry)}</span></C>

            {/* Employees */}
            <C idx={8}>{p.employee_count > 0 ? p.employee_count.toLocaleString() : dash}</C>

            {/* Followers */}
            <C idx={9}>{p.linkedin_followers > 0 ? p.linkedin_followers.toLocaleString() : dash}</C>

            {/* Skills */}
            <C idx={10} wrap>
              {skills.length > 0
                ? <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
                    {skills.slice(0, 4).map((s, j) => (
                      <span key={j} style={{ fontSize: 9, padding: '1px 5px', borderRadius: 4, background: 'var(--bg-elevated)', color: accentColor, border: `1px solid ${accentColor}25`, whiteSpace: 'nowrap' }}>{s}</span>
                    ))}
                  </div>
                : dash
              }
            </C>

            {/* Education */}
            <C idx={11} wrap>
              {edu.length > 0
                ? edu.slice(0, 2).map((e, j) => (
                    <div key={j} style={{ fontSize: 11, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{e.school || '—'}</div>
                  ))
                : dash
              }
            </C>

            {/* Saved */}
            <C idx={12}>
              <span style={{ fontSize: 11, color: 'var(--text-3)' }}>
                {p.created_at ? new Date(p.created_at).toLocaleDateString() : dash}
              </span>
            </C>

          </div>
        )
      })}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Main page
// ─────────────────────────────────────────────────────────────────────────────

export default function BrightDataFilterPage() {
  const [source, setSource]   = useState('brightdata')   // 'brightdata' | 'apollo'
  const [tab, setTab]         = useState('search')        // 'search' | 'records'

  // ── Credits state ──────────────────────────────────────────────────────────
  const [credits, setCredits]         = useState(null)
  const [creditsLoading, setCreditsLoading] = useState(false)

  // ── BrightData state ───────────────────────────────────────────────────────
  const [bdRules, setBdRules]       = useState([newBDRule()])
  const [bdGlobalOp, setBdGlobalOp] = useState('and')
  const [bdLimit, setBdLimit]       = useState(20)
  const [bdTimeout, setBdTimeout]   = useState(1800)
  const [bdLoading, setBdLoading]   = useState(false)
  const [bdResult, setBdResult]     = useState(null)
  const [bdJobStatus, setBdJobStatus] = useState(null)   // live job status while polling

  // ── Apollo state ───────────────────────────────────────────────────────────
  const [apRules, setApRules]       = useState([newAPRule()])
  const [apPerPage, setApPerPage]   = useState(20)
  const [apEnrich, setApEnrich]     = useState(false)  // false = free, true = credits
  const [apLoading, setApLoading]   = useState(false)
  const [apResult, setApResult]     = useState(null)
  const [apPage, setApPage]         = useState(1)
  const [apError, setApError]       = useState(null)   // structured error (e.g. plan limitation)

  // ── Records state (shared) ─────────────────────────────────────────────────
  const [records, setRecords]       = useState([])
  const [recTotal, setRecTotal]     = useState(0)
  const [recPage, setRecPage]       = useState(1)
  const [recLoading, setRecLoading] = useState(false)

  // ── BrightData search ──────────────────────────────────────────────────────
  const handleBDSearch = async () => {
    const filter = buildBDFilter(bdRules, bdGlobalOp)
    if (!filter) return toast.error('Fill at least one filter value')
    setBdLoading(true); setBdResult(null); setBdJobStatus(null)
    try {
      // Step 1 — trigger snapshot (returns immediately)
      const r = await fetch(`${BACKEND}/bd-filter/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: getToken(), filters: filter, limit: bdLimit, timeout: bdTimeout }),
      })
      const init = await r.json()
      if (!r.ok) throw new Error(init.detail || 'Search failed')

      const snapshotId = init.snapshot_id
      setBdJobStatus({ status: 'processing', snapshot_id: snapshotId })

      // Step 2 — poll /status/:snapshot_id until done or failed
      const deadline = Date.now() + bdTimeout * 1000
      while (Date.now() < deadline) {
        await new Promise(res => setTimeout(res, 5000))
        const sr   = await fetch(`${BACKEND}/bd-filter/status/${snapshotId}`)
        const job  = await sr.json()
        setBdJobStatus(job)

        if (job.status === 'done') {
          setBdResult(job)
          toast.success(`Saved ${job.saved} profile${job.saved !== 1 ? 's' : ''} from BrightData`)
          return
        }
        if (job.status === 'failed') {
          throw new Error(job.error || 'Snapshot failed')
        }
      }
      throw new Error(`Snapshot not ready after ${bdTimeout}s — check backend logs`)
    } catch (e) {
      toast.error(e.message || 'BrightData search failed')
    } finally { setBdLoading(false) }
  }

  // ── Apollo search ──────────────────────────────────────────────────────────
  const handleApolloSearch = async (page = 1) => {
    const grouped = buildApolloFilters(apRules)
    if (!Object.keys(grouped).length) return toast.error('Fill at least one filter value')
    const body = { token: getToken(), per_page: apPerPage, page, enrich: apEnrich, ...grouped }
    setApLoading(true); setApResult(null); setApError(null); setApPage(page)
    try {
      const r = await fetch(`${BACKEND}/bd-filter/apollo/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await r.json()
      if (!r.ok) {
        const msg = data.detail || 'Apollo search failed'
        const isPlanError = msg.includes('API_INACCESSIBLE') || msg.includes('not accessible') || msg.includes('APOLLO_MASTER_KEY_REQUIRED')
        if (isPlanError) {
          setApError('plan')
        } else {
          toast.error(msg)
        }
        return
      }
      setApResult(data)
      if (data.enrich_mode && data.enrich_failed > 0) {
        toast.error(`Enrichment failed for ${data.enrich_failed} of ${data.saved + data.enrich_failed} people — check backend logs`)
      } else {
        const label = data.enrich_mode ? 'enriched' : 'saved'
        toast.success(`${label.charAt(0).toUpperCase() + label.slice(1)} ${data.saved} profile${data.saved !== 1 ? 's' : ''} from Apollo`)
      }
    } catch (e) { toast.error(e.message || 'Apollo search failed') }
    finally { setApLoading(false) }
  }

  // ── Records ────────────────────────────────────────────────────────────────
  const loadRecords = useCallback(async (page = 1) => {
    setRecLoading(true)
    const prefix = source === 'apollo' ? 'apollo/' : ''
    try {
      const r = await fetch(`${BACKEND}/bd-filter/${prefix}records?token=${getToken()}&page=${page}&page_size=50`)
      const data = await r.json()
      setRecords(data.records || [])
      setRecTotal(data.total || 0)
      setRecPage(page)
    } catch { toast.error('Failed to load records') }
    finally { setRecLoading(false) }
  }, [source])

  useEffect(() => { if (tab === 'records') loadRecords(1) }, [tab, source, loadRecords])

  // ── Credits: fetch once on mount ───────────────────────────────────────────
  useEffect(() => {
    setCreditsLoading(true)
    fetch(`${BACKEND}/bd-filter/credits`)
      .then(r => r.json())
      .then(setCredits)
      .catch(() => {})
      .finally(() => setCreditsLoading(false))
  }, [])

  const isBD     = source === 'brightdata'
  const accent   = isBD ? '#0073b1' : '#7c3aed'
  const loading  = isBD ? bdLoading : apLoading
  const result   = isBD ? bdResult  : apResult

  // ─────────────────────────────────────────────────────────────────────────
  return (
    <div style={{ padding: '24px 28px', maxWidth: 1100, margin: '0 auto', fontFamily: "'Inter', system-ui, sans-serif" }}>

      {/* ── Page header ─────────────────────────────────────────────── */}
      <div style={{ marginBottom: 22, display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
        <div>
          <h1 style={{ fontSize: 18, fontWeight: 700, color: 'var(--text-1)', marginBottom: 4 }}>People Search</h1>
          <p style={{ fontSize: 13, color: 'var(--text-3)', margin: 0 }}>Search & import people from BrightData or Apollo into your database</p>
        </div>
        {/* Tabs */}
        <div style={{ display: 'flex', gap: 4, background: 'var(--bg-elevated)', borderRadius: 9, padding: 4, border: '1px solid var(--border-1)' }}>
          {[{ id: 'search', label: 'Filter & Search' }, { id: 'records', label: 'Saved Records' }].map(t => (
            <button key={t.id} onClick={() => setTab(t.id)} style={{
              padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, border: 'none', cursor: 'pointer',
              background: tab === t.id ? 'var(--bg-card)' : 'transparent',
              color: tab === t.id ? 'var(--text-1)' : 'var(--text-3)',
              boxShadow: tab === t.id ? '0 1px 3px rgba(0,0,0,0.08)' : 'none',
            }}>{t.label}</button>
          ))}
        </div>
      </div>

      {/* ── Credits banner ───────────────────────────────────────────── */}
      {(creditsLoading || credits) && (
        <div style={{ display: 'flex', gap: 10, marginBottom: 18, flexWrap: 'wrap' }}>
          {/* BrightData */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', borderRadius: 8, padding: '7px 14px', fontSize: 12 }}>
            <span style={{ fontWeight: 700, color: '#0073b1' }}>BrightData</span>
            {creditsLoading
              ? <span style={{ color: 'var(--text-3)' }}>loading…</span>
              : (() => {
                  const bd = credits?.brightdata
                  if (!bd) return null
                  if (typeof bd.balance === 'number') return (
                    <>
                      <span style={{ color: 'var(--text-3)' }}>Balance:</span>
                      <span style={{ fontWeight: 700, color: 'var(--text-1)' }}>${bd.balance.toFixed(2)}</span>
                      {typeof bd.pending_balance === 'number' && (
                        <span style={{ color: 'var(--text-3)', fontSize: 11 }}>(${bd.pending_balance.toFixed(2)} pending)</span>
                      )}
                    </>
                  )
                  if (bd.status === 'active' || bd.note)
                    return <span style={{ color: '#10b981', fontSize: 11 }}>● Datasets key active</span>
                  if (bd.error)
                    return <span style={{ color: '#ef4444', fontSize: 11 }}>⚠ {bd.error}</span>
                  return null
                })()
            }
          </div>

          {/* Apollo — keys are stringified JSON arrays e.g. '["api/v1/people","match"]' */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)', borderRadius: 8, padding: '7px 14px', fontSize: 12 }}>
            <span style={{ fontWeight: 700, color: '#7c3aed' }}>Apollo</span>
            {creditsLoading
              ? <span style={{ color: 'var(--text-3)' }}>loading…</span>
              : credits?.apollo?.error
                ? <span style={{ color: '#ef4444', fontSize: 11 }}>⚠ {credits.apollo.error}</span>
                : (() => {
                    const ap = credits?.apollo || {}
                    const findDay = (path, action) => {
                      const k = Object.keys(ap).find(k => k.includes(`"${path}"`) && k.includes(`"${action}"`))
                      return k ? ap[k]?.day : null
                    }
                    const search = findDay('mixed_people', 'api_search')
                    const enrich = findDay('people', 'match')
                    if (!search && !enrich) return <span style={{ color: '#10b981', fontSize: 11 }}>● Connected</span>
                    return (
                      <div style={{ display: 'flex', gap: 14 }}>
                        {search && (
                          <span title="People Search — free, no credits consumed">
                            <span style={{ color: 'var(--text-3)' }}>Search: </span>
                            <span style={{ fontWeight: 700, color: search.left_over === 0 ? '#ef4444' : 'var(--text-1)' }}>{search.left_over}</span>
                            <span style={{ color: 'var(--text-3)' }}>/{search.limit}</span>
                            <span style={{ color: 'var(--text-3)', fontSize: 11 }}> /day</span>
                          </span>
                        )}
                        {enrich && (
                          <span title="People Enrich (credits mode) — consumes credits">
                            <span style={{ color: 'var(--text-3)' }}>Enrich: </span>
                            <span style={{ fontWeight: 700, color: enrich.left_over === 0 ? '#ef4444' : 'var(--text-1)' }}>{enrich.left_over}</span>
                            <span style={{ color: 'var(--text-3)' }}>/{enrich.limit}</span>
                            <span style={{ color: 'var(--text-3)', fontSize: 11 }}> /day</span>
                          </span>
                        )}
                      </div>
                    )
                  })()
            }
          </div>

          <button
            onClick={() => {
              setCreditsLoading(true)
              fetch(`${BACKEND}/bd-filter/credits`)
                .then(r => r.json()).then(setCredits).catch(() => {})
                .finally(() => setCreditsLoading(false))
            }}
            style={{ background: 'none', border: '1px solid var(--border-1)', borderRadius: 8, padding: '7px 12px', fontSize: 11, color: 'var(--text-3)', cursor: 'pointer' }}
          >↺ Refresh</button>
        </div>
      )}

      {/* ── Source selector ──────────────────────────────────────────── */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 10 }}>Data Source</div>
        <SourceToggle source={source} setSource={s => { setSource(s); setBdResult(null); setApResult(null); setApError(null); setApRules([newAPRule()]) }} />
      </div>

      {/* ══ SEARCH TAB ══ */}
      {tab === 'search' && (
        <>
          {isBD
            ? <BDFilterBuilder
                rules={bdRules} setRules={setBdRules}
                globalOp={bdGlobalOp} setGlobalOp={setBdGlobalOp}
                limit={bdLimit} setLimit={setBdLimit}
                timeout={bdTimeout} setTimeout_={setBdTimeout}
                onSearch={handleBDSearch} loading={bdLoading}
              />
            : <ApolloFilterBuilder
                rules={apRules} setRules={setApRules}
                perPage={apPerPage} setPerPage={setApPerPage}
                enrich={apEnrich} setEnrich={setApEnrich}
                onSearch={() => handleApolloSearch(1)} loading={apLoading}
              />
          }

          {/* Loading banner */}
          {loading && (
            <div style={{ ...card, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, padding: 40, marginBottom: 16 }}>
              <Spinner size={30} />
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-1)', marginBottom: 5 }}>
                  {isBD
                    ? (() => {
                        const s = bdJobStatus?.status
                        if (s === 'building')     return `Building snapshot… (BrightData: ${bdJobStatus?.bd_status || 'scheduled'})`
                        if (s === 'downloading')  return 'Snapshot ready — downloading records…'
                        if (s === 'saving')       return 'Saving profiles to database…'
                        return 'Triggering BrightData snapshot…'
                      })()
                    : 'Searching Apollo database…'
                  }
                </div>
                <div style={{ fontSize: 12, color: 'var(--text-3)' }}>
                  {isBD ? 'BrightData can take 5–20 min for large filters. Page will update automatically.' : 'Apollo usually responds in a few seconds.'}
                </div>
                {isBD && bdJobStatus?.snapshot_id && (
                  <code style={{ marginTop: 8, display: 'inline-block', fontSize: 11, color: 'var(--text-3)', background: 'var(--bg-elevated)', padding: '3px 10px', borderRadius: 6, border: '1px solid var(--border-1)' }}>
                    {bdJobStatus.snapshot_id}
                  </code>
                )}
              </div>
            </div>
          )}

          {/* Apollo plan-limitation error card */}
          {!isBD && apError === 'plan' && !apLoading && (
            <div style={{ ...card, marginBottom: 16, border: '1px solid rgba(239,68,68,0.3)', background: 'rgba(239,68,68,0.06)' }}>
              <div style={{ display: 'flex', gap: 14, alignItems: 'flex-start' }}>
                <div style={{ width: 36, height: 36, borderRadius: 9, background: 'rgba(239,68,68,0.12)', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
                </div>
                <div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-1)', marginBottom: 6 }}>Apollo Master API Key required</div>
                  <div style={{ fontSize: 13, color: 'var(--text-2)', lineHeight: 1.6, marginBottom: 12 }}>
                    Apollo's People Search endpoint requires a <strong>Master API Key</strong> — a regular key returns 403 regardless of plan. This endpoint does not consume credits.
                    <br /><br />
                    To fix: <strong>Apollo → Settings → Integrations → API → Create Master API Key</strong>, then update <code style={{ fontSize: 12, background: 'var(--bg-elevated)', padding: '1px 6px', borderRadius: 4 }}>APOLLO_API_KEY</code> in <code style={{ fontSize: 12, background: 'var(--bg-elevated)', padding: '1px 6px', borderRadius: 4 }}>backend/.env</code>.
                  </div>
                  <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                    <a href="https://app.apollo.io/#/settings/integrations/api" target="_blank" rel="noreferrer"
                      style={{ fontSize: 12, fontWeight: 600, padding: '6px 14px', borderRadius: 7, background: '#7c3aed', color: '#fff', textDecoration: 'none' }}>
                      Apollo API Settings →
                    </a>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Results */}
          {result && !loading && (
            <>
              {/* BrightData stats */}
              {isBD && (
                <>
                  <StatCards stats={[
                    { label: 'BD Returned',  value: result.total_returned, color: '#0073b1' },
                    { label: 'Saved to DB',  value: result.saved,          color: '#10b981' },
                    { label: 'Skipped',      value: result.skipped,        color: '#f59e0b' },
                    { label: 'Failed',       value: result.failed,         color: '#ef4444' },
                  ]} />
                  {result.snapshot_id && (
                    <div style={{ marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ fontSize: 11, color: 'var(--text-3)' }}>Snapshot:</span>
                      <code style={{ fontSize: 11, background: 'var(--bg-elevated)', padding: '3px 10px', borderRadius: 6, color: 'var(--text-2)', border: '1px solid var(--border-1)' }}>{result.snapshot_id}</code>
                    </div>
                  )}
                </>
              )}

              {/* Apollo stats */}
              {!isBD && (
                <>
                  <StatCards stats={
                    result.enrich_mode
                      ? [
                          { label: 'Total Matches',    value: result.total_entries,  color: '#7c3aed' },
                          { label: 'Enriched (Full)',  value: result.enrich_ok,      color: '#10b981' },
                          { label: 'Enrich Failed',    value: result.enrich_failed,  color: '#ef4444' },
                          { label: 'Saved to DB',      value: result.saved,          color: '#6366f1' },
                        ]
                      : [
                          { label: 'Total Matches',  value: result.total_entries,   color: '#7c3aed' },
                          { label: 'This Page',      value: result.results?.length, color: '#6366f1' },
                          { label: 'Saved to DB',    value: result.saved,           color: '#10b981' },
                          { label: 'Failed',         value: result.failed,          color: '#ef4444' },
                        ]
                  } />
                  {/* Enrich failure detail */}
                  {result.enrich_mode && result.enrich_failed > 0 && result.enrich_failures?.length > 0 && (
                    <div style={{ ...card, marginBottom: 12, background: 'rgba(239,68,68,0.06)', border: '1px solid rgba(239,68,68,0.2)' }}>
                      <div style={{ fontSize: 12, fontWeight: 700, color: '#ef4444', marginBottom: 8 }}>
                        Enrichment failures ({result.enrich_failed})
                      </div>
                      {result.enrich_failures.map((f, i) => (
                        <div key={i} style={{ fontSize: 11, color: 'var(--text-3)', marginBottom: 2, fontFamily: 'monospace' }}>{f}</div>
                      ))}
                    </div>
                  )}
                  {/* Apollo pagination */}
                  {result.total_entries > apPerPage && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                      <button onClick={() => handleApolloSearch(apPage - 1)} disabled={apPage <= 1} style={btnGhost}>← Prev</button>
                      <span style={{ fontSize: 12, color: 'var(--text-3)', padding: '6px 10px' }}>Page {apPage} · {result.total_entries?.toLocaleString()} total</span>
                      <button onClick={() => handleApolloSearch(apPage + 1)} disabled={result.results?.length < apPerPage} style={btnGhost}>Next →</button>
                    </div>
                  )}
                </>
              )}

              {result.results?.length > 0
                ? <div style={card}>
                    <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', marginBottom: 14 }}>
                      {result.results.length} profile{result.results.length !== 1 ? 's' : ''} saved
                    </div>
                    <ProfileTable profiles={result.results} accentColor={accent} />
                  </div>
                : <div style={{ ...card, textAlign: 'center', padding: 40, color: 'var(--text-3)', fontSize: 13 }}>
                    No profiles saved. Try different filters or check API key configuration.
                  </div>
              }
            </>
          )}
        </>
      )}

      {/* ══ RECORDS TAB ══ */}
      {tab === 'records' && (
        <div style={card}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>
                {isBD ? 'BrightData' : 'Apollo'} Records
              </span>
              {recTotal > 0 && <span style={{ fontSize: 12, color: 'var(--text-3)' }}>{recTotal.toLocaleString()} total</span>}
            </div>
            <button onClick={() => loadRecords(recPage)} disabled={recLoading} style={btnGhost}>
              {recLoading ? <Spinner size={12} /> : <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-.18-5.19"/></svg>}
              Refresh
            </button>
          </div>

          {recLoading && <div style={{ textAlign: 'center', padding: 48 }}><Spinner size={28} /></div>}

          {!recLoading && records.length === 0 && (
            <div style={{ textAlign: 'center', padding: 48, color: 'var(--text-3)', fontSize: 13 }}>
              No records yet. Run a {isBD ? 'BrightData' : 'Apollo'} search to populate this list.
            </div>
          )}

          {!recLoading && records.length > 0 && (
            <>
              <ProfileTable profiles={records} accentColor={accent} />
              {recTotal > 50 && (
                <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginTop: 20 }}>
                  <button onClick={() => loadRecords(recPage - 1)} disabled={recPage <= 1} style={btnGhost}>← Prev</button>
                  <span style={{ fontSize: 12, color: 'var(--text-3)', padding: '7px 10px' }}>Page {recPage}</span>
                  <button onClick={() => loadRecords(recPage + 1)} disabled={records.length < 50} style={btnGhost}>Next →</button>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  )
}
