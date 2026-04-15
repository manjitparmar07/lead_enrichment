// ApiDocPage.jsx — Lead Enrichment API Reference

import { useState, useEffect } from 'react'
import { Copy, Check, Eye, EyeOff, Key, Loader } from 'lucide-react'

const API_DOC_BASE = import.meta.env.VITE_BACKEND_URL || ''

function docAuthHeaders() {
  const token = localStorage.getItem('wb_ai_token') || ''
  return { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }
}

const BASE = import.meta.env.VITE_BACKEND_URL || 'https://api-lead-enrichment-worksbuddy.lbmdemo.com'
const TOKEN   = '<YOUR_TOKEN>'

const PROFILE_EXAMPLE = `{
  "linkedin_url": "https://www.linkedin.com/in/username",
  "first_name": "John",
  "last_name": "Doe",
  "headline": "VP of Engineering at Acme Corp",
  "company": "Acme Corp",
  "location": "San Francisco, CA"
}`

export default function ApiDocPage() {
  const [activeGroup,    setActiveGroup]    = useState('token')
  const [copied,         setCopied]         = useState(null)
  const [customFeatures, setCustomFeatures] = useState([])
  const [cfLoading,      setCfLoading]      = useState(false)

  useEffect(() => {
    if (activeGroup !== 'custom') return
    if (customFeatures.length > 0) return
    setCfLoading(true)
    fetch(`${API_DOC_BASE}/api/v1/features`, { headers: docAuthHeaders() })
      .then(r => r.json())
      .then(d => setCustomFeatures(d.features || []))
      .catch(() => {})
      .finally(() => setCfLoading(false))
  }, [activeGroup]) // eslint-disable-line

  const copy = (id, text) => {
    navigator.clipboard.writeText(text)
    setCopied(id)
    setTimeout(() => setCopied(null), 1800)
  }

  const GROUPS = [
    { id: 'token',    label: 'Get Token',        icon: '🔑' },
    { id: 'bulk',     label: 'Bulk Enrich',       icon: '📦' },
    { id: 'divider1', divider: true, label: 'Enrichment Views' },
    { id: 'email',    label: 'Email Enrich',      icon: '📧' },
    { id: 'outreach', label: 'Outreach Enrich',   icon: '✉️'  },
    { id: 'company',  label: 'Company Enrich',    icon: '🏢' },
    { id: 'divider2', divider: true, label: 'AI Endpoints' },
    { id: 'core',     label: 'Core',              icon: '🔷' },
    { id: 'intel',    label: 'Intelligence',      icon: '🧠' },
    { id: 'output',   label: 'Output',            icon: '📤' },
    { id: 'divider3', divider: true, label: 'System Prompt' },
    { id: 'sysprompt', label: 'System Prompt',    icon: '🗂️' },
    { id: 'divider4', divider: true, label: 'Custom' },
    { id: 'custom',   label: 'Custom Features',   icon: '⚡' },
    { id: 'divider5', divider: true, label: 'People Search' },
    { id: 'bdfilter', label: 'BrightData Filter',  icon: '🔵' },
    { id: 'apollo',   label: 'Apollo Search',      icon: '🟣' },
  ]

  const aiNote = 'No auth required. Pass the raw BrightData profile object in the body.'

  const ENDPOINTS = {
    bulk: [
      {
        id: 'enrich-bulk', method: 'POST', path: '/api/leads/enrich/bulk', title: 'Bulk Enrich',
        desc: 'Submit up to 5000 LinkedIn profile URLs for async enrichment. Pass your JWT token in the body — organization_id and sso_id are decoded from it automatically. Returns a job_id to track progress. Optional: pass system_prompt to override the default AI system prompt for every lead in this batch (used during scoring, outreach, and intelligence generation).',
        curl:
`curl -X POST ${BASE}/api/leads/enrich/bulk \\
  -H "Content-Type: application/json" \\
  -d '{
    "token": "${TOKEN}",
    "linkedin_urls": [
      "https://www.linkedin.com/in/person-one",
      "https://www.linkedin.com/in/person-two",
      "https://www.linkedin.com/in/person-three"
    ],
    "system_prompt": "You are a B2B sales AI for Acme Inc. Focus on SaaS buyers...",
    "skip_existing": true,
    "forward_to_lio": false
  }'`,
        response:
`{
  "success": true,
  "job": {
    "id": "a1b2c3d4-e5f6-...",
    "status": "running",
    "total_urls": 3
  },
  "message": "Batch job started for 3 URLs. Track progress at GET /api/leads/jobs/a1b2c3d4"
}`,
      },
      {
        id: 'job-status', method: 'GET', path: '/api/leads/jobs/{job_id}', title: 'Poll Job Status',
        desc: 'Poll the status of a bulk enrichment job. Each completed lead in the Ably real-time event carries a leadenrich_id — use that ID to call the Email, Outreach, and Company view endpoints. No auth required.',
        curl:
`curl ${BASE}/api/leads/jobs/JOB_ID`,
        response:
`{
  "id": "job-uuid",
  "status": "running",
  "total_urls": 20,
  "processed": 8,
  "failed": 0,
  "sub_jobs": [
    { "chunk_index": 0, "status": "completed", "processed": 5, "failed": 0 },
    { "chunk_index": 1, "status": "running",   "processed": 3, "failed": 0 },
    { "chunk_index": 2, "status": "pending",   "processed": 0, "failed": 0 }
  ]
}

// Each Ably real-time event (channel: wb-leads-{org_id}) carries:
// {
//   "event": "lead_done",
//   "leadenrich_id": "abc123def456",   ← use this for view APIs
//   "linkedin_url": "https://www.linkedin.com/in/johndoe",
//   "name": "John Doe",
//   "score_tier": "hot"
// }`,
      },
      {
        id: 'job-stop', method: 'POST', path: '/api/leads/jobs/{job_id}/stop', title: 'Stop / Cancel Job',
        desc: 'Cancels a running or pending enrichment job. Marks the job and all pending sub-jobs as cancelled in the database, and calls the BrightData snapshot cancel API to immediately stop any in-progress scrape. Any webhook results that arrive after cancellation are automatically discarded. Cancellable statuses: running, pending, fallback.',
        curl:
`curl -X POST ${BASE}/api/leads/jobs/JOB_ID/stop`,
        response:
`{
  "job_id": "a1b2c3d4-e5f6-...",
  "status": "cancelled",
  "snapshots_cancelled": 4
}

// snapshots_cancelled = N → number of BrightData chunk snapshots stopped
// snapshots_cancelled = 0 → job had no BrightData snapshots (queue/fallback path)`,
      },
      {
        id: 'job-rerun', method: 'POST', path: '/api/leads/jobs/{job_id}/rerun', title: 'Rerun Job',
        desc: 'Reruns the BrightData snapshot for a failed or cancelled enrichment job. Calls the BrightData rerun API using the existing snapshot_id, updates the job with the new snapshot_id returned by BrightData, resets status to running, and clears all sub-job progress counters. Rerunnable statuses: failed, cancelled, completed. Returns 400 if job has no snapshot_id (was processed via queue/fallback, not BrightData batch).',
        curl:
`curl -X POST ${BASE}/api/leads/jobs/JOB_ID/rerun`,
        response:
`{
  "job_id": "a1b2c3d4-e5f6-...",
  "status": "running",
  "old_snapshot_id": "s_abc123",
  "new_snapshot_id": "s_xyz789"
}

// old_snapshot_id → the previous BrightData snapshot
// new_snapshot_id → the fresh snapshot BrightData created for the rerun`,
      },
    ],

    // ── Enrichment View APIs ──────────────────────────────────────────────────

    email: [
      {
        id: 'email-enrich', method: 'POST', path: '/api/leads/view/email', title: 'Email Enrichment (Single)',
        desc: 'Find and verify a work email for a single lead. Pass leadenrich_id from bulk job results. Flow: Apollo.io person match → ValidEmail.net verification → pattern guesses (first.last@, flast@, first@) → ValidEmail.net. If email is already stored it is returned immediately (no API call).',
        curl:
`curl -X POST ${BASE}/api/leads/view/email \\
  -H "Content-Type: application/json" \\
  -d '{
    "leadenrich_id": "abc123def456"
  }'`,
        response:
`{
  "lead_id": "abc123def456",
  "name": "John Doe",
  "company": "Acme Corp",

  "work_email": "john.doe@acme.com",
  "email": "john.doe@acme.com",
  "source": "apollo",          // "apollo" | "guessed" | "not_found"
  "confidence": "high",        // "high" | "low" | null
  "verified": true,
  "bounce_risk": "low",        // "low" | "medium" | "high"
  "enrichment_source": "apollo",

  "phone": "+1-415-555-0100",

  "all_emails": ["john.doe@acme.com"],
  "guessed_emails": [],        // populated when Apollo misses, holds all 3 pattern guesses
  "activity_emails": [],
  "activity_phones": [],
  "ai_generated": null
}`,
      },
      {
        id: 'email-bulk', method: 'POST', path: '/api/leads/email/bulk', title: 'Bulk Email Enrichment',
        desc: 'Find emails for multiple leads in one request. Pass a list of leadenrich_ids. Runs the same Apollo → ValidEmail.net → pattern guess flow as the single endpoint, with up to 10 leads processed in parallel. Leads that already have an email are returned immediately (no API call) unless force_refresh is true. Returns a single JSON response once all leads are done. Max 500 lead_ids per request.',
        curl:
`curl -X POST ${BASE}/api/leads/email/bulk \\
  -H "Content-Type: application/json" \\
  -d '{
    "lead_ids": [
      "abc123def456",
      "xyz789ghi012",
      "lmn345opq678"
    ],
    "force_refresh": false
  }'`,
        response:
`{
  "total": 3,
  "found": 2,
  "cached": 1,
  "skipped": 0,
  "results": [
    {
      "lead_id": "abc123def456",
      "name": "John Doe",
      "company": "Acme Corp",
      "work_email": "john.doe@acme.com",
      "email": "john.doe@acme.com",
      "source": "apollo",
      "confidence": "high",
      "verified": true,
      "bounce_risk": "low",
      "phone": "+1-415-555-0100",
      "message": "Email found: john.doe@acme.com | Source: apollo | Verified: Yes | Bounce risk: low"
    },
    {
      "lead_id": "xyz789ghi012",
      "name": "Sara Jones",
      "company": "Globex",
      "work_email": "sara.jones@globex.com",
      "source": "guessed",
      "confidence": null,
      "verified": false,
      "bounce_risk": "high",
      "message": "Email already found: sara.jones@globex.com"  // cached
    },
    {
      "lead_id": "lmn345opq678",
      "name": "Mike Tan",
      "company": "Initech",
      "work_email": null,
      "source": "not_found",
      "message": "No email found for Mike Tan at Initech."
    }
  ]
}`,
      },
      {
        id: 'email-bulk-stream', method: 'POST', path: '/api/leads/email/bulk/stream', title: 'Bulk Email Stream (SSE)',
        desc: 'Same as Bulk Email Enrichment but streams each result as a Server-Sent Event the moment it finishes — no waiting for all leads to complete. Results arrive in completion order (fastest first), not input order. The final event has done: true with summary counts. Max 500 lead_ids per request.',
        curl:
`# -N disables buffering so each event prints as it arrives
curl -N -X POST ${BASE}/api/leads/email/bulk/stream \\
  -H "Content-Type: application/json" \\
  -d '{
    "lead_ids": ["abc123def456", "xyz789ghi012", "lmn345opq678"],
    "force_refresh": false
  }'`,
        response:
`// Each line is a separate SSE event — arrives as each lead finishes:

data: {"lead_id":"xyz789ghi012","name":"Sara Jones","work_email":"sara.jones@globex.com","source":"apollo","verified":true,"bounce_risk":"low","message":"Email found: sara.jones@globex.com | Source: apollo | Verified: Yes | Bounce risk: low"}

data: {"lead_id":"lmn345opq678","name":"Mike Tan","work_email":null,"source":"not_found","message":"No email found for Mike Tan at Initech."}

data: {"lead_id":"abc123def456","name":"John Doe","work_email":"john.doe@acme.com","source":"guessed","verified":false,"bounce_risk":"high","message":"Email found: john.doe@acme.com | Source: guessed | Verified: No | Bounce risk: high"}

data: {"done":true,"total":3,"found":2,"cached":0,"skipped":1}

// ── JavaScript client example ─────────────────────────────────────
const res = await fetch('/api/leads/email/bulk/stream', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ lead_ids: ['abc123', 'xyz789'] }),
})
const reader = res.body.getReader()
const decoder = new TextDecoder()
let buffer = ''
while (true) {
  const { done, value } = await reader.read()
  if (done) break
  buffer += decoder.decode(value, { stream: true })
  const lines = buffer.split('\\n')
  buffer = lines.pop()
  for (const line of lines) {
    if (!line.startsWith('data: ')) continue
    const data = JSON.parse(line.slice(6))
    if (data.done) console.log('All done:', data)
    else console.log('Lead result:', data.lead_id, data.work_email)
  }
}`,
      },
    ],

    outreach: [
      {
        id: 'outreach-enrich', method: 'POST', path: '/api/leads/view/outreach', title: 'Outreach Enrichment',
        desc: 'Returns AI-generated outreach assets — personalised cold email, LinkedIn note, multi-step follow-up sequence, best send time, and pitch hooks. Pass leadenrich_id from bulk job results. Optional: pass system_prompt to generate fresh outreach copy live using your product context — returned in ai_generated.outreach alongside the stored data.',
        curl:
`curl -X POST ${BASE}/api/leads/view/outreach \\
  -H "Content-Type: application/json" \\
  -d '{
    "leadenrich_id": "abc123def456",
    "system_prompt": "You are a B2B sales AI for Acme Inc..."
  }'`,
        response:
`{
  "lead_id": "abc123def456",
  "name": "John Doe",
  "title": "VP of Engineering",
  "company": "Acme Corp",
  "score_tier": "hot",

  "cold_email": {
    "subject": "Quick question about your engineering stack at Acme",
    "body": "Hi John,\\n\\nI noticed you recently posted about platform engineering challenges..."
  },

  "linkedin_note": "Hey John, love what you're building at Acme — happy to connect?",

  "sequence": {
    "day_1": "Send cold email",
    "day_3": "LinkedIn connection request with note",
    "day_7": "Follow-up email referencing their recent post",
    "day_14": "Final value-add email with case study"
  },

  "best_time": "Tuesday 10–11am",
  "warm_signal": "Posted about platform engineering 3 days ago",
  "best_channel": "email",
  "outreach_angle": "Platform scaling pain",

  // Only present when system_prompt is provided:
  "ai_generated": {
    "outreach": "{\"cold_email\": {\"subject\": \"...\", \"body\": \"...\"}, \"linkedin_note\": \"...\", \"best_channel\": \"email\", \"outreach_angle\": \"...\"}"
  }
}`,
      },
    ],

    company: [
      {
        id: 'company-enrich', method: 'POST', path: '/api/leads/view/company', title: 'Company Enrichment',
        desc: 'Returns full company data — identity, profile, website intelligence, market signals, intent signals, scores, and tags. Pass leadenrich_id from bulk job results. Optional: pass system_prompt to get an AI-generated account analysis (fit summary, pain points, buying signals, ICP score) in ai_generated.company_analysis.',
        curl:
`curl -X POST ${BASE}/api/leads/view/company \\
  -H "Content-Type: application/json" \\
  -d '{
    "leadenrich_id": "abc123def456",
    "system_prompt": "You are a B2B sales AI for Acme Inc..."
  }'`,
        response:
`{
  "lead_id": "abc123def456",
  "company_id": "co_xyz789",

  "identity": {
    "name": "Acme Corp",
    "domain": "acme.com",
    "website": "https://www.acme.com",
    "linkedin_url": "https://www.linkedin.com/company/acme-corp"
  },

  "profile": {
    "industry": "Developer Tools / SaaS",
    "employee_count": "201-500",
    "revenue": "$10M-$50M ARR",
    "tech_stack": ["React", "Kubernetes", "Postgres", "AWS"]
  },

  "website_intelligence": {
    "homepage_summary": "Acme builds developer infrastructure for modern teams...",
    "key_features": ["CI/CD automation", "Zero-downtime deploys"],
    "pricing_model": "Per-seat SaaS"
  },

  "market_signals": {
    "news_mentions": [{ "title": "Acme raises $20M Series B", "date": "2025-01-15" }],
    "crunchbase": { "stage": "Series B", "total_funding": "$32M" },
    "hiring_signals": ["Hiring 12 engineers", "Opening Berlin office"]
  },

  "intent_signals": {
    "funding_event": true,
    "hiring_surge": true
  },

  "scores": {
    "company_score": 74,
    "company_score_tier": "warm",
    "combined_score": 78
  },

  "tags": {
    "company_tags": ["series-b", "dev-tools", "scaling", "hiring"],
    "account_pitch": { "hook": "Series B momentum + active hiring = ideal timing" }
  },

  // Only present when system_prompt is provided:
  "ai_generated": {
    "company_analysis": "{\"fit_summary\": \"Strong ICP match — Series B SaaS scaling fast\", \"key_pain_points\": [\"Infrastructure scaling\", \"Deployment reliability\"], \"buying_signals\": [\"Active hiring\", \"Recent funding\"], \"recommended_approach\": \"Lead with ROI on deployment failures\", \"icp_score\": 82}"
  }
}`,
      },
    ],

    // ── AI Endpoints ─────────────────────────────────────────────────────────

    core: [
      {
        id: 'identity', method: 'POST', path: '/api/v1/ai/identity', title: 'Identity',
        desc: `Extract identity information (name, title, location, seniority, department) from a profile. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/identity \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "full_name": "John Doe",
  "title": "VP of Engineering",
  "seniority": "VP",
  "department": "Engineering",
  "location": "San Francisco, CA",
  "token_usage": { "prompt_tokens": 320, "completion_tokens": 80, "total_tokens": 400 }
}`,
      },
      {
        id: 'contact', method: 'POST', path: '/api/v1/ai/contact', title: 'Contact',
        desc: `Extract contact details (work email, phone, LinkedIn URL) from a profile. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/contact \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "work_email": "john.doe@acme.com",
  "phone": "+1-415-555-0100",
  "linkedin_url": "https://www.linkedin.com/in/username",
  "token_usage": { "prompt_tokens": 290, "completion_tokens": 60, "total_tokens": 350 }
}`,
      },
      {
        id: 'scores', method: 'POST', path: '/api/v1/ai/scores', title: 'Scores',
        desc: `Calculate lead scores (total_score, score_tier, data_completeness) for a profile. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/scores \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "total_score": 78,
  "score_tier": "warm",
  "data_completeness": 88,
  "score_breakdown": { "identity": 20, "contact": 18, "company": 15, "activity": 12, "intent": 13 },
  "token_usage": { "prompt_tokens": 310, "completion_tokens": 90, "total_tokens": 400 }
}`,
      },
    ],

    intel: [
      {
        id: 'icp-match', method: 'POST', path: '/api/v1/ai/icp-match', title: 'ICP Match',
        desc: `Analyse how well a profile matches your Ideal Customer Profile. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/icp-match \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "icp_match_score": 82,
  "icp_tier": "strong",
  "match_reasons": ["VP-level seniority in Engineering", "Company size 200-500 matches ICP"],
  "disqualifiers": [],
  "token_usage": { "prompt_tokens": 350, "completion_tokens": 110, "total_tokens": 460 }
}`,
      },
      {
        id: 'behavioural-signals', method: 'POST', path: '/api/v1/ai/behavioural-signals', title: 'Behavioural Signals',
        desc: `Analyse behavioural signals and buying intent from a profile's activity. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/behavioural-signals \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "intent_level": "high",
  "buying_signals": ["Recently posted about evaluating new tools", "Hiring for expansion roles"],
  "engagement_score": 74,
  "token_usage": { "prompt_tokens": 380, "completion_tokens": 120, "total_tokens": 500 }
}`,
      },
      {
        id: 'pitch-intelligence', method: 'POST', path: '/api/v1/ai/pitch-intelligence', title: 'Pitch Intelligence',
        desc: `Generate pitch angles and value propositions tailored to the prospect. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/pitch-intelligence \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "primary_pitch": "Help John's team ship faster with automated QA pipelines",
  "value_props": ["Reduce deployment failures by 60%", "Cut QA cycle time in half"],
  "pain_points": ["Scaling engineering team", "Tech debt pressure"],
  "token_usage": { "prompt_tokens": 400, "completion_tokens": 140, "total_tokens": 540 }
}`,
      },
      {
        id: 'activity', method: 'POST', path: '/api/v1/ai/activity', title: 'Activity',
        desc: `Analyse recent activity — posts, comments, shares — to surface conversation starters. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/activity \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "recent_topics": ["AI in DevOps", "Remote team scaling"],
  "activity_level": "moderate",
  "conversation_starters": ["Saw your post on platform engineering — are you building internal tooling?"],
  "token_usage": { "prompt_tokens": 370, "completion_tokens": 130, "total_tokens": 500 }
}`,
      },
    ],

    output: [
      {
        id: 'tags', method: 'POST', path: '/api/v1/ai/tags', title: 'Tags',
        desc: `Auto-generate categorisation tags for a lead based on their profile. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/tags \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "tags": ["engineering-leader", "vp-level", "saas", "series-b", "platform-engineering", "hiring"],
  "token_usage": { "prompt_tokens": 280, "completion_tokens": 70, "total_tokens": 350 }
}`,
      },
      {
        id: 'outreach-ai', method: 'POST', path: '/api/v1/ai/outreach', title: 'Outreach',
        desc: `Generate personalised cold email and LinkedIn outreach copy for a prospect. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/outreach \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "email_subject": "Quick question about your engineering stack at Acme",
  "cold_email": "Hi John, I noticed you recently...",
  "linkedin_note": "Hey John, love what you're building at Acme...",
  "best_channel": "email",
  "best_send_time": "Tuesday 10–11am",
  "token_usage": { "prompt_tokens": 420, "completion_tokens": 180, "total_tokens": 600 }
}`,
      },
      {
        id: 'persona-analysis', method: 'POST', path: '/api/v1/ai/persona-analysis', title: 'Persona Analysis',
        desc: `Build a deep buyer persona — communication style, decision traits, motivators, objections. ${aiNote}`,
        curl:
`curl -X POST ${BASE}/api/v1/ai/persona-analysis \\
  -H "Content-Type: application/json" \\
  -d '{ "profile": ${PROFILE_EXAMPLE} }'`,
        response:
`{
  "persona_type": "Analytical Builder",
  "communication_style": "Direct, data-driven, prefers async",
  "decision_style": "Research-heavy, involves team",
  "motivators": ["Technical excellence", "Team autonomy"],
  "likely_objections": ["Budget approval process", "Integration complexity"],
  "token_usage": { "prompt_tokens": 430, "completion_tokens": 170, "total_tokens": 600 }
}`,
      },
    ],

    // ── System Prompt Endpoints ───────────────────────────────────────────────

    sysprompt: [
      {
        id: 'sp-generate', method: 'GET', path: '/api/v1/system-prompt/generate', title: 'Generate System Prompt',
        desc: 'Returns the fully assembled system prompt for your organisation. Built from three layers: (1) hardcoded platform baseline, (2) your workspace ICP config (product, tone, banned phrases, etc.), (3) any active dynamic prompt sections saved for your org. Use the returned system_prompt string directly in your AI calls. No auth required.',
        curl:
`curl "${BASE}/api/v1/system-prompt/generate?org_id=YOUR_ORG_ID"`,
        response:
`{
  "system_prompt": "You are an AI-powered lead intelligence assistant...\\n\\n---\\n\\n## Tenant Configuration\\n- Product: Acme Sales Intelligence\\n- Tone: peer\\n- Target titles: VP Sales, Head of Growth\\n- Banned phrases: synergy, game-changer\\n\\n---\\n\\nOnly reference verified data from enriched lead profiles.",

  "layers": {
    "default": "You are an AI-powered lead intelligence assistant...",
    "workspace": "## Tenant Configuration\\n- Product: Acme Sales Intelligence...",
    "dynamic": [
      {
        "id": "d1e2f3a4-...",
        "name": "Data Accuracy Rule",
        "key": "restrictions",
        "content": "Only reference verified data from enriched lead profiles.",
        "priority": 50
      }
    ]
  }
}`,
      },
      {
        id: 'sp-default', method: 'GET', path: '/api/v1/system-prompt/default', title: 'Get Default Prompt',
        desc: 'Returns the hardcoded platform baseline system prompt. This is always the first layer in every generated prompt — you cannot override it, but you can extend it with dynamic sections. No auth required.',
        curl:
`curl "${BASE}/api/v1/system-prompt/default"`,
        response:
`{
  "system_prompt": "You are an AI-powered lead intelligence assistant for the WorksBuddy Lead Enrichment platform.\\n\\nYour role is to help sales and marketing teams understand, score, and engage with their leads effectively.\\n\\n## Core Capabilities\\n...",
  "description": "Hardcoded platform baseline — always included in the generated prompt."
}`,
      },
      {
        id: 'sp-list', method: 'GET', path: '/api/v1/system-prompt/prompts', title: 'List Dynamic Sections',
        desc: 'List all dynamic prompt sections saved for your organisation. Each section is injected into the generated system prompt in priority order (lower number = earlier). No auth required.',
        curl:
`curl "${BASE}/api/v1/system-prompt/prompts?org_id=YOUR_ORG_ID"`,
        response:
`{
  "org_id": "org_abc123",
  "count": 2,
  "prompts": [
    {
      "id": "d1e2f3a4-...",
      "org_id": "org_abc123",
      "name": "Data Accuracy Rule",
      "key": "restrictions",
      "content": "Only reference verified data from enriched lead profiles.",
      "is_active": true,
      "priority": 50,
      "created_at": "2025-03-30T10:00:00+00:00",
      "updated_at": "2025-03-30T10:00:00+00:00"
    },
    {
      "id": "e5f6a7b8-...",
      "org_id": "org_abc123",
      "name": "Outreach Style",
      "key": "tone",
      "content": "Always open with a genuine observation about the lead's recent activity before pitching.",
      "is_active": true,
      "priority": 100,
      "created_at": "2025-03-30T11:00:00+00:00",
      "updated_at": "2025-03-30T11:00:00+00:00"
    }
  ]
}`,
      },
      {
        id: 'sp-create', method: 'POST', path: '/api/v1/system-prompt/prompts', title: 'Add Dynamic Section',
        desc: 'Add a new dynamic prompt section. It will be appended to the generated prompt according to its priority. Use key as a stable identifier for your section (e.g. "tone", "restrictions", "context"). No auth required.',
        curl:
`curl -X POST "${BASE}/api/v1/system-prompt/prompts?org_id=YOUR_ORG_ID" \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "Data Accuracy Rule",
    "key": "restrictions",
    "content": "Only reference verified data from enriched lead profiles. Never fabricate contact details.",
    "is_active": true,
    "priority": 50
  }'`,
        response:
`{
  "id": "d1e2f3a4-5678-...",
  "org_id": "org_abc123",
  "name": "Data Accuracy Rule",
  "key": "restrictions",
  "content": "Only reference verified data from enriched lead profiles. Never fabricate contact details.",
  "is_active": true,
  "priority": 50,
  "created_at": "2025-03-30T10:00:00+00:00",
  "updated_at": "2025-03-30T10:00:00+00:00"
}`,
      },
      {
        id: 'sp-update', method: 'PUT', path: '/api/v1/system-prompt/prompts/{prompt_id}', title: 'Update Dynamic Section',
        desc: 'Update any field of an existing dynamic prompt section. Only the fields you send will be updated. No auth required.',
        curl:
`curl -X PUT "${BASE}/api/v1/system-prompt/prompts/d1e2f3a4-5678-...?org_id=YOUR_ORG_ID" \\
  -H "Content-Type: application/json" \\
  -d '{
    "content": "Only use verified enrichment data. Highlight confidence levels when uncertain.",
    "priority": 40
  }'`,
        response:
`{
  "id": "d1e2f3a4-5678-...",
  "org_id": "org_abc123",
  "name": "Data Accuracy Rule",
  "key": "restrictions",
  "content": "Only use verified enrichment data. Highlight confidence levels when uncertain.",
  "is_active": true,
  "priority": 40,
  "created_at": "2025-03-30T10:00:00+00:00",
  "updated_at": "2025-03-30T12:30:00+00:00"
}`,
      },
      {
        id: 'sp-delete', method: 'DELETE', path: '/api/v1/system-prompt/prompts/{prompt_id}', title: 'Delete Dynamic Section',
        desc: 'Permanently removes a dynamic prompt section. The next call to /generate will no longer include it. No auth required.',
        curl:
`curl -X DELETE "${BASE}/api/v1/system-prompt/prompts/d1e2f3a4-5678-...?org_id=YOUR_ORG_ID"`,
        response:
`{
  "success": true,
  "deleted_id": "d1e2f3a4-5678-..."
}`,
      },
    ],

    // ── BrightData Filter ─────────────────────────────────────────────────────

    bdfilter: [
      {
        id: 'bd-search', method: 'POST', path: '/api/bd-filter/search', title: 'BrightData People Search',
        desc: 'Trigger a BrightData LinkedIn People dataset filter (620M+ profiles). Returns immediately with a snapshot_id — the poll→download→save pipeline runs in the background. Poll GET /api/bd-filter/status/{snapshot_id} to track progress. Auth: JWT token in body.',
        curl:
`# Simple filter — returns immediately
curl -X POST ${BASE}/api/bd-filter/search \\
  -H "Content-Type: application/json" \\
  -d '{
    "token": "${TOKEN}",
    "filters": {
      "name": "position",
      "operator": "includes",
      "value": "CTO"
    },
    "limit": 20,
    "timeout": 1800
  }'

# Compound AND/OR filter
curl -X POST ${BASE}/api/bd-filter/search \\
  -H "Content-Type: application/json" \\
  -d '{
    "token": "${TOKEN}",
    "filters": {
      "operator": "and",
      "filters": [
        {
          "operator": "or",
          "filters": [
            { "name": "position", "operator": "includes", "value": "CTO" },
            { "name": "position", "operator": "includes", "value": "Chief Technology Officer" }
          ]
        },
        { "name": "city",     "operator": "includes", "value": "London" },
        { "name": "followers", "operator": ">",        "value": "1000"  }
      ]
    },
    "limit": 50,
    "timeout": 1800
  }'`,
        response:
`// Immediate response — snapshot triggered, job running in background
{
  "snapshot_id": "snap_abc123xyz",
  "dataset_id":  "gd_l1viktl72bvl7bjuj0",
  "status":      "processing"
}

// Poll GET /api/bd-filter/status/{snapshot_id} until status == "done"`,
      },
      {
        id: 'bd-status', method: 'GET', path: '/api/bd-filter/status/{snapshot_id}', title: 'BrightData Snapshot Status',
        desc: 'Poll the status of a BrightData snapshot job. Call every 5–10 seconds after triggering a search. status progresses: processing → building → downloading → saving → done (or failed). Results are in the response when status == "done".',
        curl:
`curl "${BASE}/api/bd-filter/status/snap_abc123xyz"`,
        response:
`// While building:
{
  "snapshot_id": "snap_abc123xyz",
  "status":      "building",
  "bd_status":   "running",
  "triggered_at": "2026-04-15T10:00:00+00:00"
}

// When complete:
{
  "snapshot_id":    "snap_abc123xyz",
  "dataset_id":     "gd_l1viktl72bvl7bjuj0",
  "status":         "done",
  "total_returned": 20,
  "saved":          20,
  "skipped":        0,
  "failed":         0,
  "completed_at":   "2026-04-15T10:08:42+00:00",
  "results": [
    {
      "id":           "a1b2c3d4e5f6",
      "linkedin_url": "https://www.linkedin.com/in/johndoe",
      "name":         "John Doe",
      "title":        "CTO",
      "company":      "Acme Corp",
      "city":         "London",
      "country":      "GB",
      "status":       "bd_filter"
    }
  ]
}

// status values:
// processing  — snapshot triggered, waiting for BrightData
// building    — BrightData building snapshot (bd_status = scheduled|running)
// downloading — snapshot ready, fetching records
// saving      — records being upserted to DB
// done        — complete, results populated
// failed      — error field contains reason`,
      },
      {
        id: 'bd-credits', method: 'GET', path: '/api/bd-filter/credits', title: 'API Credits & Usage',
        desc: 'Fetch live account balance from BrightData and per-endpoint rate limit usage from Apollo. No auth required. BrightData balance requires an account-level API key (datasets key returns key status instead). Apollo returns day/hour/minute limits and consumed counts for every endpoint.',
        curl:
`curl "${BASE}/api/bd-filter/credits"`,
        response:
`{
  "brightdata": {
    // If account-level key: balance in USD
    "balance":         12.50,
    "pending_balance":  0.00,

    // If datasets key (no account scope):
    "note":    "Balance requires an account-level API key. Datasets key is active.",
    "status":  "active",
    "customer": "hl_b95863b4",
    "can_make_requests": false
  },
  "apollo": {
    // Keys are stringified JSON arrays: ["api/path", "action"]
    "[\"api/v1/mixed_people\", \"api_search\"]": {
      "day":    { "limit": 600, "consumed": 5,  "left_over": 595 },
      "hour":   { "limit": 200, "consumed": 0,  "left_over": 200 },
      "minute": { "limit": 50,  "consumed": 0,  "left_over": 50  }
    },
    "[\"api/v1/people\", \"match\"]": {
      "day":    { "limit": 600, "consumed": 2,  "left_over": 598 },
      "hour":   { "limit": 200, "consumed": 0,  "left_over": 200 },
      "minute": { "limit": 50,  "consumed": 0,  "left_over": 50  }
    }
    // ... all other Apollo endpoints
  }
}`,
      },
      {
        id: 'bd-records', method: 'GET', path: '/api/bd-filter/records', title: 'BrightData Saved Records',
        desc: 'List profiles saved via BrightData filter search. Token is optional — omit to retrieve records across all organisations.',
        curl:
`# With token (org-scoped)
curl "${BASE}/api/bd-filter/records?token=${TOKEN}&page=1&page_size=50"

# Without token (all orgs)
curl "${BASE}/api/bd-filter/records?page=1&page_size=50"`,
        response:
`{
  "total":     142,
  "page":      1,
  "page_size": 50,
  "records": [
    {
      "id":               "a1b2c3d4e5f6",
      "linkedin_url":     "https://www.linkedin.com/in/johndoe",
      "name":             "John Doe",
      "title":            "CTO",
      "company":          "Acme Corp",
      "city":             "London",
      "country":          "GB",
      "linkedin_followers": 3200,
      "top_skills":       "[\"Python\",\"AWS\",\"Kubernetes\"]",
      "education":        "[{\"school\":\"MIT\",\"degree\":\"MSc CS\"}]",
      "status":           "bd_filter",
      "created_at":       "2026-04-15T10:30:00+00:00"
    }
  ]
}

// Filterable fields (pass in filters object):
// position, name, first_name, last_name
// city, country_code, location
// current_company_name, current_company_industry, current_company_employee_count
// about, skills, industry, language
// school, degree_name, field_of_study, certification
// followers (number), connections (number), url

// Operators:
// Text fields: includes | = | != | starts_with | ends_with | is_not_null
// Number fields: > | < | = | != | is_not_null`,
      },
    ],

    // ── Apollo Search ─────────────────────────────────────────────────────────

    apollo: [
      {
        id: 'ap-search', method: 'POST', path: '/api/bd-filter/apollo/search', title: 'Apollo People Search (Free)',
        desc: 'Search Apollo\'s 275M+ person database using the master API key — no credits consumed. Returns obfuscated last names and no email/phone. Results saved to enriched_leads with status "apollo_search". Requires APOLLO_API_KEY_MASTER configured on the server.',
        curl:
`curl -X POST ${BASE}/api/bd-filter/apollo/search \\
  -H "Content-Type: application/json" \\
  -d '{
    "token": "${TOKEN}",
    "person_titles":            ["CTO", "Chief Technology Officer"],
    "person_locations":         ["London, GB", "Manchester, GB"],
    "organization_names":       ["Stripe", "Shopify"],
    "organization_locations":   ["San Francisco, US"],
    "organization_industries":  ["Software"],
    "organization_num_employees_ranges": ["201,500", "501,1000"],
    "person_seniorities":       ["c_suite", "vp"],
    "person_departments":       ["engineering"],
    "contact_email_status":     ["verified"],
    "q_keywords":               "machine learning saas",
    "per_page": 20,
    "page":     1,
    "enrich":   false
  }'`,
        response:
`{
  "total_entries": 8420,
  "page":          1,
  "per_page":      20,
  "enrich_mode":   false,
  "saved":         20,
  "skipped":       0,
  "failed":        0,
  "results": [
    {
      "id":          "b2c3d4e5f6a7",
      "linkedin_url":"https://app.apollo.io/#/people/6241d3ba65027a0001e88b3e",
      "name":        "John D***",
      "first_name":  "John",
      "last_name":   "D***",
      "title":       "CTO",
      "company":     "Acme Corp",
      "city":        "London",
      "country":     "GB",
      "status":      "apollo_search"
    }
  ]
}`,
      },
      {
        id: 'ap-enrich', method: 'POST', path: '/api/bd-filter/apollo/search', title: 'Apollo Enrichment (Credits)',
        desc: 'Same endpoint as free search but with enrich: true. After the initial search, calls POST /api/v1/people/match for each result using the regular API key — costs 1 credit per successful match. Returns full unobfuscated profile: real last name, work email, LinkedIn URL, and phone (async via webhook if APOLLO_WEBHOOK_BASE_URL is set). Status saved as "apollo_enriched".',
        curl:
`curl -X POST ${BASE}/api/bd-filter/apollo/search \\
  -H "Content-Type: application/json" \\
  -d '{
    "token":          "${TOKEN}",
    "person_titles":  ["CTO"],
    "person_locations":["London, GB"],
    "per_page": 10,
    "page":     1,
    "enrich":   true
  }'`,
        response:
`{
  "total_entries":   8420,
  "page":            1,
  "per_page":        10,
  "enrich_mode":     true,
  "saved":           9,
  "skipped":         0,
  "failed":          0,
  "enrich_ok":       9,
  "enrich_failed":   1,
  "enrich_failures": ["id=abc123: people/match returned null person"],
  "results": [
    {
      "id":           "b2c3d4e5f6a7",
      "linkedin_url": "https://www.linkedin.com/in/johndoe",
      "name":         "John Doe",
      "first_name":   "John",
      "last_name":    "Doe",
      "title":        "CTO",
      "company":      "Acme Corp",
      "work_email":   "john.doe@acme.com",
      "direct_phone": "",
      "status":       "apollo_enriched"
    }
  ]
}`,
      },
      {
        id: 'ap-records', method: 'GET', path: '/api/bd-filter/apollo/records', title: 'Apollo Saved Records',
        desc: 'List profiles saved via Apollo search (both free and enriched). Token optional — omit to get all organisations. Returns records with status "apollo_search" or "apollo_enriched".',
        curl:
`# With token (org-scoped)
curl "${BASE}/api/bd-filter/apollo/records?token=${TOKEN}&page=1&page_size=50"

# Without token (all orgs)
curl "${BASE}/api/bd-filter/apollo/records?page=1&page_size=50"`,
        response:
`{
  "total":     67,
  "page":      1,
  "page_size": 50,
  "records": [
    {
      "id":            "b2c3d4e5f6a7",
      "name":          "John Doe",
      "title":         "CTO",
      "company":       "Acme Corp",
      "work_email":    "john.doe@acme.com",
      "direct_phone":  "+44 20 1234 5678",
      "industry":      "Software",
      "employee_count": 350,
      "status":        "apollo_enriched",
      "organization_id": "48eaef0d-...",
      "created_at":    "2026-04-15T09:00:00+00:00"
    }
  ]
}`,
      },
      {
        id: 'ap-seniorities', method: 'GET', path: '/api/bd-filter/apollo/seniorities', title: 'Apollo Seniority Options',
        desc: 'Returns the list of valid seniority values accepted by Apollo\'s people search filter.',
        curl:
`curl "${BASE}/api/bd-filter/apollo/seniorities"`,
        response:
`{
  "seniorities": [
    "owner", "founder", "c_suite", "partner",
    "vp", "head", "director", "manager",
    "senior", "entry", "intern"
  ]
}`,
      },
      {
        id: 'ap-webhook', method: 'POST', path: '/api/bd-filter/apollo/phone-webhook', title: 'Apollo Phone Webhook',
        desc: 'Apollo calls this endpoint asynchronously when a phone reveal is ready (triggered by reveal_phone_number: true in people/match). Updates direct_phone in enriched_leads for the matching lead. No auth required — Apollo calls this directly. Requires APOLLO_WEBHOOK_BASE_URL set in backend/.env pointing to this server\'s public URL.',
        curl:
`# Apollo POSTs automatically — example payload shape:
curl -X POST ${BASE}/api/bd-filter/apollo/phone-webhook \\
  -H "Content-Type: application/json" \\
  -d '{
    "person": {
      "id": "6241d3ba65027a0001e88b3e",
      "phone_numbers": [
        { "sanitized_number": "+442012345678", "type": "work_direct" }
      ]
    }
  }'`,
        response:
`{
  "ok":      true,
  "updated": true,
  "lead_id": "b2c3d4e5f6a7",
  "phone":   "+442012345678"
}`,
      },
    ],
  }

  const METHOD_COLOR = {
    GET:    { bg: 'rgba(16,185,129,0.12)', color: '#10b981', border: 'rgba(16,185,129,0.3)' },
    POST:   { bg: 'rgba(99,102,241,0.12)', color: '#818cf8', border: 'rgba(99,102,241,0.3)' },
    PUT:    { bg: 'rgba(245,158,11,0.12)', color: '#f59e0b', border: 'rgba(245,158,11,0.3)' },
    DELETE: { bg: 'rgba(239,68,68,0.12)',  color: '#ef4444', border: 'rgba(239,68,68,0.3)' },
  }

  const list = ENDPOINTS[activeGroup] || []

  return (
    <div style={{ height: '100%', overflow: 'auto', background: 'var(--bg-base)', padding: '24px 28px' }}>
      {/* Header */}
      <div style={{ marginBottom: 22, display: 'flex', alignItems: 'center', gap: 12 }}>
        <div style={{
          width: 36, height: 36, borderRadius: 10,
          background: 'linear-gradient(135deg,#0ea5e9,#6366f1)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
        }}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
          </svg>
        </div>
        <div>
          <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: 'var(--text-1)' }}>API Reference</h1>
          <p style={{ margin: 0, fontSize: 11, color: 'var(--text-3)' }}>
            Base URL: <code style={{ color: '#818cf8' }}>{BASE}</code>
          </p>
        </div>
      </div>

      <div style={{ display: 'flex', gap: 0 }}>

        {/* Left nav */}
        <div style={{
          width: 192, flexShrink: 0, borderRight: '1px solid var(--border-1)',
          paddingRight: 16, display: 'flex', flexDirection: 'column', gap: 2,
        }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
            letterSpacing: '0.08em', marginBottom: 8 }}>Sections</div>

          {GROUPS.map(g => {
            if (g.divider) return (
              <div key={g.id} style={{ marginTop: 10, marginBottom: 4 }}>
                <div style={{
                  fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase',
                  letterSpacing: '0.1em', paddingLeft: 10,
                  borderLeft: '2px solid rgba(99,102,241,0.3)',
                }}>
                  {g.label}
                </div>
              </div>
            )
            const isActive = activeGroup === g.id
            return (
              <button key={g.id} onClick={() => setActiveGroup(g.id)} style={{
                display: 'flex', alignItems: 'center', gap: 8, padding: '7px 10px',
                borderRadius: 7, border: 'none', cursor: 'pointer', textAlign: 'left', width: '100%',
                background: isActive ? 'rgba(99,102,241,0.12)' : 'transparent',
                color: isActive ? '#a5b4fc' : 'var(--text-2)',
                fontSize: 12, fontWeight: isActive ? 600 : 400,
                transition: 'all 0.12s',
              }}>
                <span style={{ fontSize: 13 }}>{g.icon}</span>
                {g.label}
              </button>
            )
          })}

          {/* Legend cards */}
          <div style={{ marginTop: 16, padding: '10px 12px', borderRadius: 8,
            background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: '#818cf8', textTransform: 'uppercase',
              letterSpacing: '0.07em', marginBottom: 5 }}>Bulk Auth</div>
            <div style={{ fontSize: 10, color: 'var(--text-2)', fontFamily: 'monospace', lineHeight: 1.7 }}>
              Token in body:<br /><span style={{ color: '#a7f3d0' }}>"token": "&lt;token&gt;"</span>
            </div>
          </div>

          <div style={{ marginTop: 8, padding: '10px 12px', borderRadius: 8,
            background: 'rgba(16,185,129,0.06)', border: '1px solid rgba(16,185,129,0.2)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: '#10b981', textTransform: 'uppercase',
              letterSpacing: '0.07em', marginBottom: 5 }}>AI Endpoints</div>
            <div style={{ fontSize: 10, color: 'var(--text-2)', fontFamily: 'monospace', lineHeight: 1.7 }}>
              No auth required<br />Just send profile
            </div>
          </div>
        </div>

        {/* Right content */}
        <div style={{ flex: 1, paddingLeft: 24 }}>
          {activeGroup === 'token'
            ? <TokenGenerator BASE={BASE} copy={copy} copied={copied} />
            : activeGroup === 'custom'
            ? <CustomFeaturesDoc
                BASE={BASE}
                features={customFeatures}
                loading={cfLoading}
                copy={copy}
                copied={copied}
                onRefresh={() => {
                  setCustomFeatures([])
                  setCfLoading(true)
                  fetch(`${API_DOC_BASE}/api/v1/features`, { headers: docAuthHeaders() })
                    .then(r => r.json())
                    .then(d => setCustomFeatures(d.features || []))
                    .catch(() => {})
                    .finally(() => setCfLoading(false))
                }}
              />
            : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
                {list.map(ep => {
                  const mc = METHOD_COLOR[ep.method] || METHOD_COLOR.GET
                  return (
                    <div key={ep.id} style={{
                      borderRadius: 10, border: '1px solid var(--border-1)',
                      background: 'var(--bg-card)', overflow: 'hidden',
                    }}>
                      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-1)',
                        display: 'flex', alignItems: 'center', gap: 10 }}>
                        <span style={{
                          fontSize: 10, fontWeight: 700, padding: '2px 9px', borderRadius: 5, flexShrink: 0,
                          background: mc.bg, color: mc.color, border: `1px solid ${mc.border}`,
                        }}>{ep.method}</span>
                        <code style={{ fontSize: 12, color: '#a5b4fc', fontFamily: 'monospace', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{ep.path}</code>
                        <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', flexShrink: 0 }}>{ep.title}</span>
                      </div>
                      <div style={{ padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 14 }}>
                        <p style={{ margin: 0, fontSize: 12, color: 'var(--text-2)', lineHeight: 1.65 }}>{ep.desc}</p>
                        <div>
                          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                            <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>curl</span>
                            <CopyBtn id={ep.id + '-curl'} text={ep.curl} copied={copied} onCopy={copy} />
                          </div>
                          <pre style={{
                            margin: 0, padding: '10px 14px', borderRadius: 8, overflow: 'auto',
                            background: 'rgba(0,0,0,0.4)', border: '1px solid rgba(255,255,255,0.06)',
                            fontSize: 11, lineHeight: 1.75, color: '#e2e8f0', fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                            whiteSpace: 'pre',
                          }}>{ep.curl}</pre>
                        </div>
                        <div>
                          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                            <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>response</span>
                            <CopyBtn id={ep.id + '-res'} text={ep.response} copied={copied} onCopy={copy} />
                          </div>
                          <pre style={{
                            margin: 0, padding: '10px 14px', borderRadius: 8, overflow: 'auto',
                            background: 'rgba(0,0,0,0.4)', border: '1px solid rgba(16,185,129,0.2)',
                            fontSize: 11, lineHeight: 1.75, color: '#6ee7b7', fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                            whiteSpace: 'pre',
                          }}>{ep.response}</pre>
                        </div>
                      </div>
                    </div>
                  )
                })}
              </div>
            )
          }
        </div>
      </div>
    </div>
  )
}

// ── Token Generator ────────────────────────────────────────────────────────────

function TokenGenerator({ BASE, copy, copied }) {
  const [email,    setEmail]    = useState('')
  const [password, setPassword] = useState('')
  const [showPw,   setShowPw]   = useState(false)
  const [loading,  setLoading]  = useState(false)
  const [result,   setResult]   = useState(null)   // { token, user } | { error }

  const generate = async () => {
    if (!email.trim() || !password.trim()) return
    setLoading(true)
    setResult(null)
    try {
      const r = await fetch(`${BASE}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: email.trim(), password }),
      })
      const d = await r.json()
      if (!r.ok) {
        setResult({ error: d.detail || 'Login failed' })
      } else {
        setResult({ token: d.access_token, user: d.user, expires_in: d.expires_in })
      }
    } catch (e) {
      setResult({ error: 'Could not reach server' })
    } finally {
      setLoading(false)
    }
  }

  const inputStyle = {
    width: '100%', padding: '9px 12px', borderRadius: 8, fontSize: 13,
    border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
    color: 'var(--text-1)', outline: 'none', boxSizing: 'border-box',
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Info card */}
      <div style={{ padding: '14px 16px', borderRadius: 10, border: '1px solid rgba(99,102,241,0.25)',
        background: 'rgba(99,102,241,0.06)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <Key size={14} color="#818cf8" />
          <span style={{ fontSize: 13, fontWeight: 700, color: '#a5b4fc' }}>Generate Your WorksBuddy Token</span>
        </div>
        <p style={{ margin: 0, fontSize: 12, color: 'var(--text-2)', lineHeight: 1.65 }}>
          Login with your <strong style={{ color: 'var(--text-1)' }}>WorksBuddy admin account</strong>.
          Only admin + owner accounts from the WorksBuddy organisation can generate a token.
          The token is valid for <strong style={{ color: 'var(--text-1)' }}>8 hours</strong> and carries{' '}
          <code style={{ color: '#a7f3d0', fontSize: 11 }}>platform: "worksbuddy"</code>,{' '}
          <code style={{ color: '#fde68a', fontSize: 11 }}>organization_id</code>, and{' '}
          <code style={{ color: '#fde68a', fontSize: 11 }}>sso_id</code> — all required by the Bulk Enrich API.
        </p>
      </div>

      {/* Form */}
      <div style={{ padding: '20px', borderRadius: 10, border: '1px solid var(--border-1)', background: 'var(--bg-card)', display: 'flex', flexDirection: 'column', gap: 14 }}>
        <div>
          <label style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-3)', display: 'block', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            WorksBuddy Email
          </label>
          <input
            type="email"
            placeholder="you@worksbuddy.ai"
            value={email}
            onChange={e => setEmail(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && generate()}
            style={inputStyle}
          />
        </div>

        <div>
          <label style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-3)', display: 'block', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            Password
          </label>
          <div style={{ position: 'relative' }}>
            <input
              type={showPw ? 'text' : 'password'}
              placeholder="••••••••"
              value={password}
              onChange={e => setPassword(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && generate()}
              style={{ ...inputStyle, paddingRight: 40 }}
            />
            <button onClick={() => setShowPw(p => !p)} style={{
              position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
              background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-3)', padding: 2,
            }}>
              {showPw ? <EyeOff size={14} /> : <Eye size={14} />}
            </button>
          </div>
        </div>

        <button
          onClick={generate}
          disabled={loading || !email.trim() || !password.trim()}
          style={{
            padding: '10px 20px', borderRadius: 8, border: 'none', cursor: loading ? 'wait' : 'pointer',
            background: loading ? 'rgba(99,102,241,0.5)' : 'linear-gradient(135deg,#6366f1,#0ea5e9)',
            color: '#fff', fontSize: 13, fontWeight: 700, display: 'flex', alignItems: 'center',
            justifyContent: 'center', gap: 8, opacity: (!email.trim() || !password.trim()) ? 0.5 : 1,
            transition: 'opacity 0.15s',
          }}
        >
          {loading ? <><Loader size={14} style={{ animation: 'spin 1s linear infinite' }} /> Generating…</> : <><Key size={14} /> Generate Token</>}
        </button>
      </div>

      {/* Result */}
      {result && (
        result.error ? (
          <div style={{ padding: '14px 16px', borderRadius: 10, border: '1px solid rgba(239,68,68,0.3)',
            background: 'rgba(239,68,68,0.08)', color: '#f87171', fontSize: 13 }}>
            {result.error}
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {/* User info */}
            <div style={{ padding: '14px 16px', borderRadius: 10, border: '1px solid rgba(16,185,129,0.25)',
              background: 'rgba(16,185,129,0.06)' }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: '#10b981', textTransform: 'uppercase',
                letterSpacing: '0.07em', marginBottom: 10 }}>Login Successful</div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 16px' }}>
                {[
                  ['Name',    result.user?.full_name],
                  ['Email',   result.user?.email],
                  ['SSO ID',  result.user?.sso_id],
                  ['Role',    result.user?.role],
                  ['Org',     result.user?.organization],
                  ['Org ID',  result.user?.organization_id],
                  ['Expires', `${(result.expires_in / 3600).toFixed(0)}h`],
                ].map(([k, v]) => (
                  <div key={k}>
                    <div style={{ fontSize: 9, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{k}</div>
                    <div style={{ fontSize: 12, color: 'var(--text-1)', fontWeight: 500, wordBreak: 'break-all' }}>{v || '—'}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Token */}
            <div style={{ padding: '14px 16px', borderRadius: 10, border: '1px solid var(--border-1)', background: 'var(--bg-card)' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
                <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
                  Your Token
                </span>
                <CopyBtn id="generated-token" text={result.token} copied={copied} onCopy={copy} />
              </div>
              <pre style={{
                margin: 0, padding: '10px 14px', borderRadius: 8, overflow: 'auto',
                background: 'rgba(0,0,0,0.4)', border: '1px solid rgba(255,255,255,0.06)',
                fontSize: 11, lineHeight: 1.6, color: '#6ee7b7', fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                whiteSpace: 'pre-wrap', wordBreak: 'break-all',
              }}>{result.token}</pre>
              <p style={{ margin: '10px 0 0', fontSize: 11, color: 'var(--text-3)', lineHeight: 1.5 }}>
                Copy this token and use it as <code style={{ color: '#fde68a' }}>"token"</code> in the Bulk Enrich body.
                It carries <code style={{ color: '#a7f3d0' }}>organization_id</code>, <code style={{ color: '#a7f3d0' }}>sso_id</code>,
                and <code style={{ color: '#a7f3d0' }}>platform: "worksbuddy"</code>.
              </p>
            </div>
          </div>
        )
      )}

      {/* curl reference */}
      <div style={{ padding: '14px 16px', borderRadius: 10, border: '1px solid var(--border-1)', background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
          <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
            curl equivalent
          </span>
          <CopyBtn id="token-curl"
            text={`curl -X POST ${BASE}/api/auth/login \\\n  -H "Content-Type: application/json" \\\n  -d '{"email":"you@worksbuddy.ai","password":"your_password"}'`}
            copied={copied} onCopy={copy} />
        </div>
        <pre style={{
          margin: 0, padding: '10px 14px', borderRadius: 8, overflow: 'auto',
          background: 'rgba(0,0,0,0.4)', border: '1px solid rgba(255,255,255,0.06)',
          fontSize: 11, lineHeight: 1.75, color: '#e2e8f0', fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
          whiteSpace: 'pre',
        }}>{`curl -X POST ${BASE}/api/auth/login \\
  -H "Content-Type: application/json" \\
  -d '{
    "email": "you@worksbuddy.ai",
    "password": "your_password"
  }'`}</pre>
      </div>

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}

// ── Custom Features Doc ────────────────────────────────────────────────────────

function buildSampleInputs(params) {
  const obj = {}
  for (const p of (params || [])) {
    obj[p.key] = p.placeholder || `<${p.label || p.key}>`
  }
  return obj
}

function buildCurl(BASE, feature) {
  const params = typeof feature.input_params === 'string'
    ? JSON.parse(feature.input_params || '[]')
    : (feature.input_params || [])
  const sample = buildSampleInputs(params)
  const body = JSON.stringify({ inputs: sample }, null, 4)
    .split('\n').map((l, i) => i === 0 ? l : '  ' + l).join('\n')
  return `curl -X POST ${BASE}/api/v1/features/run/${feature.endpoint_slug} \\
  -H "Content-Type: application/json" \\
  -d '${body}'`
}

function buildSampleResponse(feature) {
  return `{
  "feature_code": "${feature.feature_code}",
  "feature_name": "${feature.feature_name}",
  "slug": "${feature.endpoint_slug}",
  "result": {
    // ... AI-generated JSON output based on system prompt
  },
  "token_usage": {
    "prompt_tokens": 120,
    "completion_tokens": 80,
    "total_tokens": 200,
    "model": "${feature.model_name || 'llama-3.1-8b-instant'}",
    "provider": "${feature.model_provider || 'groq'}"
  },
  "debug": {
    "system_prompt": "... resolved system prompt ...",
    "user_message": "... built from inputs ...",
    "model_provider": "${feature.model_provider || 'groq'}",
    "model_name": "${feature.model_name || 'llama-3.1-8b-instant'}",
    "temperature": ${feature.temperature ?? 0.3}
  }
}`
}

function CustomFeaturesDoc({ BASE, features, loading, copy, copied, onRefresh }) {
  const POST_COLOR = { bg: 'rgba(99,102,241,0.12)', color: '#818cf8', border: 'rgba(99,102,241,0.3)' }

  if (loading) {
    return <div style={{ color: 'var(--text-3)', fontSize: 13 }}>Loading custom features…</div>
  }

  if (features.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: '48px 32px', border: '1px dashed var(--border-1)', borderRadius: 12 }}>
        <div style={{ fontSize: 32, marginBottom: 12 }}>⚡</div>
        <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-1)', marginBottom: 8 }}>No custom features yet</div>
        <div style={{ fontSize: 12, color: 'var(--text-3)', marginBottom: 20 }}>
          Create custom features in the <b style={{ color: 'var(--text-2)' }}>Custom Features</b> page — they'll appear here automatically.
        </div>
        <button onClick={onRefresh} style={{
          background: 'transparent', border: '1px solid var(--border-1)',
          color: 'var(--text-3)', cursor: 'pointer', borderRadius: 7,
          padding: '6px 14px', fontSize: 12, fontWeight: 600,
        }}>↻ Refresh</button>
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
      {/* Header info */}
      <div style={{
        padding: '12px 16px', borderRadius: 10,
        background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
      }}>
        <div>
          <div style={{ fontSize: 12, fontWeight: 700, color: '#a5b4fc', marginBottom: 4 }}>
            {features.length} Custom Feature{features.length !== 1 ? 's' : ''}
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-3)' }}>
            No authentication required — pass inputs directly in the request body.
          </div>
        </div>
        <button onClick={onRefresh} style={{
          background: 'transparent', border: '1px solid var(--border-1)',
          color: 'var(--text-3)', cursor: 'pointer', borderRadius: 7,
          padding: '5px 12px', fontSize: 11, fontWeight: 600,
        }}>↻ Refresh</button>
      </div>

      {features.map(f => {
        const params = typeof f.input_params === 'string'
          ? JSON.parse(f.input_params || '[]')
          : (f.input_params || [])
        const curlText = buildCurl(BASE, f)
        const resText  = buildSampleResponse(f)
        const curlId   = `cf-curl-${f.id}`
        const resId    = `cf-res-${f.id}`

        return (
          <div key={f.id} style={{
            borderRadius: 10, border: '1px solid var(--border-1)',
            background: 'var(--bg-card)', overflow: 'hidden',
          }}>
            {/* Header */}
            <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-1)',
              display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
              <span style={{
                fontSize: 10, fontWeight: 700, padding: '2px 9px', borderRadius: 5, flexShrink: 0,
                background: POST_COLOR.bg, color: POST_COLOR.color, border: `1px solid ${POST_COLOR.border}`,
              }}>POST</span>
              <code style={{ fontSize: 11, color: '#a5b4fc', fontFamily: 'monospace', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                /api/v1/features/run/{f.endpoint_slug}
              </code>
              <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)', flexShrink: 0 }}>{f.feature_name}</span>
              <span style={{ fontSize: 9, padding: '2px 7px', borderRadius: 4, fontWeight: 700, background: 'rgba(99,102,241,0.15)', color: '#a5b4fc', flexShrink: 0 }}>
                {f.module}
              </span>
              <span style={{ fontSize: 9, padding: '2px 7px', borderRadius: 4, fontWeight: 700, background: 'rgba(16,185,129,0.1)', color: '#34d399', flexShrink: 0 }}>
                {f.task_type}
              </span>
            </div>

            <div style={{ padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 14 }}>
              {/* Description */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {f.description && (
                  <p style={{ margin: 0, fontSize: 12, color: 'var(--text-2)', lineHeight: 1.65 }}>{f.description}</p>
                )}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 14, fontSize: 11, color: 'var(--text-3)' }}>
                  <span>Code: <b style={{ color: 'var(--text-2)', fontFamily: 'monospace' }}>{f.feature_code}</b></span>
                  <span>Model: <b style={{ color: 'var(--text-2)', fontFamily: 'monospace' }}>{f.model_provider}/{f.model_name || '—'}</b></span>
                  <span>Temp: <b style={{ color: 'var(--text-2)' }}>{f.temperature ?? 0.3}</b></span>
                </div>
              </div>

              {/* Input params table */}
              {params.length > 0 && (
                <div>
                  <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 8 }}>
                    Input Parameters
                  </div>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
                    <thead>
                      <tr style={{ borderBottom: '1px solid var(--border-1)' }}>
                        {['key', 'label', 'type', 'placeholder'].map(h => (
                          <th key={h} style={{ padding: '5px 10px', textAlign: 'left', color: 'var(--text-3)', fontWeight: 700, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.06em' }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {params.map((p, i) => (
                        <tr key={i} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                          <td style={{ padding: '6px 10px', fontFamily: 'monospace', color: '#a5b4fc' }}>{p.key}</td>
                          <td style={{ padding: '6px 10px', color: 'var(--text-2)' }}>{p.label}</td>
                          <td style={{ padding: '6px 10px', color: '#fbbf24', fontFamily: 'monospace' }}>{p.type}</td>
                          <td style={{ padding: '6px 10px', color: 'var(--text-3)', fontStyle: 'italic' }}>{p.placeholder}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* System prompt */}
              {f.system_prompt && (
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                    <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>System Prompt</span>
                    <CopyBtn id={`cf-sys-${f.id}`} text={f.system_prompt} copied={copied} onCopy={copy} />
                  </div>
                  <pre style={{
                    margin: 0, padding: '10px 14px', borderRadius: 8, overflow: 'auto',
                    background: 'rgba(0,0,0,0.4)', border: '1px solid rgba(245,158,11,0.2)',
                    fontSize: 11, lineHeight: 1.75, color: '#fde68a',
                    fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                    whiteSpace: 'pre-wrap', wordBreak: 'break-word', maxHeight: 160,
                  }}>{f.system_prompt}</pre>
                </div>
              )}

              {/* Curl */}
              <div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>curl</span>
                  <CopyBtn id={curlId} text={curlText} copied={copied} onCopy={copy} />
                </div>
                <pre style={{
                  margin: 0, padding: '10px 14px', borderRadius: 8, overflow: 'auto',
                  background: 'rgba(0,0,0,0.4)', border: '1px solid rgba(255,255,255,0.06)',
                  fontSize: 11, lineHeight: 1.75, color: '#e2e8f0',
                  fontFamily: "'JetBrains Mono', 'Fira Code', monospace", whiteSpace: 'pre',
                }}>{curlText}</pre>
              </div>

              {/* Response */}
              <div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>response schema</span>
                  <CopyBtn id={resId} text={resText} copied={copied} onCopy={copy} />
                </div>
                <pre style={{
                  margin: 0, padding: '10px 14px', borderRadius: 8, overflow: 'auto',
                  background: 'rgba(0,0,0,0.4)', border: '1px solid rgba(16,185,129,0.2)',
                  fontSize: 11, lineHeight: 1.75, color: '#6ee7b7',
                  fontFamily: "'JetBrains Mono', 'Fira Code', monospace", whiteSpace: 'pre',
                }}>{resText}</pre>
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Copy Button ────────────────────────────────────────────────────────────────

function CopyBtn({ id, text, copied, onCopy }) {
  const active = copied === id
  return (
    <button onClick={() => onCopy(id, text)} style={{
      display: 'flex', alignItems: 'center', gap: 4, padding: '3px 9px',
      borderRadius: 5, border: '1px solid var(--border-1)', background: 'var(--bg-elevated)',
      cursor: 'pointer', fontSize: 10, color: active ? '#10b981' : 'var(--text-3)',
      transition: 'color 0.15s',
    }}>
      {active ? <><Check size={9} /> Copied</> : <><Copy size={9} /> Copy</>}
    </button>
  )
}
