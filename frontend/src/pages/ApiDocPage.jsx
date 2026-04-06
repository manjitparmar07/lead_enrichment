// ApiDocPage.jsx — Lead Enrichment API Reference

import { useState, useEffect } from 'react'
import { Copy, Check, Eye, EyeOff, Key, Loader } from 'lucide-react'

const API_DOC_BASE = import.meta.env.VITE_BACKEND_URL || ''

function docAuthHeaders() {
  const token = localStorage.getItem('wb_ai_token') || ''
  return { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }
}

const BASE = import.meta.env.VITE_BACKEND_URL || 'https://leadenrichment-production-5b78.up.railway.app'
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
        id: 'email-enrich', method: 'POST', path: '/api/leads/view/email', title: 'Email Enrichment',
        desc: 'Returns the email enrichment result — best verified work email, source provider (apollo/hunter), confidence score, verification status, bounce risk, and all discovered emails. Pass leadenrich_id from bulk job results. If no email is stored a live Apollo → Hunter lookup runs automatically. Optional: pass system_prompt to get an AI-generated personalised email template in ai_generated.email_template.',
        curl:
`curl -X POST ${BASE}/api/leads/view/email \\
  -H "Content-Type: application/json" \\
  -d '{
    "leadenrich_id": "abc123def456",
    "system_prompt": "You are a B2B sales AI for Acme Inc..."
  }'`,
        response:
`{
  "lead_id": "abc123def456",
  "name": "John Doe",
  "company": "Acme Corp",

  "work_email": "john.doe@acme.com",
  "email": "john.doe@acme.com",
  "source": "apollo",
  "confidence": 92,
  "verified": true,
  "bounce_risk": "low",
  "enrichment_source": "apollo_person_match",

  "phone": "+1-415-555-0100",

  "all_emails": ["john.doe@acme.com", "jdoe@acme.com"],
  "activity_emails": ["john@personalsite.com"],
  "activity_phones": [],

  // Only present when system_prompt is provided:
  "ai_generated": {
    "email_template": "{\"subject\": \"Quick idea for John at Acme\", \"body\": \"Hi John, ...\"}"
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
  -H "Authorization: Bearer <YOUR_TOKEN>" \\
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
            Auth: <code style={{ color: '#a7f3d0', fontSize: 10 }}>Authorization: Bearer &lt;token&gt;</code> header required
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
