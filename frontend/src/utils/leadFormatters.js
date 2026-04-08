// leadFormatters.js — formatting and parsing helpers
import { COUNTRY_CODES } from './leadConfig.jsx'

/** Map ISO country code to full name; pass-through if already a name */
export const fmtCountry = (v) => {
  if (!v) return ''
  const upper = v.trim().toUpperCase()
  if (upper.length <= 3 && COUNTRY_CODES[upper]) return COUNTRY_CODES[upper]
  return v.trim()
}

/** Return only the city part from "City, State, Country" strings */
export const fmtCity = (v) => (v || '').split(',')[0].trim()

export function parseArr(v) {
  if (Array.isArray(v)) return v.filter(Boolean)
  if (typeof v === 'string' && v.startsWith('[')) {
    try { return JSON.parse(v).filter(Boolean) } catch {}
  }
  if (typeof v === 'string' && v) return v.split(',').map(s => s.trim()).filter(Boolean)
  return []
}

export function parseTags(v) { return parseArr(v) }

export function _buildJsonPayload(lead) {
  let full = {}
  try {
    full = (lead.full_data && typeof lead.full_data === 'object')
      ? lead.full_data
      : JSON.parse(lead.full_data || '{}')
  } catch {}
  return {
    id: lead.id,
    enriched_at: lead.enriched_at,
    data_completeness: `${lead.data_completeness}%`,
    identity: {
      full_name: lead.name,
      avatar_url: lead.avatar_url,
      work_email: lead.work_email,
      personal_email: lead.personal_email,
      direct_phone: lead.direct_phone,
      linkedin_url: lead.linkedin_url,
      twitter: lead.twitter,
      city: lead.city,
      country: lead.country,
      timezone: lead.timezone,
      ...(full.identity || {}),
    },
    professional: {
      current_title: lead.title,
      seniority_level: lead.seniority_level,
      department: lead.department,
      years_in_role: lead.years_in_role,
      previous_companies: parseArr(lead.previous_companies),
      top_skills: parseArr(lead.top_skills),
      education: lead.education,
      certifications: parseArr(lead.certifications),
      languages: parseArr(lead.languages),
      ...(full.professional || {}),
    },
    company: {
      name: lead.company,
      logo: lead.company_logo,
      website: lead.company_website,
      email: lead.company_email,
      phone: lead.company_phone,
      description: lead.company_description,
      linkedin: lead.company_linkedin,
      twitter: lead.company_twitter,
      industry: lead.industry,
      employee_count: lead.employee_count,
      hq_location: lead.hq_location,
      founded_year: lead.founded_year,
      funding_stage: lead.funding_stage,
      total_funding: lead.total_funding,
      last_funding_date: lead.last_funding_date,
      lead_investor: lead.lead_investor,
      annual_revenue_est: lead.annual_revenue,
      tech_stack: parseArr(lead.tech_stack),
      hiring_velocity: lead.hiring_velocity,
      ...(full.company || {}),
    },
    intent_signals: {
      recent_funding_event: lead.recent_funding_event,
      hiring_signal: lead.hiring_signal,
      job_change: lead.job_change,
      linkedin_activity: lead.linkedin_activity,
      news_mention: lead.news_mention,
      product_launch: lead.product_launch,
      competitor_usage: lead.competitor_usage,
      review_activity: lead.review_activity,
      ...(full.intent_signals || {}),
    },
    scoring: {
      icp_fit_score: lead.icp_fit_score,
      intent_score: lead.intent_score,
      timing_score: lead.timing_score,
      overall_score: lead.total_score,
      score_explanation: lead.score_explanation,
      icp_match_tier: lead.icp_match_tier,
      disqualification_flags: parseArr(lead.disqualification_flags),
      ...(full.scoring || {}),
    },
    outreach: {
      email_subject: lead.email_subject,
      cold_email: lead.cold_email,
      linkedin_note: lead.linkedin_note,
      best_channel: lead.best_channel,
      best_send_time: lead.best_send_time,
      outreach_angle: lead.outreach_angle,
      sequence_type: lead.sequence_type,
      sequence: (() => { try { return JSON.parse(lead.outreach_sequence || '{}') } catch { return {} } })(),
      last_contacted: lead.last_contacted,
      email_status: lead.email_status,
      ...(full.outreach || {}),
    },
    crm: {
      lead_source: lead.lead_source,
      enrichment_source: lead.enrichment_source,
      enrichment_date: lead.enriched_at,
      data_completeness: lead.data_completeness,
      crm_stage: lead.crm_stage,
      assigned_owner: lead.assigned_owner,
      tags: parseArr(lead.tags),
      ...(full.crm || {}),
    },
    website_intelligence: (() => {
      const wi = full.website_intelligence || full.website_intel || {}
      return {
        product_offerings: parseArr(lead.product_offerings),
        value_proposition: lead.value_proposition || wi.value_proposition,
        target_customers: parseArr(lead.target_customers),
        business_model: lead.business_model || wi.business_model,
        pricing_signals: lead.pricing_signals || wi.pricing_signals,
        product_category: lead.product_category || wi.product_category,
        ...wi,
      }
    })(),
    lead_scoring: {
      icp_fit_score: lead.icp_fit_score,
      intent_score: lead.intent_score,
      timing_score: lead.timing_score,
      data_completeness_score: lead.data_completeness_score,
      overall_score: lead.total_score,
      score_tier: lead.score_tier,
      score_explanation: lead.score_explanation,
      icp_match_tier: lead.icp_match_tier,
      disqualification_flags: parseArr(lead.disqualification_flags),
      ...(full.lead_scoring || full.scoring || {}),
    },
    waterfall_log: (() => { try { return JSON.parse(lead.waterfall_log || '[]') } catch { return [] } })(),
  }
}
