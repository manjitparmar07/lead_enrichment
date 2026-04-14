"""
_enrichment.py
--------------
Main enrichment pipeline functions.

Covers:
  - enrich_company_url  (Phase A/C/E — BD company → website → score → store)
  - enrich_single       (8-stage person pipeline, cache-aware)
  - enrich_single_stream (SSE streaming enrichment, 6 stages)
  - _poll_and_process_snapshot / _bd_chunk_size
  - enrich_bulk         (BD webhook-based batch pipeline)
  - _process_one_webhook_profile (full pipeline per profile)
  - rerun_brightdata_snapshot / cancel_brightdata_snapshot
  - process_webhook_profiles
  - regenerate_outreach_for_lead / regenerate_company_for_lead / regenerate_crm_brief_for_lead
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Optional

import httpx

from db import get_pool, named_args
from ._utils import (
    _parse_name, _country_name, _safe_int, _safe_json, _parse_json_safe,
    _lead_id, _bd_api_key, _plog, _pipeline_log,
    _resolve_tier, _patch_crm_brief_images, _patch_crm_brief_scores,
    _is_company_url, _is_person_url,
)
from ._clients import (
    _enrichment_semaphore, _in_flight_leads, _job_webhook_locks,
    _bd_trigger_semaphore, BD_BASE,
)
from ._db import (
    _upsert_lead, _update_job, _create_sub_job, _update_sub_job,
    get_lead, get_lead_by_url, check_existing_lead, check_existing_leads_batch,
    mark_snapshot_processed, is_snapshot_processed, audit_log,
    _format_linkedin_enrich, _publish_lead_done, _publish_job_done,
    get_job,
)
from ._llm import _call_llm, _parse_json_from_llm
from ._brightdata import (
    _normalize_linkedin_url, _clean_bd_linkedin_url,
    _normalize_bd_profile, fetch_profile_sync, trigger_batch_snapshot,
    poll_snapshot, _bd_headers,
)
from ._contact import _extract_domain, find_contact_info
from ._company import enrich_company_waterfall, _normalize_bd_company, _try_bd_company
from ._website import scrape_website_intelligence, _extract_avatar
from ._outreach import (
    send_to_lio, send_to_lio_failed, build_comprehensive_enrichment,
    generate_outreach_with_lio, _merge_lio_into_enrichment, run_lio_pipeline,
    _validate_crm_brief, _DEFAULT_CRM_BRIEF_PROMPT, _build_llm_profile,
    _trim_profile_to_budget,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Company URL direct enrichment pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_company_url(
    linkedin_url: str,
    job_id: Optional[str] = None,
    org_id: str = "default",
    forward_to_lio: bool = False,
) -> dict:
    """
    Enrich a LinkedIn Company URL directly.

    Phase A — Bright Data company dataset scrape
    Phase B — Apollo company name/website search (fills gaps)
    Phase C — Website intelligence (deep scrape)
    Phase D — Score + store

    Returns the same flat DB dict as enrich_single() so the same
    UI and API response shape is used for both person and company enrichment.
    """
    url = linkedin_url.strip().rstrip("/")
    if not url.startswith("http"):
        url = f"https://{url}"
    lead_id = _lead_id(url)
    now = datetime.now(timezone.utc).isoformat()

    logger.info("━━━ [Company-A] Bright Data company scrape: %s", url)
    bd_co = await _try_bd_company(url)

    company_name = (
        bd_co.get("name") or bd_co.get("company_name")
        or url.rstrip("/").split("/")[-1].replace("-", " ").title()
    )
    website = bd_co.get("company_website", "")
    domain = ""
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m:
            domain = m.group(1).lower()

    logger.info("[Company-A] BD result: name=%r  website=%r  domain=%r  fields=%d",
                company_name, website, domain, len(bd_co))

    # Merge: BD data only (Apollo removed)
    company_data: dict = {}
    company_data.update({k: v for k, v in bd_co.items() if v})

    # Re-sync website/domain from merged data
    if company_data.get("company_website") and not website:
        website = company_data["company_website"]
        m3 = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m3:
            domain = m3.group(1).lower()

    company_email = company_data.get("company_email", "")

    # ── Phase C: Website intelligence ─────────────────────────────────────────
    logger.info("━━━ [Company-C] Website intelligence: %s", website or "none")
    website_intel = await scrape_website_intelligence(website) if website else {}
    if website_intel:
        logger.info("[Company-D] Scraped %d pages, category=%s",
                    len(website_intel.get("pages_scraped", [])),
                    website_intel.get("product_category") or "?")

    # ── Phase E: Score + build record ─────────────────────────────────────────
    logger.info("━━━ [Company-E] Scoring + building record")

    # apollo_data was removed — use empty dict for compat
    apollo_data: dict = {}

    final_name = (
        company_data.get("name") or company_data.get("company_name")
        or apollo_data.get("name") or company_name
    )

    # Company completeness score (0-100)
    completeness_fields = [
        website, company_email,
        company_data.get("company_description"),
        company_data.get("company_logo"),
        company_data.get("industry"),
        str(company_data.get("employee_count") or ""),
        company_data.get("hq_location"),
        company_data.get("founded_year"),
        website_intel.get("product_category"),
        website_intel.get("value_proposition"),
    ]
    data_completeness = sum(10 for f in completeness_fields if f)

    # ICP / intent scoring for company leads
    icp_score = 0
    if company_data.get("industry"): icp_score += 8
    if company_data.get("employee_count", 0) > 10: icp_score += 6
    if company_data.get("employee_count", 0) > 100: icp_score += 4
    if website: icp_score += 6
    if website_intel.get("product_category"): icp_score += 8
    if website_intel.get("value_proposition"): icp_score += 4
    if company_data.get("funding_stage"): icp_score += 4
    icp_score = min(icp_score, 40)

    intent_score = 0
    if company_email: intent_score += 10
    if website_intel.get("pages_scraped"): intent_score += 8
    if company_data.get("company_description"): intent_score += 6
    if company_data.get("tech_stack"): intent_score += 6
    intent_score = min(intent_score, 30)

    timing_score = 0
    if domain: timing_score += 8
    if company_data.get("founded_year"): timing_score += 4
    if company_data.get("funding_stage"): timing_score += 5
    if company_data.get("hq_location"): timing_score += 3
    timing_score = min(timing_score, 20)

    dc_score = min(data_completeness, 10)
    total_score = icp_score + intent_score + timing_score + dc_score
    score_tier = _resolve_tier(
        "hot" if total_score >= 70 else "warm" if total_score >= 50 else "cool" if total_score >= 30 else "cold"
    )

    # Waterfall log
    waterfall_log = []
    if bd_co:
        waterfall_log.append({"step": 0, "source": "Bright Data Company", "fields_found": list(bd_co.keys()), "note": url})
    if apollo_data:
        waterfall_log.append({"step": 1, "source": "Apollo.io", "fields_found": list(apollo_data.keys()), "note": domain})
    if website_intel:
        waterfall_log.append({"step": 2, "source": "Website Scrape", "fields_found": website_intel.get("pages_scraped", []), "note": website})

    wi = website_intel or {}
    lead: dict = {
        "id": lead_id,
        "linkedin_url": url,
        # Identity — use company name as "person" name so UI renders it
        "name": final_name,
        "first_name": final_name,
        "last_name": "",
        "work_email": company_email or "",
        "personal_email": "",
        "direct_phone": company_data.get("company_phone", ""),
        "twitter": company_data.get("company_twitter", ""),
        "city": (company_data.get("hq_location") or "").split(",")[0].strip(),
        "country": (company_data.get("hq_location") or "").split(",")[-1].strip(),
        "timezone": "",
        # Professional — company-level context
        "title": wi.get("value_proposition") or company_data.get("company_description", "")[:100] or "Company",
        "seniority_level": "Company Account",
        "department": company_data.get("industry", ""),
        "years_in_role": "",
        "company": final_name,
        "previous_companies": "[]",
        "top_skills": _safe_json(company_data.get("tech_stack") or []),
        "education": "",
        "certifications": "[]",
        "languages": "[]",
        # Company
        "company_website": website or "",
        "industry": company_data.get("industry", ""),
        "employee_count": _safe_int(company_data.get("employee_count")),
        "hq_location": company_data.get("hq_location", ""),
        "founded_year": str(company_data.get("founded_year") or ""),
        "funding_stage": company_data.get("funding_stage", ""),
        "total_funding": company_data.get("total_funding", ""),
        "last_funding_date": company_data.get("last_funding_date", ""),
        "lead_investor": company_data.get("lead_investor", ""),
        "annual_revenue": "",
        "tech_stack": _safe_json(company_data.get("tech_stack") or []),
        "hiring_velocity": "",
        # Company enrichment fields
        "avatar_url": company_data.get("company_logo", ""),
        "company_logo": company_data.get("company_logo", ""),
        "company_email": company_email or "",
        "company_description": company_data.get("company_description", ""),
        "company_linkedin": url,
        "company_twitter": company_data.get("company_twitter", ""),
        "company_phone": company_data.get("company_phone", ""),
        "waterfall_log": _safe_json(waterfall_log),
        # Website intelligence
        "website_intelligence": _safe_json(wi),
        "product_offerings": _safe_json(wi.get("product_offerings") or []),
        "value_proposition": wi.get("value_proposition", ""),
        "target_customers": wi.get("target_customers", ""),
        "business_model": wi.get("business_model", ""),
        "pricing_signals": wi.get("pricing_signals", ""),
        "product_category": wi.get("product_category", ""),
        "data_completeness_score": dc_score,
        # Intent (company-level)
        "recent_funding_event": company_data.get("funding_stage", ""),
        "hiring_signal": "",
        "job_change": "",
        "linkedin_activity": "",
        "news_mention": "",
        "product_launch": "",
        "competitor_usage": "",
        "review_activity": "",
        # Scoring
        "icp_fit_score": icp_score,
        "intent_score": intent_score,
        "timing_score": timing_score,
        "engagement_score": dc_score,
        "total_score": total_score,
        "score_tier": score_tier,
        "score_explanation": f"Company account: ICP={icp_score} Intent={intent_score} Timing={timing_score} Data={dc_score}",
        "icp_match_tier": score_tier,
        "disqualification_flags": "[]",
        "score_reasons": _safe_json([
            f"ICP fit: {icp_score}/40",
            f"Intent signals: {intent_score}/30",
            f"Timing: {timing_score}/20",
            f"Data completeness: {dc_score}/10",
        ]),
        # Outreach — no person-specific copy for company records
        "email_subject": "",
        "cold_email": "",
        "linkedin_note": "",
        "best_channel": "email" if company_email else "linkedin",
        "best_send_time": "",
        "outreach_angle": wi.get("value_proposition", ""),
        "sequence_type": "",
        "outreach_sequence": "{}",
        # CRM
        "lead_source": "LinkedIn Company URL",
        "enrichment_source": "brightdata_company",
        "data_completeness": data_completeness,
        "crm_stage": "researching",
        "tags": _safe_json(["company-account"]),
        "assigned_owner": "",
        # Meta
        "full_data": _safe_json({"company": company_data, "website_intel": wi}),
        "raw_profile": _safe_json(bd_co),
        "about": company_data.get("company_description", ""),
        "followers": _safe_int(company_data.get("followers_count") or company_data.get("employee_count")),
        "connections": 0,
        "email_source": "apollo" if company_email else "",
        "email_confidence": "medium" if company_email else "",
        "status": "enriched",
        "job_id": job_id or "",
        "organization_id": org_id,
        "enriched_at": now,
        "created_at": now,
        "skills": "[]",
    }

    await _upsert_lead(lead)
    logger.info(
        "✓ Company enriched: %s | score=%d (%s) | email=%s | website=%s",
        final_name, total_score, score_tier, company_email or "—", website or "—",
    )
    if not forward_to_lio and lead.get("name"):
        asyncio.create_task(send_to_lio(lead))
    return lead


# ─────────────────────────────────────────────────────────────────────────────
# Main enrichment pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_single(
    linkedin_url: str,
    engagement_data: Optional[dict] = None,
    job_id: Optional[str] = None,
    org_id: str = "default",
    generate_outreach_flag: bool = True,
    tools: Optional[dict] = None,
    sso_id: str = "",
    forward_to_lio: bool = False,
    system_prompt: Optional[str] = None,
    skip_contact: bool = False,
) -> dict:
    """
    Sequential 8-stage enrichment waterfall:

    Phase A — Person Intelligence
      Stage 1: Bright Data LinkedIn person profile scrape
      Stage 2: Extract all person fields (name, title, skills, education, career)

    Phase B — Company Identification (using person profile data)
      Stage 3: Bright Data LinkedIn Company page scrape (company_link from profile)
      Stage 4: Apollo company name search → verify real domain + logo + description
      Stage 5: Clearbit logo fallback

    Phase C — Person Contact Waterfall (now with VERIFIED company domain)
      Stage 6: Apollo.io person match (correct domain)
      Stage 7: Dropcontact / PDL fallback

    Phase D — Deep Intelligence
      Stage 8: Website scrape (verified real company URL)
      Stage 9: Lead scoring + outreach generation
    """
    url = _normalize_linkedin_url(linkedin_url)
    lead_id = _lead_id(url)

    # ── Route: company URL → dedicated company pipeline ───────────────────────
    if _is_company_url(url):
        logger.info("[Enrich] Company URL detected — routing to company pipeline: %s", url)
        return await enrich_company_url(url, job_id=job_id, org_id=org_id, forward_to_lio=forward_to_lio)

    # ── Cache check: return existing enrichment if already in DB ─────────────
    cached = await get_lead(lead_id)
    if cached and cached.get("status") == "enriched":
        logger.info("[Enrich] Cache HIT for %s (lead_id=%s) — skipping Bright Data", url, lead_id)
        cached["_cache_hit"] = True
        cached["linkedin_enrich"] = _format_linkedin_enrich(cached, include_contact=not skip_contact)
        if not forward_to_lio:
            asyncio.create_task(send_to_lio(cached, sso_id=sso_id))
        return cached

    # ── In-flight deduplication: same URL already being processed by another request ──
    if lead_id in _in_flight_leads:
        logger.info("[Enrich] URL %s already in-flight — waiting for first request to finish", url)
        await _in_flight_leads[lead_id].wait()
        # First request done — read result from DB
        result = await get_lead(lead_id)
        if result and result.get("status") == "enriched":
            result["_cache_hit"] = True
            result["linkedin_enrich"] = _format_linkedin_enrich(result, include_contact=not skip_contact)
            if not forward_to_lio:
                asyncio.create_task(send_to_lio(result, sso_id=sso_id))
            return result
        # First request failed — fall through and try own enrichment

    # Register this URL as in-flight so concurrent requests wait instead of duplicating
    _in_flight_event = asyncio.Event()
    _in_flight_leads[lead_id] = _in_flight_event

    logger.info("[Enrich] Cache MISS for %s — fetching from Bright Data", url)

    # ── Tool availability (from enrichment config) ────────────────────────────
    _tools = tools or {}
    _brightdata_ok = _tools.get("brightdata", True)
    _apollo_ok     = _tools.get("apollo",      True)

    if not _brightdata_ok:
        logger.warning("[Enrich] Bright Data disabled or out of credits for org=%s — aborting", org_id)
        return {"error": "Bright Data is disabled or has no credits remaining for this organisation. Please update your enrichment configuration.", "linkedin_url": linkedin_url}

    # ── Phase A: Person Intelligence ──────────────────────────────────────────
    logger.info("━━━ [Phase A] Person Intelligence — scraping: %s", url)
    profile = await fetch_profile_sync(url)

    # Bright Data account/auth error — abort without saving incomplete data to DB
    if profile.get("_bd_error"):
        msg = profile.get("_bd_message", "Bright Data account error")
        logger.error("[Enrich] Aborting enrichment for %s — BD error: %s", url, msg)
        return {"error": f"Bright Data account error: {msg}", "linkedin_url": linkedin_url}

    name    = profile.get("name", "")
    first, last = _parse_name(name)
    company = profile.get("current_company_name", "")
    raw_link = profile.get("current_company_link", "")

    logger.info("[Phase A] Got: name=%r  title=%r  company=%r",
                name, profile.get("position") or profile.get("headline", ""), company)

    # Initial domain guess — may be wrong, will be corrected by Phase B
    initial_domain = _extract_domain(company, raw_link)
    logger.info("[Phase A] Initial domain guess: %s (from company name %r)", initial_domain, company)

    # ── Phase B: Company Identification Waterfall ─────────────────────────────
    logger.info("━━━ [Phase B] Company Waterfall — identifying real domain for: %s", company)
    company_extras, company_wf_log = await enrich_company_waterfall(company, initial_domain, profile, lead_id=lead_id)

    verified_domain = company_extras.pop("_verified_domain", initial_domain) or initial_domain
    real_website = company_extras.get("company_website") or (f"https://{verified_domain}" if verified_domain else "")
    logger.info("[Phase B] Verified domain: %s  website: %s  logo: %s",
                verified_domain, real_website, "✓" if company_extras.get("company_logo") else "✗")

    # ── Phases C + 1C + D — DISABLED in bulk flow ─────────────────────────────
    # Phase C (Email find)    → handled separately via /api/email-enrich/start
    # Phase 1C (Company enrich) → handled separately via company enrichment API
    # Phase D (Website scrape)  → commented out, not needed in bulk flow
    logger.info("━━━ [Bulk] Skipping Phase C (email) / Phase 1C (company) / Phase D (website)")

    activity_emails = profile.get("_activity_emails") or []
    activity_phones = profile.get("_activity_phones") or []

    # Phase C — skipped in bulk; email enrichment runs separately
    contact: dict = {}

    # Phase 1C — skipped in bulk; company enrichment runs separately
    _company_record: dict = {}
    _company_id_val: str  = ""
    _company_score: int   = 0

    # Phase D — website scrape commented out; not needed in bulk flow
    # website_intel = await scrape_website_intelligence(real_website)
    website_intel: dict = {}

    _combined_score: int = 0

    logger.info("[Phase C] Skipped — email will be found via email enrichment API")
    logger.info("[Phase 1C] Skipped — company will be enriched via company enrichment API")

    logger.info("━━━ [Phase E] Scoring + Outreach — building 8-stage report")

    # Inject company extras into profile so rule-based enrichment can access them
    profile["_company_extras"] = company_extras

    # Add website scraping step to waterfall log
    if website_intel and website_intel.get("pages_scraped"):
        company_wf_log.append({
            "step": 5, "source": "Website Scrape",
            "fields_found": website_intel.get("pages_scraped", []),
            "note": f"pages={len(website_intel.get('pages_scraped', []))}, category={website_intel.get('product_category', '?')}",
        })

    # Step 7: Comprehensive LLM enrichment → 8-stage JSON + LIO pipeline
    logger.info("[Stage 7] Scoring — LLM enrichment starting")
    enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel=website_intel, org_id=org_id, system_prompt_override=system_prompt)

    logger.info("[Stage 8] Outreach — generating personalized sequence")

    # Patch person_profile email/phone from waterfall (authoritative)
    person_prof = enrichment.get("person_profile", {})
    ident = enrichment.get("identity", {})  # legacy compat
    if contact.get("email"):
        person_prof["work_email"] = contact["email"]
        ident["work_email"] = contact["email"]
    if contact.get("phone"):
        person_prof["direct_phone"] = contact["phone"]
        ident["direct_phone"] = contact["phone"]

    # Patch company_profile section with waterfall extras (fill missing fields only)
    comp_prof = enrichment.get("company_profile", {})
    comp_section = enrichment.get("company", {})  # legacy compat
    for k, v in company_extras.items():
        _map = {
            "company_logo": "company_logo",
            "company_email": "company_email",
            "company_phone": "company_phone",
            "company_twitter": "company_twitter",
            "company_website": None,
            "industry": "industry",
            "employee_count": "employee_count",
            "hq_location": "hq_location",
            "founded_year": "founded_year",
            "funding_stage": None,
            "total_funding": None,
            "annual_revenue": "annual_revenue_est",
            "tech_stack": "tech_stack",
            "hiring_velocity": None,
        }
        _comp_section_map = {
            "company_website": "website",
            "industry": "industry",
            "employee_count": "employee_count",
            "hq_location": "hq_location",
            "founded_year": "founded_year",
            "funding_stage": "funding_stage",
            "total_funding": "total_funding",
            "annual_revenue": "annual_revenue_est",
            "tech_stack": "tech_stack",
            "hiring_velocity": "hiring_velocity",
        }
        prof_key = _map.get(k)
        if prof_key and v and not comp_prof.get(prof_key):
            comp_prof[prof_key] = v
        sect_key = _comp_section_map.get(k)
        if sect_key and v and not comp_section.get(sect_key):
            comp_section[sect_key] = v

    # Pull out scoring for flat DB columns
    scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
    total   = scoring.get("overall_score", 0)
    tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
    score_tier = _resolve_tier(tier_raw)

    # Derive crm from enrichment (may live in "crm" key)
    crm = enrichment.get("crm", {})

    # Build waterfall log combining all steps
    waterfall_log = [
        {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
        *company_wf_log,
        {
            "step": 10,
            "source": contact.get("source") or "none",
            "fields_found": ["email"],
            "note": f"email={contact.get('email')}",
        },
    ]

    # Convenience shortcuts into 8-stage keys
    wi = enrichment.get("website_intelligence", website_intel or {})
    ls = enrichment.get("lead_scoring", scoring)
    intent_s = enrichment.get("intent_signals", {})
    outreach = enrichment.get("outreach", {})
    ci = enrichment.get("company_identity", {})
    cp = enrichment.get("company_profile", comp_prof)
    cin = enrichment.get("company_intelligence", {})
    pp = enrichment.get("person_profile", {})
    # Legacy fallbacks
    prof_legacy = enrichment.get("professional", {})
    comp_legacy = enrichment.get("company", comp_section)

    # Build flat DB record
    now = datetime.now(timezone.utc).isoformat()

    # ── Activity-level intent signals ─────────────────────────────────────────
    hiring_signals_raw  = profile.get("_hiring_signals")  or []
    activity_emails_raw = profile.get("_activity_emails") or []

    # ── P3: AI Analysis Layer (tags, behavioural signals, pitch intelligence) ─
    try:
        from ai_analysis import run_ai_analysis, _rule_based_analysis
        from workspace import workspace_service as _ws
        _ws_config = await _ws.get_workspace_config(org_id)
        _ai = await run_ai_analysis(profile, _ws_config)
    except Exception as _ai_err:
        logger.warning("[AIAnalysis] Skipped (%s) — using rule-based fallback", _ai_err)
        try:
            from ai_analysis import _rule_based_analysis
            _ai = _rule_based_analysis(profile, {})
        except Exception:
            _ai = {}

    lead: dict = {
        "id": lead_id,
        "linkedin_url": url,
        # ── Identity ──────────────────────────────────────────────────────────
        "name": pp.get("full_name") or ident.get("full_name") or name,
        "first_name": profile.get("first_name") or first,
        "last_name": profile.get("last_name") or last,
        "work_email": pp.get("work_email") or ident.get("work_email") or contact.get("email"),
        "personal_email": pp.get("personal_email") or ident.get("personal_email"),
        "direct_phone": (
            pp.get("direct_phone") or ident.get("direct_phone")
            or contact.get("phone") or profile.get("phone_number") or profile.get("phone")
        ),
        "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter") or profile.get("twitter"),
        "city": (
            pp.get("city") or ident.get("city")
            or (profile.get("city") or "").split(",")[0].strip()
            or profile.get("location", "").split(",")[0].strip()
        ),
        "country": _country_name(
            pp.get("country") or ident.get("country")
            or profile.get("country") or profile.get("country_code", "")
        ),
        "timezone": pp.get("timezone") or ident.get("timezone"),
        # ── Professional ───────────────────────────────────────────────────────
        "title": (
            pp.get("current_title") or prof_legacy.get("current_title")
            or profile.get("position") or profile.get("headline") or profile.get("bio", "")
        ),
        "seniority_level": pp.get("seniority_level") or prof_legacy.get("seniority_level"),
        "department": pp.get("department") or prof_legacy.get("department"),
        "years_in_role": pp.get("years_in_role") or prof_legacy.get("years_in_role"),
        "company": ci.get("name") or comp_legacy.get("name") or company,
        "previous_companies": _safe_json(
            prof_legacy.get("previous_companies")
            or [e.get("company", "") for e in (pp.get("career_history") or [])[1:4] if isinstance(e, dict)]
            or [e.get("company_name", "") or e.get("company", "") for e in (profile.get("experience") or [])[1:4] if isinstance(e, dict)]
        ),
        "top_skills": _safe_json(
            pp.get("top_skills") or prof_legacy.get("top_skills")
            or profile.get("skills") or []
        ),
        "education": (
            pp.get("education") or prof_legacy.get("education")
            or profile.get("educations_details") or profile.get("education_details")
        ),
        "certifications": _safe_json(
            pp.get("certifications") or prof_legacy.get("certifications")
            or profile.get("honors_and_awards") or []
        ),
        "languages": _safe_json(pp.get("languages") or prof_legacy.get("languages") or []),
        # ── Company (from waterfall + LLM) ────────────────────────────────────
        "company_website": cp.get("website") or ci.get("website") or comp_legacy.get("website"),
        "industry": cp.get("industry") or comp_legacy.get("industry"),
        "employee_count": _safe_int(cp.get("employee_count") or comp_legacy.get("employee_count")),
        "hq_location": cp.get("hq_location") or comp_legacy.get("hq_location"),
        "founded_year": str(cp.get("founded_year") or comp_legacy.get("founded_year") or ""),
        "funding_stage": cin.get("funding_stage") or comp_legacy.get("funding_stage"),
        "total_funding": cin.get("total_funding") or comp_legacy.get("total_funding"),
        "last_funding_date": cin.get("last_funding_date") or comp_legacy.get("last_funding_date"),
        "lead_investor": cin.get("lead_investor") or comp_legacy.get("lead_investor"),
        "annual_revenue": cp.get("annual_revenue_est") or comp_legacy.get("annual_revenue_est"),
        "tech_stack": _safe_json(cp.get("tech_stack") or comp_legacy.get("tech_stack") or []),
        "hiring_velocity": cin.get("hiring_velocity") or comp_legacy.get("hiring_velocity"),
        # ── Company enrichment (waterfall) ────────────────────────────────────
        "avatar_url": _extract_avatar(profile) or contact.get("avatar_url"),
        "company_logo": cp.get("company_logo") or company_extras.get("company_logo"),
        "company_email": (
            cp.get("company_email") or company_extras.get("company_email")
            # Fall back to activity emails that match the company domain
            or next((e for e in activity_emails_raw if verified_domain and verified_domain.split(".")[0] in e), None)
        ),
        "company_description": wi.get("company_description") or company_extras.get("company_description"),
        "company_linkedin": ci.get("linkedin_url") or company_extras.get("company_linkedin") or contact.get("company_linkedin"),
        "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
        "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
        "waterfall_log": json.dumps(waterfall_log),
        # ── Stage 3 — Website Intelligence ───────────────────────────────────
        "website_intelligence": json.dumps(website_intel or {}),
        "product_offerings": _safe_json(wi.get("product_offerings")),
        "value_proposition": wi.get("value_proposition"),
        "target_customers": _safe_json(wi.get("target_customers")),
        "business_model": wi.get("business_model"),
        "pricing_signals": wi.get("pricing_signals"),
        "product_category": wi.get("product_category"),
        "data_completeness_score": int(ls.get("data_completeness_score", 0)),
        # ── Intent signals ────────────────────────────────────────────────────
        "recent_funding_event": intent_s.get("recent_funding_event"),
        # Hiring signal: LLM result OR extracted from activity posts
        "hiring_signal": (
            intent_s.get("hiring_signal")
            or (hiring_signals_raw[0] if hiring_signals_raw else None)
        ),
        "job_change": intent_s.get("job_change"),
        # LinkedIn activity: combine LLM intent + actual post snippets
        "linkedin_activity": (
            intent_s.get("linkedin_activity")
            or (_safe_json(hiring_signals_raw) if hiring_signals_raw else None)
        ),
        "news_mention": intent_s.get("news_mention"),
        "product_launch": intent_s.get("product_launch"),
        "competitor_usage": intent_s.get("competitor_usage"),
        "review_activity": (
            intent_s.get("review_activity")
            or (profile.get("recommendations_text"))
        ),
        # ── Scoring ───────────────────────────────────────────────────────────
        "icp_fit_score": _safe_int(ls.get("icp_fit_score")),
        "intent_score": _safe_int(ls.get("intent_score")),
        "timing_score": _safe_int(ls.get("timing_score")),
        "engagement_score": 0,
        "total_score": _safe_int(total),
        "score_tier": score_tier,
        "score_explanation": ls.get("score_explanation"),
        "icp_match_tier": ls.get("icp_match_tier") or tier_raw,
        "disqualification_flags": _safe_json(ls.get("disqualification_flags")),
        # ── Outreach ──────────────────────────────────────────────────────────
        "email_subject": outreach.get("email_subject"),
        "cold_email": outreach.get("cold_email"),
        "linkedin_note": outreach.get("linkedin_note"),
        "best_channel": outreach.get("best_channel"),
        "best_send_time": outreach.get("best_send_time"),
        "outreach_angle": outreach.get("outreach_angle"),
        "sequence_type": outreach.get("sequence_type"),
        "outreach_sequence": _safe_json(outreach.get("sequence")),
        "last_contacted": outreach.get("last_contacted"),
        "email_status": outreach.get("email_status"),
        # ── CRM ───────────────────────────────────────────────────────────────
        "lead_source": crm.get("lead_source", "LinkedIn URL"),
        "enrichment_source": crm.get("enrichment_source", "Bright Data + Apollo"),
        "data_completeness": int(crm.get("data_completeness") or 0),
        "crm_stage": crm.get("crm_stage"),
        "tags": _safe_json(crm.get("tags")),
        "assigned_owner": crm.get("assigned_owner"),
        # ── Meta ──────────────────────────────────────────────────────────────
        "full_data": json.dumps({
            "person_profile": enrichment.get("person_profile", {}),
            "company_identity": enrichment.get("company_identity", {}),
            "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
            "company_profile": enrichment.get("company_profile", {}),
            "company_intelligence": enrichment.get("company_intelligence", {}),
            "intent_signals": enrichment.get("intent_signals", {}),
            "lead_scoring": enrichment.get("lead_scoring", {}),
            "outreach": enrichment.get("outreach", {}),
            # Raw BD fields preserved
            "activity_emails": activity_emails_raw,
            "activity_phones": profile.get("_activity_phones") or [],
            "activity_full": profile.get("_activity_full") or [],
            "linkedin_posts": profile.get("_posts") or [],
            "hiring_signals": hiring_signals_raw,
            "recommendations": profile.get("recommendations") or [],
            "recommendations_count": profile.get("recommendations_count") or 0,
            "similar_profiles": profile.get("_similar_profiles") or [],
            "banner_image": profile.get("banner_image") or "",
            "linkedin_num_id": profile.get("linkedin_num_id") or "",
            "influencer": profile.get("_influencer") or False,
            "memorialized_account": profile.get("_memorialized_account") or False,
            "bio_links": profile.get("_bio_links") or [],
            "bd_scrape_timestamp": profile.get("_bd_scrape_timestamp") or "",
            # Legacy compat
            "identity": enrichment.get("identity", {}),
            "professional": enrichment.get("professional", {}),
            "company": enrichment.get("company", {}),
            "scoring": enrichment.get("scoring", {}),
        }, default=str),
        "raw_profile": json.dumps(profile, default=str),
        "crm_brief": json.dumps(enrichment.get("crm_brief"), default=str) if enrichment.get("crm_brief") is not None else None,
        "apollo_raw": json.dumps(contact.get("_apollo_raw"), default=str) if contact.get("_apollo_raw") else None,
        "about": (profile.get("about") or "")[:1000],
        "followers": _safe_int(profile.get("followers")),
        "connections": _safe_int(profile.get("connections")),
        "email_source": contact.get("source"),
        "email_confidence": contact.get("confidence"),
        "email_verified": 1 if contact.get("verified") else 0,
        "bounce_risk": contact.get("bounce_risk"),
        # P1 — Full activity feed stored as dedicated column for AI analysis
        "activity_feed": json.dumps(profile.get("_activity_full") or []),
        # P3 — AI Analysis layer (populated after _run_ai_analysis call below)
        "auto_tags":           _ai.get("auto_tags_json"),
        "behavioural_signals": _ai.get("behavioural_signals_json"),
        "pitch_intelligence":  _ai.get("pitch_intelligence_json"),
        "warm_signal":         _ai.get("warm_signal"),
        # ── Company enrichment (P1/P5) — flat columns mirrored from company_enrichments ─
        "company_id":        _company_id_val,
        "company_score":     _company_score,
        "combined_score":    _combined_score,
        "company_score_tier": _company_record.get("company_score_tier") or "",
        "company_tags":      _safe_json(_company_record.get("company_tags") or []),
        "culture_signals":   _safe_json(_company_record.get("culture_signals") or {}),
        "account_pitch":     _safe_json(_company_record.get("account_pitch") or {}),
        "wappalyzer_tech":   _safe_json(_company_record.get("wappalyzer_tech") or []),
        "news_mentions":     _safe_json(_company_record.get("news_mentions") or []),
        "crunchbase_data":   _safe_json(_company_record.get("crunchbase_data") or {}),
        "linkedin_posts":    _safe_json(_company_record.get("linkedin_posts") or []),
        "status": "enriched",
        "job_id": job_id,
        "organization_id": org_id,
        "enriched_at": now,
    }

    # P5: Compute combined_score after we have both total_score and company_score
    try:
        from company_service import compute_combined_score
        lead["combined_score"] = compute_combined_score(
            person_score=int(lead.get("total_score") or 0),
            company_score=_company_score,
        )
    except Exception:
        lead["combined_score"] = int(lead.get("total_score") or 0)

    await _upsert_lead(lead)
    logger.info(
        "[Enrich] Done: %s | score=%s (%s) | email=%s | completeness=%s%% | category=%s",
        lead["name"] or url, total, score_tier,
        lead["work_email"] or "not found", lead["data_completeness"],
        lead["product_category"] or "?",
    )

    # Embed structured LinkedIn Enrich view for immediate use by caller / LIO
    lead["_cache_hit"] = False
    lead["linkedin_enrich"] = _format_linkedin_enrich(lead, include_contact=not skip_contact)

    # If crm_brief generation failed or empty — regenerate in background
    _saved_brief_single = _parse_json_safe(lead.get("crm_brief"), None)
    if not _validate_crm_brief(_saved_brief_single) and lead.get("name"):
        logger.warning("[Enrich] crm_brief missing/invalid for %s — scheduling background regeneration", url)
        asyncio.create_task(regenerate_crm_brief_for_lead(lead_id, org_id=org_id))

    # Forward to LIO — only if BrightData returned real data (lead has a name)
    if not forward_to_lio and lead.get("name"):
        asyncio.create_task(send_to_lio(lead, sso_id=sso_id))

    # Release in-flight lock — wake up all waiters for this URL
    _in_flight_leads.pop(lead_id, None)
    _in_flight_event.set()

    return lead


# ─────────────────────────────────────────────────────────────────────────────
# SSE streaming enrichment (6 stages)
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_single_stream(
    linkedin_url: str,
    org_id: str = "default",
) -> AsyncGenerator[dict, None]:
    """
    Async generator that yields SSE events after each enrichment stage.

    Stage 1 — profile:   Bright Data person scrape
    Stage 2 — company:   Company waterfall (BD + Apollo + Clearbit)
    Stage 3 — contact:   Email / phone waterfall (Apollo → Dropcontact → PDL → pattern)
    Stage 4 — website:   Website intelligence scrape + LLM extraction
    Stage 5 — scoring:   Comprehensive LLM scoring + outreach generation
    Stage 6 — complete:  Save to DB, return final lead object

    Each stage yields {"stage": <name>, "status": "loading"} first,
    then {"stage": <name>, "status": "done", "data": {...}} when finished.
    On error: {"stage": <name>, "status": "error", "error": <msg>}
    """
    url = _normalize_linkedin_url(linkedin_url)
    lead_id = _lead_id(url)

    # ── Company URL: proxy to company pipeline, stream synthetic events ───────
    if _is_company_url(url):
        yield {"stage": "profile",  "status": "loading"}
        yield {"stage": "company",  "status": "loading"}
        yield {"stage": "contact",  "status": "loading"}
        yield {"stage": "website",  "status": "loading"}
        yield {"stage": "scoring",  "status": "loading"}
        try:
            lead = await enrich_company_url(url, org_id=org_id, forward_to_lio=True)
            yield {"stage": "profile",  "status": "done", "data": {
                "name": lead.get("name", ""), "title": lead.get("title", ""),
                "company": lead.get("company", ""), "location": lead.get("location", ""),
                "avatar_url": lead.get("avatar_url", ""),
                "followers": lead.get("followers", 0),
            }}
            yield {"stage": "company",  "status": "done", "data": {
                "company_logo": lead.get("company_logo"),
                "company_website": lead.get("company_website"),
                "industry": lead.get("industry"),
                "employee_count": lead.get("employee_count", 0),
            }}
            _co_email = lead.get("work_email") or ""
            if "placeholder" in _co_email.lower():
                _co_email = ""
            yield {"stage": "contact",  "status": "done", "data": {
                "email": _co_email or None, "phone": lead.get("direct_phone"),
                "email_source": lead.get("email_source"), "email_confidence": lead.get("email_confidence"),
            }}
            yield {"stage": "website",  "status": "done", "data": {
                "value_proposition": lead.get("value_proposition"),
                "product_offerings": json.loads(lead.get("product_offerings") or "[]"),
                "tech_stack": json.loads(lead.get("tech_stack") or "[]"),
                "business_model": lead.get("business_model"),
            }}
            yield {"stage": "scoring",  "status": "done", "data": {
                "total_score": lead.get("total_score", 0), "score_tier": lead.get("score_tier"),
                "icp_fit_score": lead.get("icp_fit_score", 0), "intent_score": lead.get("intent_score", 0),
                "cold_email": lead.get("cold_email"), "linkedin_note": lead.get("linkedin_note"),
            }}
            yield {"stage": "complete", "status": "done", "lead": lead}
        except Exception as e:
            yield {"stage": "complete", "status": "error", "error": str(e)}
        return

    # ── Stage 1: Person Profile ───────────────────────────────────────────────
    yield {"stage": "profile", "status": "loading"}
    try:
        profile = await fetch_profile_sync(url)
        # Bright Data account/auth error — abort without saving incomplete data
        if profile.get("_bd_error"):
            err_msg = profile.get("_bd_message", "Bright Data account error")
            logger.error("[Stream] Aborting — BD account error: %s", err_msg)
            yield {"stage": "profile", "status": "error", "error": err_msg}
            yield {"stage": "complete", "status": "error", "error": f"Bright Data account error: {err_msg}"}
            return
        name     = profile.get("name", "")
        first, last = _parse_name(name)
        company  = profile.get("current_company_name", "")
        raw_link = profile.get("current_company_link", "")
        yield {"stage": "profile", "status": "done", "data": {
            "name":       name,
            "title":      profile.get("position") or profile.get("headline", ""),
            "company":    company,
            "location":   profile.get("location") or profile.get("city", ""),
            "avatar_url": profile.get("avatar_url") or profile.get("avatar", ""),
            "followers":  _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "about":      (profile.get("about") or "")[:300],
            "banner_image": profile.get("banner_image", ""),
        }}
    except Exception as e:
        yield {"stage": "profile", "status": "error", "error": str(e)}
        yield {"stage": "complete", "status": "error", "error": f"Profile fetch failed: {e}"}
        return

    # ── Stage 2: Company Waterfall ────────────────────────────────────────────
    yield {"stage": "company", "status": "loading"}
    try:
        initial_domain = _extract_domain(company, raw_link)
        company_extras, company_wf_log = await enrich_company_waterfall(company, initial_domain, profile, lead_id=lead_id)
        verified_domain = company_extras.pop("_verified_domain", initial_domain) or initial_domain
        real_website = company_extras.get("company_website") or (f"https://{verified_domain}" if verified_domain else "")
        yield {"stage": "company", "status": "done", "data": {
            "company_logo":    company_extras.get("company_logo"),
            "company_website": real_website,
            "industry":        company_extras.get("industry"),
            "employee_count":  company_extras.get("employee_count", 0),
            "hq_location":     company_extras.get("hq_location"),
            "tech_stack":      company_extras.get("tech_stack", []),
            "founded_year":    company_extras.get("founded_year"),
            "funding_stage":   company_extras.get("funding_stage"),
        }}
    except Exception as e:
        yield {"stage": "company", "status": "error", "error": str(e)}
        company_extras, company_wf_log = {}, []
        verified_domain = _extract_domain(company, raw_link)
        real_website = ""

    # ── Stage 3: Contact Waterfall ────────────────────────────────────────────
    yield {"stage": "contact", "status": "loading"}
    try:
        activity_emails = profile.get("_activity_emails") or []
        activity_phones = profile.get("_activity_phones") or []
        if activity_emails:
            matched = next((e for e in activity_emails if verified_domain and verified_domain.split(".")[0] in e), None)
            pre_contact = matched or activity_emails[0]
            contact = {"email": pre_contact, "source": "linkedin_activity", "confidence": "high"}
            if activity_phones:
                contact["phone"] = activity_phones[0]
        else:
            contact = await find_contact_info(first, last, verified_domain, linkedin_url=url)
            if not contact.get("phone") and activity_phones:
                contact["phone"] = activity_phones[0]
        _contact_email = contact.get("email") or ""
        if "placeholder" in _contact_email.lower():
            logger.warning("[Stream] Dropping placeholder email from contact stage: %s", _contact_email)
            _contact_email = ""
        yield {"stage": "contact", "status": "done", "data": {
            "email":            _contact_email or None,
            "phone":            contact.get("phone"),
            "email_source":     contact.get("source"),
            "email_confidence": contact.get("confidence"),
        }}
    except Exception as e:
        yield {"stage": "contact", "status": "error", "error": str(e)}
        contact = {"email": None, "phone": None, "source": None, "confidence": None}

    # ── Stage 4: Website Intelligence ────────────────────────────────────────
    yield {"stage": "website", "status": "loading"}
    try:
        website_intel = await scrape_website_intelligence(real_website)
        yield {"stage": "website", "status": "done", "data": {
            "value_proposition":   website_intel.get("value_proposition"),
            "product_offerings":   website_intel.get("product_offerings", []),
            "tech_stack_clues":    website_intel.get("tech_stack_clues", []),
            "business_model":      website_intel.get("business_model"),
            "product_category":    website_intel.get("product_category"),
            "market_positioning":  website_intel.get("market_positioning"),
            "pricing_signals":     website_intel.get("pricing_signals"),
            "target_customers":    website_intel.get("target_customers", []),
            "problem_solved":      website_intel.get("problem_solved"),
            "hiring_signals":      website_intel.get("hiring_signals"),
            "pages_scraped":       website_intel.get("pages_scraped", []),
        }}
    except Exception as e:
        yield {"stage": "website", "status": "error", "error": str(e)}
        website_intel = {}

    # ── Stage 5: LLM Scoring + Outreach ──────────────────────────────────────
    yield {"stage": "scoring", "status": "loading"}
    try:
        profile["_company_extras"] = company_extras
        if website_intel and website_intel.get("pages_scraped"):
            company_wf_log.append({
                "step": 5, "source": "Website Scrape",
                "fields_found": website_intel.get("pages_scraped", []),
                "note": f"pages={len(website_intel.get('pages_scraped', []))}, category={website_intel.get('product_category', '?')}",
            })
        enrichment = await build_comprehensive_enrichment(url, profile, contact, website_intel=website_intel, org_id=org_id)
        scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        total = scoring.get("overall_score", 0)
        tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
        score_tier = _resolve_tier(tier_raw)
        outreach = enrichment.get("outreach", {})
        yield {"stage": "scoring", "status": "done", "data": {
            "total_score":  int(total),
            "score_tier":   score_tier,
            "icp_fit_score":  int(scoring.get("icp_fit_score", 0)),
            "intent_score":   int(scoring.get("intent_score", 0)),
            "timing_score":   int(scoring.get("timing_score", 0)),
            "icp_match_tier": scoring.get("icp_match_tier") or tier_raw,
            "score_explanation": scoring.get("score_explanation"),
            "cold_email":     outreach.get("cold_email"),
            "linkedin_note":  outreach.get("linkedin_note"),
            "email_subject":  outreach.get("email_subject"),
            "best_channel":   outreach.get("best_channel"),
            "outreach_angle": outreach.get("outreach_angle"),
        }}
    except Exception as e:
        yield {"stage": "scoring", "status": "error", "error": str(e)}
        enrichment = {}
        scoring = {}
        total = 0
        score_tier = "cold"
        outreach = {}

    # ── Stage 6: Save + Complete ──────────────────────────────────────────────
    try:
        # Build full lead record (same logic as enrich_single)
        person_prof = enrichment.get("person_profile", {})
        ident = enrichment.get("identity", {})
        if contact.get("email"):
            person_prof["work_email"] = contact["email"]
            ident["work_email"] = contact["email"]
        if contact.get("phone"):
            person_prof["direct_phone"] = contact["phone"]
            ident["direct_phone"] = contact["phone"]

        comp_prof = enrichment.get("company_profile", {})
        comp_section = enrichment.get("company", {})
        for k, v in company_extras.items():
            _map = {
                "company_logo": "company_logo", "company_email": "company_email",
                "company_phone": "company_phone", "company_twitter": "company_twitter",
                "industry": "industry", "employee_count": "employee_count",
                "hq_location": "hq_location", "founded_year": "founded_year",
                "annual_revenue": "annual_revenue_est", "tech_stack": "tech_stack",
            }
            prof_key = _map.get(k)
            if prof_key and v and not comp_prof.get(prof_key):
                comp_prof[prof_key] = v

        scoring_f = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        crm = enrichment.get("crm", {})
        wi = enrichment.get("website_intelligence", website_intel or {})
        intent_s = enrichment.get("intent_signals", {})
        ci = enrichment.get("company_identity", {})
        cp = enrichment.get("company_profile", comp_prof)
        cin = enrichment.get("company_intelligence", {})
        pp = enrichment.get("person_profile", {})
        hiring_signals_raw = profile.get("_hiring_signals") or []
        activity_emails_raw = profile.get("_activity_emails") or []
        now = datetime.now(timezone.utc).isoformat()

        waterfall_log = [
            {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
            *company_wf_log,
            {"step": 10, "source": contact.get("source") or "none",
             "fields_found": ["email"], "note": f"email={contact.get('email')}"},
        ]

        lead: dict = {
            "id": lead_id, "linkedin_url": url,
            "name": pp.get("full_name") or ident.get("full_name") or profile.get("name", ""),
            "first_name": profile.get("first_name") or first,
            "last_name": profile.get("last_name") or last,
            "work_email": pp.get("work_email") or ident.get("work_email") or contact.get("email"),
            "personal_email": pp.get("personal_email") or ident.get("personal_email"),
            "direct_phone": (pp.get("direct_phone") or ident.get("direct_phone")
                             or contact.get("phone") or profile.get("phone_number") or profile.get("phone")),
            "twitter": pp.get("twitter") or ident.get("twitter") or contact.get("twitter") or profile.get("twitter"),
            "city": (pp.get("city") or ident.get("city")
                     or (profile.get("city") or "").split(",")[0].strip()
                     or profile.get("location", "").split(",")[0].strip()),
            "country": _country_name(
                pp.get("country") or ident.get("country")
                or profile.get("country") or profile.get("country_code", "")
            ),
            "avatar_url": _extract_avatar(profile) or contact.get("avatar_url") or "",
            "title": (pp.get("current_title") or ident.get("current_title")
                      or profile.get("position") or profile.get("headline", "")),
            "seniority_level": pp.get("seniority_level") or ident.get("seniority_level"),
            "department": pp.get("department") or ident.get("department"),
            "company": (cp.get("name") or ci.get("name") or comp_section.get("name") or
                        comp_prof.get("name") or profile.get("current_company_name", "")),
            "company_website": (cp.get("website") or ci.get("website") or comp_section.get("website") or
                                real_website or company_extras.get("company_website")),
            "company_logo": (cp.get("company_logo") or comp_section.get("logo") or
                             company_extras.get("company_logo") or profile.get("current_company_logo")),
            "company_linkedin": (ci.get("linkedin_url") or profile.get("current_company_link") or
                                 company_extras.get("company_linkedin")),
            "company_description": (cp.get("company_description") or comp_section.get("description") or
                                    company_extras.get("company_description") or wi.get("company_description")),
            "industry": (cp.get("industry") or comp_section.get("industry") or company_extras.get("industry")),
            "employee_count": _safe_int(cp.get("employee_count") or company_extras.get("employee_count")),
            "hq_location": (cp.get("hq_location") or comp_section.get("hq_location") or
                            company_extras.get("hq_location")),
            "founded_year": (cp.get("founded_year") or comp_section.get("founded_year") or
                             company_extras.get("founded_year")),
            "annual_revenue": (cp.get("annual_revenue_est") or comp_section.get("annual_revenue_est") or
                               company_extras.get("annual_revenue")),
            "tech_stack": _safe_json(cp.get("tech_stack") or company_extras.get("tech_stack") or []),
            "company_email": cp.get("company_email") or company_extras.get("company_email"),
            "company_phone": cp.get("company_phone") or company_extras.get("company_phone"),
            "company_twitter": cp.get("company_twitter") or company_extras.get("company_twitter"),
            "top_skills": _safe_json(pp.get("top_skills") or profile.get("skills") or []),
            "education": _safe_json(profile.get("education") or []),
            "certifications": _safe_json(pp.get("certifications") or profile.get("certifications") or []),
            "languages": _safe_json(pp.get("languages") or profile.get("languages") or []),
            "value_proposition": wi.get("value_proposition"),
            "product_offerings": _safe_json(wi.get("product_offerings")),
            "target_customers": _safe_json(wi.get("target_customers")),
            "business_model": wi.get("business_model"),
            "pricing_signals": wi.get("pricing_signals"),
            "product_category": wi.get("product_category"),
            "data_completeness_score": _safe_int(scoring_f.get("data_completeness_score")),
            "recent_funding_event": intent_s.get("recent_funding_event"),
            "hiring_signal": intent_s.get("hiring_signal") or (hiring_signals_raw[0] if hiring_signals_raw else None),
            "job_change": intent_s.get("job_change"),
            "linkedin_activity": intent_s.get("linkedin_activity") or (_safe_json(hiring_signals_raw) if hiring_signals_raw else None),
            "news_mention": intent_s.get("news_mention"),
            "product_launch": intent_s.get("product_launch"),
            "competitor_usage": intent_s.get("competitor_usage"),
            "review_activity": intent_s.get("review_activity") or profile.get("recommendations_text"),
            "icp_fit_score": _safe_int(scoring_f.get("icp_fit_score")),
            "intent_score": _safe_int(scoring_f.get("intent_score")),
            "timing_score": _safe_int(scoring_f.get("timing_score")),
            "engagement_score": 0,
            "total_score": _safe_int(total),
            "score_tier": score_tier,
            "score_explanation": scoring_f.get("score_explanation"),
            "icp_match_tier": scoring_f.get("icp_match_tier") or tier_raw,
            "disqualification_flags": _safe_json(scoring_f.get("disqualification_flags")),
            "email_subject": outreach.get("email_subject"),
            "cold_email": outreach.get("cold_email"),
            "linkedin_note": outreach.get("linkedin_note"),
            "best_channel": outreach.get("best_channel"),
            "best_send_time": outreach.get("best_send_time"),
            "outreach_angle": outreach.get("outreach_angle"),
            "sequence_type": outreach.get("sequence_type"),
            "outreach_sequence": _safe_json(outreach.get("sequence")),
            "last_contacted": outreach.get("last_contacted"),
            "email_status": outreach.get("email_status"),
            "lead_source": crm.get("lead_source", "LinkedIn URL"),
            "enrichment_source": crm.get("enrichment_source", "Bright Data + Apollo"),
            "data_completeness": _safe_int(crm.get("data_completeness")),
            "crm_stage": crm.get("crm_stage"),
            "tags": _safe_json(crm.get("tags")),
            "assigned_owner": crm.get("assigned_owner"),
            "full_data": json.dumps({
                "person_profile": enrichment.get("person_profile", {}),
                "company_identity": enrichment.get("company_identity", {}),
                "website_intelligence": enrichment.get("website_intelligence", website_intel or {}),
                "company_profile": enrichment.get("company_profile", {}),
                "company_intelligence": enrichment.get("company_intelligence", {}),
                "intent_signals": enrichment.get("intent_signals", {}),
                "lead_scoring": enrichment.get("lead_scoring", {}),
                "outreach": enrichment.get("outreach", {}),
                # Profile-level data needed for UI sections
                "recommendations": profile.get("recommendations") or [],
                "recommendations_count": profile.get("recommendations_count") or 0,
                "similar_profiles": profile.get("_similar_profiles") or [],
                "banner_image": profile.get("banner_image") or "",
                "linkedin_num_id": profile.get("linkedin_num_id") or "",
                "influencer": profile.get("_influencer") or False,
                "memorialized_account": profile.get("_memorialized_account") or False,
                "bio_links": profile.get("_bio_links") or [],
                "bd_scrape_timestamp": profile.get("_bd_scrape_timestamp") or "",
                "activity_emails": activity_emails_raw,
                "activity_phones": profile.get("_activity_phones") or [],
                "activity_full": profile.get("_activity_full") or [],
                "linkedin_posts": profile.get("_posts") or [],
                "hiring_signals": hiring_signals_raw,
                "waterfall_log": waterfall_log,
            }, default=str),
            "raw_profile": json.dumps(profile, default=str),
            "about": (profile.get("about") or "")[:1000],
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "email_source": contact.get("source"),
            "email_confidence": contact.get("confidence"),
            "status": "enriched",
            "job_id": None,
            "organization_id": org_id,
            "enriched_at": now,
        }
        await _upsert_lead(lead)
        yield {"stage": "complete", "status": "done", "lead": lead}
    except Exception as e:
        logger.error("[StreamEnrich] Save failed: %s", e, exc_info=True)
        yield {"stage": "complete", "status": "error", "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Bulk pipeline
# ─────────────────────────────────────────────────────────────────────────────


async def _poll_and_process_snapshot(
    snapshot_id: str, job_id: str, org_id: str, sub_job_id: Optional[str] = None
) -> None:
    """Poll BrightData for snapshot results and process them (used when no webhook URL is set)."""
    _pipeline_log.info("[job=%s] [snapshot=%s] POLL_START — waiting for BrightData (interval=15s timeout=1800s)", (job_id or "")[-8:], snapshot_id)
    try:
        logger.info("[BulkPoll] Polling snapshot %s for job %s (sub_job=%s)", snapshot_id, job_id, sub_job_id or "—")
        profiles = await poll_snapshot(snapshot_id, interval=3, timeout=1800)
        _pipeline_log.info("[job=%s] [snapshot=%s] POLL_READY — %d profiles received", (job_id or "")[-8:], snapshot_id, len(profiles))
        logger.info("[BulkPoll] Snapshot %s ready — %d profiles, processing…", snapshot_id, len(profiles))
        result = await process_webhook_profiles(profiles, job_id=job_id, sub_job_id=sub_job_id)
        _pipeline_log.info("[job=%s] [snapshot=%s] POLL_DONE — processed=%d failed=%d", (job_id or "")[-8:], snapshot_id, result["processed"], result["failed"])
        logger.info("[BulkPoll] Done: processed=%d failed=%d", result["processed"], result["failed"])
    except Exception as e:
        _pipeline_log.error("[job=%s] [snapshot=%s] POLL_ERROR — %s", (job_id or "")[-8:], snapshot_id, e)
        logger.error("[BulkPoll] snapshot %s failed: %s", snapshot_id, e)
        if sub_job_id:
            await _update_sub_job(sub_job_id, status="failed")
        await _update_job(job_id, status="failed", error=str(e))


def _bd_chunk_size(total: int) -> int:
    """
    1 URL per chunk — each lead gets its own BrightData snapshot for granular progress.
    Max 200 leads per job, so max 200 parallel snapshots.
    """
    return 1


async def enrich_bulk(
    urls: list[str],
    webhook_url: Optional[str] = None,
    notify_url: Optional[str] = None,
    webhook_auth: Optional[str] = None,
    org_id: str = "default",
    sso_id: str = "",
    forward_to_lio: bool = False,
    system_prompt: Optional[str] = None,
) -> dict:
    """
    Bulk enrichment pipeline — BrightData webhook only.

    Triggers a BrightData batch snapshot for each URL.
    Results arrive via POST /api/leads/webhook/brightdata when scraping completes.
    """
    job_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc)
    snapshot_id = None
    status = "pending"
    error_msg = None

    # Base app URL — used to build per-chunk webhook URLs
    app_url = os.getenv("APP_URL", "").rstrip("/")

    # Insert the parent job row FIRST so FK constraints on enrichment_sub_jobs are satisfied
    job = {
        "id": job_id, "snapshot_id": None,
        "total_urls": len(urls), "processed": 0, "failed": 0,
        "status": "pending", "error": None,
        "webhook_url": webhook_url,
        "organization_id": org_id,
        "sso_id": sso_id,
        "forward_to_lio": forward_to_lio,
        "created_at": now, "updated_at": now,
    }
    _sql, _args = named_args(
        """INSERT INTO enrichment_jobs
           (id,snapshot_id,total_urls,processed,failed,status,error,webhook_url,organization_id,sso_id,forward_to_lio,created_at,updated_at)
           VALUES (:id,:snapshot_id,:total_urls,:processed,:failed,:status,:error,:webhook_url,:organization_id,:sso_id,:forward_to_lio,:created_at,:updated_at)""",
        job,
    )
    async with get_pool().acquire() as conn:
        await conn.execute(_sql, *_args)

    # Trigger BrightData — 1 URL per sub-job, each gets its own snapshot + webhook
    # Semaphore limits concurrent BD API calls to avoid rate-limiting (max 10 at a time)
    if _bd_api_key():
        try:
            # 1 URL per chunk — 100 URLs = 100 sub-jobs = 100 snapshots
            url_chunks  = [[u] for u in urls]
            sub_job_ids = [str(uuid.uuid4()) for _ in url_chunks]
            total_chunks = len(url_chunks)

            async def _trigger_bd_chunk(idx: int, sjid: str, chunk: list) -> str:
                async with _bd_trigger_semaphore:  # global cap across all orgs
                    if app_url:
                        # BD sends full data to chunk_webhook — no need for notify
                        chunk_webhook = f"{app_url}/api/leads/webhook/brightdata?job_id={job_id}&sub_job_id={sjid}"
                        chunk_notify  = None
                    elif webhook_url:
                        sep = "&" if "?" in webhook_url else "?"
                        chunk_webhook = f"{webhook_url}{sep}sub_job_id={sjid}"
                        chunk_notify  = notify_url
                    else:
                        chunk_webhook = None
                        chunk_notify  = None
                    snap_id = await trigger_batch_snapshot(chunk, chunk_webhook, chunk_notify, webhook_auth)
                    await _create_sub_job(sjid, job_id, idx, len(chunk), org_id, snapshot_id=snap_id)
                    if not chunk_webhook:
                        asyncio.create_task(_poll_and_process_snapshot(snap_id, job_id, org_id, sub_job_id=sjid))
                    logger.info(
                        "[BulkEnrich] Sub-job %d/%d | url=%s → snapshot=%s (webhook=%s)",
                        idx + 1, total_chunks, chunk[0], snap_id, chunk_webhook or "polling",
                    )
                    return snap_id

            snap_ids    = await asyncio.gather(*[_trigger_bd_chunk(i, sjid, chunk) for i, (sjid, chunk) in enumerate(zip(sub_job_ids, url_chunks))])
            snapshot_id = snap_ids[0] if snap_ids else None  # store first on parent job for compat
            # Set webhook_url on parent job row so stop/rerun routes can find it
            if not webhook_url and app_url:
                webhook_url = f"{app_url}/api/leads/webhook/brightdata?job_id={job_id}"
            status = "running"
            logger.info(
                "[BulkEnrich] %d URLs → %d sub-jobs triggered for job %s (org=%s)",
                len(urls), total_chunks, job_id, org_id,
            )
        except Exception as e:
            logger.error("[BulkEnrich] BD chunk trigger failed: %s", e)
            error_msg = str(e)

    # Update parent job with results from BD trigger (snapshot_id, status, error, webhook_url)
    await _update_job(job_id, snapshot_id=snapshot_id, status=status, error=error_msg, webhook_url=webhook_url)
    job = {
        "id": job_id, "snapshot_id": snapshot_id,
        "total_urls": len(urls), "processed": 0, "failed": 0,
        "status": status, "error": error_msg,
        "webhook_url": webhook_url,
        "organization_id": org_id,
        "created_at": now, "updated_at": now,
    }

    return job


async def _process_one_webhook_profile(profile: dict, job_id: Optional[str], org_id: str, sub_job_id: Optional[str] = None, sso_id: str = "") -> Optional[dict]:
    """
    Process a single BrightData profile through the full enrichment pipeline.
    Returns the enriched lead dict on success, None on failure.
    Designed to be called concurrently via asyncio.gather.
    """
    # BrightData error responses nest the URL under {"input": {"url": "..."}} instead of top-level
    _bd_input = profile.get("input") or {}
    raw_url = (
        profile.get("input_url")
        or profile.get("url")
        or profile.get("linkedin_url")
        or (_bd_input.get("url") if isinstance(_bd_input, dict) else None)
    )
    if not raw_url:
        _pipeline_log.warning("[job=%s] PROFILE_SKIP — no URL in profile keys=%s", (job_id or "")[-8:], list(profile.keys())[:10])
        return None
    try:
        url = _normalize_linkedin_url(_clean_bd_linkedin_url(raw_url))
        _plog(job_id, url, "START", f"profile received from BD (keys={list(profile.keys())[:8]})")

        # ── Data validation ───────────────────────────────────────────────────────
        # A usable profile must have at least a name (or first+last) and must not
        # carry an explicit BD error flag. Partial profiles (no position/company)
        # are accepted and enriched further downstream.
        def _has_data(p: dict) -> bool:
            if p.get("error") or p.get("_bd_error") or p.get("_error"):
                return False
            has_name = bool(
                p.get("name") or p.get("full_name")
                or (p.get("first_name") and p.get("last_name"))
            )
            if not has_name:
                missing = [f for f in ("name", "full_name", "first_name") if not p.get(f)]
                logger.debug("[Validation] Profile missing identity fields: %s", missing)
                return False
            return True

        def _is_private_profile(p: dict) -> bool:
            """Detect BrightData private/hidden profile responses — no retry needed."""
            # BrightData may return is_private as a boolean field
            if p.get("is_private"):
                return True
            _PRIVATE_SIGNALS = ("hidden or private", "profile is private", "private profile", "profile not found")
            for _field in ("error", "_error", "_bd_message", "message", "about", "name"):
                _val = str(p.get(_field) or "").lower()
                if any(s in _val for s in _PRIVATE_SIGNALS):
                    return True
            return False

        # ── Private profile — fail immediately, no retries ────────────────────────
        if _is_private_profile(profile):
            _err_msg = profile.get("error") or profile.get("_bd_message") or "Profile is private or hidden"
            logger.warning("[Pipeline] Private/hidden profile for %s — marking failed, no retry", url)
            _plog(job_id, url, "FAILED", f"Private profile: {_err_msg}", "error")
            _lead_id_val = _lead_id(url)
            try:
                await _upsert_lead({
                    "id": _lead_id_val,
                    "linkedin_url": url,
                    "status": "failed",
                    "job_id": job_id,
                    "organization_id": org_id,
                    "score_explanation": "Profile is private or hidden on LinkedIn — BrightData cannot access it.",
                    "enriched_at": datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                pass
            asyncio.create_task(send_to_lio_failed(url, org_id, sso_id, reason="private_profile", error_message=_err_msg, profile_data=profile))
            return None

        # ── Retry if BrightData returned empty/partial/error profile ─────────────
        # Up to 3 re-scrape attempts with exponential back-off (2s, 4s, 8s).
        if not _has_data(profile):
            if profile.get("error") or profile.get("_error"):
                logger.warning("[Pipeline] BD returned error for %s: %s", url,
                               profile.get("error") or profile.get("_error"))
            for _attempt in range(3):
                from ._clients import _bd_backoff
                wait = _bd_backoff(_attempt)
                logger.warning("[Pipeline] Empty BD profile for %s — retry %d/3 in %.1fs", url, _attempt + 1, wait)
                await asyncio.sleep(wait)
                retried = await fetch_profile_sync(url)
                if retried and _has_data(retried):
                    profile = retried
                    logger.info("[Pipeline] Retry %d succeeded for %s", _attempt + 1, url)
                    break
                # Check if retry itself returned a private profile
                if retried and _is_private_profile(retried):
                    logger.warning("[Pipeline] Retry confirmed private profile for %s — stopping retries", url)
                    _err_msg_retry = retried.get("error") or retried.get("_bd_message") or "Profile is private or hidden"
                    _lead_id_val = _lead_id(url)
                    try:
                        await _upsert_lead({
                            "id": _lead_id_val,
                            "linkedin_url": url,
                            "status": "failed",
                            "job_id": job_id,
                            "organization_id": org_id,
                            "score_explanation": "Profile is private or hidden on LinkedIn — BrightData cannot access it.",
                            "enriched_at": datetime.now(timezone.utc).isoformat(),
                        })
                    except Exception:
                        pass
                    asyncio.create_task(send_to_lio_failed(url, org_id, sso_id, reason="private_profile", error_message=_err_msg_retry, profile_data=retried))
                    return None
            else:
                _plog(job_id, url, "VALIDATION", "all 3 retries returned empty/invalid profile — skipping", "error")
                logger.error("[Pipeline] All 3 retries returned empty/invalid profile for %s — skipping", url)
                asyncio.create_task(send_to_lio_failed(url, org_id, sso_id, reason="empty_after_retries"))
                return None

        name = profile.get("name", "")
        first, last = _parse_name(name)
        company = profile.get("current_company_name", "")
        raw_link = profile.get("current_company_link", "")
        domain = _extract_domain(company, raw_link)
        lead_id = _lead_id(url)

        # ── Stage 1: SCRAPING — BrightData profile received, save basic data ──
        await _upsert_lead({
            "id": lead_id,
            "linkedin_url": url,
            "name": name,
            "first_name": first,
            "last_name": last,
            "title": profile.get("position", ""),
            "company": company,
            "avatar_url": _extract_avatar(profile),
            "raw_brightdata": json.dumps(profile, default=str),
            "about": (profile.get("about") or "")[:1000],
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "status": "scraping",
            "job_id": job_id,
            "organization_id": org_id,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        })
        _plog(job_id, url, "SCRAPING", f"saved to DB — name={name!r} company={company!r}")
        logger.info("[Pipeline] %s → scraping", url)

        # Mark sub_job as scraped (yellow) — BrightData data is in DB, LLM not started yet
        if sub_job_id:
            try:
                await _update_sub_job(sub_job_id, status="scraped")
            except Exception:
                pass

        # ── Stage 2: ENRICHING — LLM only ────────────────────────────────────────
        async with get_pool().acquire() as _conn:
            await _conn.execute(
                "UPDATE enriched_leads SET status='enriching', updated_at=NOW() WHERE id=$1",
                lead_id,
            )
        _plog(job_id, url, "ENRICHING", "sending to LLM")
        logger.info("[Pipeline] %s → enriching", url)

        enrichment = await build_comprehensive_enrichment(url, profile, {}, website_intel={}, org_id=org_id)

        scoring = enrichment.get("lead_scoring", enrichment.get("scoring", {}))
        tier_raw = scoring.get("icp_match_tier") or scoring.get("score_tier", "")
        score_tier = _resolve_tier(tier_raw)
        now = datetime.now(timezone.utc).isoformat()

        pp = enrichment.get("person_profile", {})
        ident = enrichment.get("identity", {})
        prof_legacy = enrichment.get("professional", {})
        comp_legacy = enrichment.get("company", {})
        ci = enrichment.get("company_identity", {})
        cp = enrichment.get("company_profile", {})
        cin = enrichment.get("company_intelligence", {})
        intent_s = enrichment.get("intent_signals", {})
        outreach = enrichment.get("outreach", {})
        crm = enrichment.get("crm", {})
        ls = enrichment.get("lead_scoring", scoring)
        wi = enrichment.get("website_intelligence", {})

        # Build waterfall log
        waterfall_log = [
            {"step": 0, "source": "Bright Data", "fields_found": ["profile"], "note": f"LinkedIn scrape: {url}"},
        ]

        lead = {
            "id": _lead_id(url),
            "linkedin_url": url,
            "name": pp.get("full_name") or ident.get("full_name") or name,
            "first_name": first, "last_name": last,
            # Email fields intentionally empty — populated via POST /api/leads/view/email
            "work_email": None,
            "personal_email": None,
            "direct_phone": pp.get("direct_phone") or ident.get("direct_phone"),
            "twitter": pp.get("twitter") or ident.get("twitter"),
            "city": pp.get("city") or ident.get("city"),
            "country": pp.get("country") or ident.get("country"),
            "timezone": pp.get("timezone") or ident.get("timezone"),
            "title": pp.get("current_title") or prof_legacy.get("current_title") or profile.get("position", ""),
            "seniority_level": pp.get("seniority_level") or prof_legacy.get("seniority_level"),
            "department": pp.get("department") or prof_legacy.get("department"),
            "years_in_role": pp.get("years_in_role") or prof_legacy.get("years_in_role"),
            "company": ci.get("name") or comp_legacy.get("name") or profile.get("current_company_name", ""),
            "previous_companies": _safe_json(prof_legacy.get("previous_companies")),
            "top_skills": _safe_json(pp.get("top_skills") or prof_legacy.get("top_skills")),
            "education": pp.get("education") or prof_legacy.get("education"),
            "certifications": _safe_json(pp.get("certifications") or prof_legacy.get("certifications")),
            "languages": _safe_json(pp.get("languages") or prof_legacy.get("languages")),
            "company_website": cp.get("website") or ci.get("website") or comp_legacy.get("website"),
            "industry": cp.get("industry") or comp_legacy.get("industry"),
            "employee_count": _safe_int(cp.get("employee_count") or comp_legacy.get("employee_count")),
            "hq_location": cp.get("hq_location") or comp_legacy.get("hq_location"),
            "founded_year": str(cp.get("founded_year") or comp_legacy.get("founded_year") or ""),
            "funding_stage": cin.get("funding_stage") or comp_legacy.get("funding_stage"),
            "total_funding": cin.get("total_funding") or comp_legacy.get("total_funding"),
            "last_funding_date": cin.get("last_funding_date") or comp_legacy.get("last_funding_date"),
            "lead_investor": cin.get("lead_investor") or comp_legacy.get("lead_investor"),
            "annual_revenue": cp.get("annual_revenue_est") or comp_legacy.get("annual_revenue_est"),
            "tech_stack": _safe_json(cp.get("tech_stack") or comp_legacy.get("tech_stack")),
            "hiring_velocity": cin.get("hiring_velocity") or comp_legacy.get("hiring_velocity"),
            "avatar_url": _extract_avatar(profile),
            "company_logo": cp.get("company_logo"),
            "company_email": cp.get("company_email"),
            "company_description": wi.get("company_description"),
            "company_linkedin": ci.get("linkedin_url"),
            "company_twitter": cp.get("company_twitter"),
            "company_phone": cp.get("company_phone"),
            "waterfall_log": json.dumps(waterfall_log),
            # Stage 3 — Website Intelligence
            "website_intelligence": json.dumps({}),
            "product_offerings": _safe_json(wi.get("product_offerings")),
            "value_proposition": wi.get("value_proposition"),
            "target_customers": _safe_json(wi.get("target_customers")),
            "business_model": wi.get("business_model"),
            "pricing_signals": wi.get("pricing_signals"),
            "product_category": wi.get("product_category"),
            "data_completeness_score": int(ls.get("data_completeness_score", 0)),
            # Intent signals
            "recent_funding_event": intent_s.get("recent_funding_event"),
            "hiring_signal": intent_s.get("hiring_signal"),
            "job_change": intent_s.get("job_change"),
            "linkedin_activity": intent_s.get("linkedin_activity"),
            "news_mention": intent_s.get("news_mention"),
            "product_launch": intent_s.get("product_launch"),
            "competitor_usage": intent_s.get("competitor_usage"),
            "review_activity": intent_s.get("review_activity"),
            # Scoring
            "icp_fit_score": _safe_int(ls.get("icp_fit_score")),
            "intent_score": _safe_int(ls.get("intent_score")),
            "timing_score": _safe_int(ls.get("timing_score")),
            "engagement_score": 0,
            "total_score": _safe_int(ls.get("overall_score")),
            "score_tier": score_tier,
            "score_explanation": ls.get("score_explanation"),
            "icp_match_tier": ls.get("icp_match_tier") or tier_raw,
            "disqualification_flags": _safe_json(ls.get("disqualification_flags")),
            "email_subject": outreach.get("email_subject"),
            "cold_email": outreach.get("cold_email"),
            "linkedin_note": outreach.get("linkedin_note"),
            "best_channel": outreach.get("best_channel"),
            "best_send_time": outreach.get("best_send_time"),
            "outreach_angle": outreach.get("outreach_angle"),
            "sequence_type": outreach.get("sequence_type"),
            "outreach_sequence": _safe_json(outreach.get("sequence")),
            "last_contacted": outreach.get("last_contacted"),
            "email_status": outreach.get("email_status"),
            "lead_source": crm.get("lead_source", "LinkedIn URL"),
            "enrichment_source": "Bright Data",
            "data_completeness": _safe_int(crm.get("data_completeness")),
            "crm_stage": crm.get("crm_stage"),
            "tags": _safe_json(crm.get("tags")),
            "assigned_owner": crm.get("assigned_owner"),
            "full_data": json.dumps({
                "person_profile": enrichment.get("person_profile", {}),
                "company_identity": enrichment.get("company_identity", {}),
                "website_intelligence": enrichment.get("website_intelligence", {}),
                "company_profile": enrichment.get("company_profile", {}),
                "company_intelligence": enrichment.get("company_intelligence", {}),
                "intent_signals": enrichment.get("intent_signals", {}),
                "lead_scoring": enrichment.get("lead_scoring", {}),
                "outreach": enrichment.get("outreach", {}),
                # Legacy compatibility
                "identity": enrichment.get("identity", {}),
                "professional": enrichment.get("professional", {}),
                "company": enrichment.get("company", {}),
                "scoring": enrichment.get("scoring", {}),
                # Profile-level data for UI
                "recommendations": profile.get("recommendations") or [],
                "recommendations_count": profile.get("recommendations_count") or 0,
                "similar_profiles": profile.get("_similar_profiles") or [],
                "banner_image": profile.get("banner_image") or "",
                "linkedin_num_id": profile.get("linkedin_num_id") or "",
                "activity_emails": profile.get("_activity_emails") or [],
                "activity_phones": profile.get("_activity_phones") or [],
                "activity_full": profile.get("_activity_full") or [],
                "linkedin_posts": profile.get("_posts") or [],
                "hiring_signals": profile.get("_hiring_signals") or [],
            }, default=str),
            "raw_brightdata": json.dumps(profile, default=str),
            "about": (profile.get("about") or "")[:1000],
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            # Email source fields empty — set when /view/email is called
            "email_source": None,
            "email_confidence": None,
            "apollo_raw": None,
            # ── Stage 3: COMPLETED — LLM done, full data saved ───────────────
            # Patch crm_brief before saving:
            #   1. profile_image + company_logo — LLM never has real image URLs
            #   2. crm_scores — LLM returns "High"/"Very High", normalize to integers
            "crm_brief": json.dumps(
                _patch_crm_brief_scores(
                    _patch_crm_brief_images(
                        enrichment.get("crm_brief"),
                        avatar_url=_extract_avatar(profile) or "",
                        company_logo=cp.get("company_logo") or "",
                    ),
                    icp_fit_score=_safe_int(ls.get("icp_fit_score")),
                    intent_score=_safe_int(ls.get("intent_score")),
                    timing_score=_safe_int(ls.get("timing_score")),
                ),
                default=str,
            ) if enrichment.get("crm_brief") is not None else None,
            "status": "completed",
            "job_id": job_id,
            "organization_id": org_id,
            "enriched_at": now,
        }
        await _upsert_lead(lead)
        _plog(job_id, url, "COMPLETED", f"score_tier={score_tier!r} — lead saved and sent to LIO")
        logger.info("[Pipeline] %s → completed", url)

        # If crm_brief generation failed or empty — regenerate in background
        _saved_brief = _parse_json_safe(lead.get("crm_brief"), None)
        if not _validate_crm_brief(_saved_brief):
            logger.warning("[Pipeline] crm_brief missing/invalid for %s — scheduling background regeneration", url)
            asyncio.create_task(regenerate_crm_brief_for_lead(lead_id, org_id=org_id))

        return lead
    except Exception as e:
        import traceback
        _pipeline_log.error("[job=%s] [%s] FAILED — %s\n%s", (job_id or "")[-8:], raw_url, e, traceback.format_exc())
        logger.warning("[Webhook] %s: %s", raw_url, e)
        # Mark lead as failed if it was already saved in scraping/enriching stage
        try:
            lead_id = _lead_id(_normalize_linkedin_url(_clean_bd_linkedin_url(raw_url)))
            async with get_pool().acquire() as _conn:
                await _conn.execute(
                    "UPDATE enriched_leads SET status='failed', updated_at=NOW() WHERE id=$1 AND status IN ('scraping','enriching')",
                    lead_id,
                )
        except Exception:
            pass
        return None


async def rerun_brightdata_snapshot(snapshot_id: str) -> str:
    """Rerun a BrightData snapshot and return the new snapshot_id."""
    if not _bd_api_key():
        raise ValueError("BRIGHT_DATA_API_KEY not configured")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{BD_BASE}/snapshot/{snapshot_id}/rerun",
            headers=_bd_headers(),
        )
        resp.raise_for_status()
    new_snapshot_id = resp.json().get("snapshot_id") or resp.json().get("id")
    if not new_snapshot_id:
        raise ValueError(f"No snapshot_id in rerun response: {resp.text[:200]}")
    logger.info("[Rerun] BD snapshot %s rerun → new snapshot_id=%s", snapshot_id, new_snapshot_id)
    return new_snapshot_id


async def cancel_brightdata_snapshot(snapshot_id: str) -> None:
    """Cancel an in-progress BrightData snapshot via the cancel API."""
    if not _bd_api_key():
        return
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{BD_BASE}/snapshot/{snapshot_id}/cancel",
                headers=_bd_headers(),
            )
            logger.info("[Cancel] BD snapshot %s cancel response: %s", snapshot_id, resp.status_code)
    except Exception as e:
        logger.warning("[Cancel] BD snapshot %s cancel failed: %s", snapshot_id, e)


async def process_webhook_profiles(
    profiles: list[dict],
    job_id: Optional[str] = None,
    sub_job_id: Optional[str] = None,
) -> dict:
    # Resolve org_id, sso_id, forward_to_lio from job record
    org_id = "default"
    sso_id = ""
    forward_to_lio = False
    if job_id:
        job = await get_job(job_id)
        if job:
            # Guard: discard webhook results if job was cancelled
            if job.get("status") == "cancelled":
                logger.info("[Webhook] job %s is cancelled — discarding %d profiles", job_id, len(profiles))
                return {"processed": 0, "failed": 0, "cancelled": True}
            org_id = job.get("organization_id") or "default"
            sso_id = job.get("sso_id") or ""
            forward_to_lio = bool(job.get("forward_to_lio", False))

    # Use pre-created sub_job (chunked BD path) or create one (legacy/webhook path)
    _sub_job_id = sub_job_id
    if job_id:
        try:
            if _sub_job_id:
                # Chunk was pre-created by enrich_bulk — mark it running
                await _update_sub_job(_sub_job_id, status="running", total_urls=len(profiles))
            else:
                async with get_pool().acquire() as conn:
                    chunk_index = await conn.fetchval(
                        "SELECT COUNT(*) FROM enrichment_sub_jobs WHERE job_id=$1", job_id
                    )
                _sub_job_id = str(uuid.uuid4())
                await _create_sub_job(_sub_job_id, job_id, int(chunk_index), len(profiles), org_id)
                await _update_sub_job(_sub_job_id, status="running")
        except Exception as e:
            logger.debug("[Webhook] sub_job setup failed: %s", e)
            _sub_job_id = None

    # Process all profiles fully in parallel — no lock needed.
    # The job counter UPDATE below uses LEAST() which is atomic at DB level
    # and handles concurrent increments safely without any application-level lock.
    coros = [_process_one_webhook_profile(p, job_id, org_id, sub_job_id=_sub_job_id, sso_id=sso_id) for p in profiles]
    results = await asyncio.gather(*coros, return_exceptions=True)

    ok_leads = [r for r in results if isinstance(r, dict)]
    processed = len(ok_leads)
    failed    = len(results) - processed

    logger.info("[Webhook] batch done: processed=%d failed=%d job=%s", processed, failed, job_id or "—")

    # Update sub_job to reflect completion of this chunk
    if _sub_job_id:
        try:
            sub_status = (
                "completed" if failed == 0
                else ("failed" if processed == 0 else "completed_with_errors")
            )
            await _update_sub_job(_sub_job_id, status=sub_status, processed=processed, failed=failed)
        except Exception as e:
            logger.debug("[Webhook] sub_job update failed: %s", e)

    # Atomic job progress update — avoids race condition when multiple webhook
    # calls arrive simultaneously (BrightData sends partial batches).
    # Cap increments so processed+failed never exceeds total_urls (prevents >100% progress).
    if job_id:
        async with get_pool().acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE enrichment_jobs
                      SET processed   = LEAST(processed + $1, total_urls),
                          failed      = LEAST(failed + $2,    GREATEST(total_urls - LEAST(processed + $1, total_urls), 0)),
                          updated_at  = $3
                    WHERE id = $4
                RETURNING processed, failed, total_urls, organization_id, status""",
                processed, failed, datetime.now(timezone.utc), job_id,
            )
        if row:
            org_id = row["organization_id"] or org_id
            total_done = row["processed"] + row["failed"]
            if total_done >= row["total_urls"] and row["status"] not in ("completed", "failed", "completed_with_errors"):
                final_status = (
                    "completed" if row["failed"] == 0
                    else ("failed" if row["processed"] == 0 else "completed_with_errors")
                )
                await _update_job(job_id, status=final_status)
                await _publish_job_done(org_id, job_id, row["processed"], row["failed"])

    # Publish per-lead Ably events (real-time frontend updates)
    if job_id and ok_leads:
        for lead in ok_leads:
            await _publish_lead_done(org_id, job_id, lead)

    # Forward to LIO — mirror the single-enrich behaviour (send unless forward_to_lio=True)
    if not forward_to_lio and ok_leads:
        for lead in ok_leads:
            if lead.get("name"):
                lead.setdefault("linkedin_enrich", _format_linkedin_enrich(lead))
                asyncio.create_task(send_to_lio(lead, sso_id=sso_id))

    return {"processed": processed, "failed": failed}


async def regenerate_outreach_for_lead(lead_id: str) -> Optional[dict]:
    lead = await get_lead(lead_id)
    if not lead:
        return None

    org_id = lead.get("organization_id") or "default"

    # Use LIO Stage 5 system + user prompt — fast, targeted, config-driven
    # Let RuntimeError propagate so the route can return a proper error response
    generated = await generate_outreach_with_lio(lead, org_id=org_id)

    email_subject = generated["cold_email"]["subject"]
    cold_email    = generated["cold_email"]["full_email"] or generated["cold_email"]["body"]
    linkedin_note = generated["linkedin_note"]

    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE enriched_leads SET email_subject=$1, cold_email=$2, linkedin_note=$3 WHERE id=$4",
            email_subject, cold_email, linkedin_note, lead_id,
        )
    return await get_lead(lead_id)


async def regenerate_company_for_lead(lead_id: str) -> Optional[dict]:
    """
    Re-run full company enrichment waterfall (BrightData + website + Apollo + LLM)
    for an already-enriched lead. Saves results back to enriched_leads.
    """
    from ._company import enrich_company_waterfall

    lead = await get_lead(lead_id)
    if not lead:
        return None

    company_name = lead.get("company") or ""
    domain = ""
    website = lead.get("company_website") or ""
    if website:
        import re as _re
        m = _re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m:
            domain = m.group(1).lower()

    # Build a minimal profile dict from lead fields for the waterfall
    profile = {
        "current_company_link": lead.get("company_linkedin") or "",
        "current_company_logo": lead.get("company_logo") or "",
        "current_company_description": lead.get("company_description") or "",
        "current_company_employees_count": lead.get("employee_count") or 0,
        "current_company_industry": lead.get("industry") or "",
        "current_company_founded": lead.get("founded_year") or "",
    }

    company_data, _ = await enrich_company_waterfall(
        company_name=company_name,
        domain=domain,
        profile=profile,
        lead_id=lead_id,
    )

    if not company_data:
        return await get_lead(lead_id)

    # Persist all enriched company fields back to the lead row
    fields = [
        "company_logo", "company_description", "company_phone", "company_linkedin",
        "company_twitter", "company_website", "employee_count", "industry",
        "tech_stack", "hq_location", "founded_year", "funding_stage", "total_funding",
        "annual_revenue", "hiring_velocity", "recent_news", "company_tags",
        "culture_signals", "account_pitch", "crm_brief",
    ]
    updates = {k: company_data[k] for k in fields if k in company_data}
    if updates:
        # asyncpg requires lists/dicts to be JSON strings for TEXT columns
        serialised = {}
        for k, v in updates.items():
            if isinstance(v, (list, dict)):
                serialised[k] = json.dumps(v, default=str)
            else:
                serialised[k] = v
        set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(serialised))
        vals = list(serialised.values()) + [lead_id]
        async with get_pool().acquire() as conn:
            await conn.execute(
                f"UPDATE enriched_leads SET {set_clause}, updated_at = NOW() WHERE id = ${len(vals)}",
                *vals,
            )

    return await get_lead(lead_id)


async def regenerate_crm_brief_for_lead(lead_id: str, org_id: str = "") -> Optional[dict]:
    """
    Re-run the CRM brief LLM call for an already-enriched lead.
    Reads raw_profile from DB, uses lio_system_prompt + lio_model from
    workspace_configs, saves updated crm_brief back to enriched_leads.
    org_id is taken from the JWT (passed by the route) and falls back to
    the lead's stored organization_id so the right config is always loaded.
    """
    import re as _re
    from config.enrichment_config_service import get_workspace_config

    lead = await get_lead(lead_id)
    if not lead:
        return None

    # Prefer org_id from JWT (request), fall back to what's stored on the lead
    org_id = org_id or lead.get("organization_id") or "default"

    # Read raw BrightData profile from DB (raw_brightdata JSONB preferred, raw_profile TEXT fallback)
    raw_bd = lead.get("raw_brightdata")
    if raw_bd:
        profile = raw_bd if isinstance(raw_bd, dict) else (json.loads(raw_bd) if isinstance(raw_bd, str) else {})
    else:
        raw_profile_raw = lead.get("raw_profile")
        if not raw_profile_raw:
            raise RuntimeError("No raw BrightData profile found for this lead — cannot regenerate CRM brief.")
        try:
            profile = json.loads(raw_profile_raw) if isinstance(raw_profile_raw, str) else raw_profile_raw
        except Exception:
            profile = {}

    # Read config from workspace_configs
    try:
        cfg = await get_workspace_config(org_id)
        model_override   = cfg.get("lio_model", "").strip() or None
        crm_brief_prompt = cfg.get("lio_system_prompt", "").strip()
    except Exception:
        model_override   = None
        crm_brief_prompt = ""

    if not crm_brief_prompt:
        crm_brief_prompt = _DEFAULT_CRM_BRIEF_PROMPT

    # Build optimized, noise-free profile for the LLM; trim if combined content is too large
    optimized = _build_llm_profile(profile)
    _profile_budget = max(20000, 80000 - len(crm_brief_prompt))
    optimized = _trim_profile_to_budget(optimized, _profile_budget)
    optimized_str = json.dumps(optimized, separators=(",", ":"), ensure_ascii=False, default=str)
    logger.info(
        "[RegenerateCrmBrief] Optimized profile: %d chars (budget %d) ~%d tokens",
        len(optimized_str), _profile_budget, len(optimized_str) // 4,
    )

    brief = await _call_llm([
        {"role": "system", "content": crm_brief_prompt},
        {"role": "user",   "content": f"Analyze this LinkedIn prospect data and return the JSON exactly as specified:\n\n{optimized_str}"},
    ], max_tokens=6000, temperature=0.3, model_override=model_override, wb_llm_model_override=model_override,
       hf_first=True)

    if not brief:
        raise RuntimeError("LLM unavailable or returned no content.")

    # Strip <think>...</think> blocks (qwen3, deepseek, etc.)
    brief = _re.sub(r"<think>.*?</think>", "", brief, flags=_re.DOTALL)
    brief = _re.sub(r"<think>.*", "", brief, flags=_re.DOTALL).strip()
    # Strip any [inferred] markers the model may have added
    brief = brief.replace("[inferred]", "").replace("[Inferred]", "")

    # Extract first complete JSON object using brace counting
    _start = brief.find("{")
    if _start != -1:
        _depth, _end = 0, -1
        for _i, _ch in enumerate(brief[_start:], _start):
            if _ch == "{": _depth += 1
            elif _ch == "}":
                _depth -= 1
                if _depth == 0:
                    _end = _i
                    break
        if _end == -1:
            # JSON was truncated — LLM hit max_tokens mid-response
            raise RuntimeError(f"CRM brief truncated (max_tokens hit) — partial JSON discarded. len={len(brief)}")
        brief = brief[_start:_end + 1]
    else:
        raise RuntimeError("No JSON object found in LLM response")

    try:
        _crm = json.loads(brief)

        # ── Authoritative sources ─────────────────────────────────────────────
        _bd_company = (
            profile.get("current_company_name")
            or (profile.get("current_company") or {}).get("name")
            or lead.get("company") or ""
        )
        _bd_title = (
            profile.get("position") or profile.get("headline")
            or lead.get("title") or ""
        )

        # ── who_they_are — force company + title from BrightData ─────────────
        # LLM reads experience array and picks wrong company/title.
        # current_company.name is the ONLY authoritative source.
        if isinstance(_crm.get("who_they_are"), dict):
            if _bd_company:
                _crm["who_they_are"]["company"] = _bd_company
            if _bd_title:
                _crm["who_they_are"]["title"] = _bd_title

        # ── their_company — override with real DB data from waterfall ─────────
        # If no data found by waterfall, store empty — never use fake/hallucinated values.
        if isinstance(_crm.get("their_company"), dict):
            _tc = _crm["their_company"]
            _tc["website"]      = lead.get("company_website") or ""
            _tc["industry"]     = lead.get("industry") or ""
            _tc["company_size"] = str(lead.get("employee_count") or "")
            _tc["founded"]      = str(lead.get("founded_year") or "")
            _tc["stage"]        = lead.get("funding_stage") or ""

        crm_brief_db = json.dumps(_crm, default=str)
    except Exception as _je:
        # Do NOT store malformed JSON — raises so the caller knows regen failed
        raise RuntimeError(f"CRM brief JSON parse failed: {_je} | preview={brief[:100]!r}")

    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE enriched_leads SET crm_brief=$1 WHERE id=$2",
            crm_brief_db, lead_id,
        )

    logger.info("[RegenerateCrmBrief] Updated crm_brief for lead=%s (%d chars)", lead_id, len(crm_brief_db))
    updated_lead = await get_lead(lead_id)
    # Auto-send to LIO after successful regen — but only if called standalone (not from send_to_lio itself)
    if updated_lead and updated_lead.get("name"):
        sso_id_for_lio = updated_lead.get("sso_id") or ""
        asyncio.create_task(send_to_lio(updated_lead, sso_id=sso_id_for_lio))
    return updated_lead
