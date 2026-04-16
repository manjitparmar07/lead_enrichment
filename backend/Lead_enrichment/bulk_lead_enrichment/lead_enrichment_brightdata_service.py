"""
lead_enrichment_brightdata_service.py
--------------------------------------
Backward-compatibility re-export shim.

The original 7,800-line monolith has been split into focused modules:
  _utils.py       — pure utilities, API key accessors, scoring helpers
  _clients.py     — HTTP client singletons, circuit breaker, semaphores
  _db.py          — all database operations (leads, jobs, Redis)
  _llm.py         — HuggingFace LLM integration
  _brightdata.py  — BrightData scraping (profile + company)
  _contact.py     — contact discovery waterfall (Apollo → DC → PDL → ZB)
  _company.py     — company enrichment waterfall
  _website.py     — website intelligence scraping
  _outreach.py    — LIO pipeline and outreach generation
  _enrichment.py  — main enrichment pipeline (single, bulk, stream)

This file re-exports every public symbol so existing callers continue to work
without modification.
"""

from __future__ import annotations

# ── Utilities ─────────────────────────────────────────────────────────────────
from ._utils import (
    _plog,
    _pipeline_log,
    _k,
    _bd_api_key,
    _bd_profile_dataset,
    _bd_company_dataset,
    _hf_token,
    _hf_model,
    _apollo_api_key,
    _dropcontact_key,
    _pdl_api_key,
    _zerobounce_key,
    _outreach_threshold,
    _outreach_threshold_for_org,
    _EMOJI_RE,
    _strip_emojis,
    _clean_lead_strings,
    _clean_title,
    _to_third_person,
    _normalise_person_text,
    _parse_json_safe,
    _safe_json,
    _safe_int,
    _parse_name,
    _COUNTRY_CODES,
    _country_name,
    _lead_id,
    _resolve_tier,
    _patch_crm_brief_images,
    _patch_crm_brief_scores,
    _SCORE_LABEL_MAP,
    _is_company_url,
    _is_person_url,
)

# ── Clients / concurrency ─────────────────────────────────────────────────────
from ._clients import (
    BD_BASE,
    LIO_RECEIVE_URL,
    REDIS_URL,
    BD_REQUEST_TIMEOUT,
    _llm_semaphore,
    _enrichment_semaphore,
    _in_flight_leads,
    _bd_trigger_semaphore,
    _job_webhook_locks,
    _get_lio_client,
    _get_api_client,
    _get_bd_client,
    _get_web_client,
    _bd_circuit_ok,
    _bd_circuit_success,
    _bd_circuit_failure,
    _bd_rate_limit_wait,
    _bd_backoff,
)

# ── Database ──────────────────────────────────────────────────────────────────
from ._db import (
    init_leads_db,
    _get_redis,
    _push_lead_task,
    _publish_lead_done,
    _publish_job_done,
    _upsert_lead,
    get_lead,
    get_lead_by_url,
    list_leads,
    delete_lead,
    _update_job,
    _create_sub_job,
    _update_sub_job,
    list_sub_jobs,
    get_job,
    list_jobs,
    get_lead_lio_results,
    save_lead_lio_results,
    check_existing_lead,
    check_existing_leads_batch,
    get_stale_leads,
    mark_snapshot_processed,
    is_snapshot_processed,
    audit_log,
    list_audit_log,
    add_lead_note,
    list_lead_notes,
    delete_lead_note,
    _format_linkedin_enrich,
)

# ── LLM ───────────────────────────────────────────────────────────────────────
from ._llm import (
    _call_llm,
    _parse_json_from_llm,
)

# ── BrightData ────────────────────────────────────────────────────────────────
from ._brightdata import (
    _normalize_linkedin_url,
    _clean_bd_linkedin_url,
    _bd_headers,
    _normalize_bd_profile,
    fetch_profile_sync,
    trigger_batch_snapshot,
    poll_snapshot,
)

# ── Contact waterfall ─────────────────────────────────────────────────────────
from ._contact import (
    _SOCIAL_DOMAINS,
    _extract_domain,
    _try_apollo,
    _try_dropcontact,
    _try_pdl,
    find_contact_info,
)

# ── Company enrichment ────────────────────────────────────────────────────────
from ._company import (
    _try_clearbit_logo,
    _normalize_apollo_org,
    _try_apollo_org,
    _try_apollo_company_search,
    _guess_domain_variants,
    _normalize_bd_company,
    _try_bd_company,
    _COMPANY_CRM_SYSTEM_PROMPT,
    _build_company_crm_user_prompt,
    _fetch_apollo_company_raw,
    _enrich_company_with_llm,
    enrich_company_waterfall,
)

# ── Website intelligence ──────────────────────────────────────────────────────
from ._website import (
    _extract_avatar,
    scrape_website_intelligence,
)

# ── Outreach / LIO pipeline ───────────────────────────────────────────────────
from ._outreach import (
    _LIO_DLQ_KEY,
    _lio_dlq_push,
    lio_dlq_replay,
    _validate_crm_brief,
    send_to_lio_failed,
    send_to_lio,
    _DEFAULT_CRM_BRIEF_PROMPT,
    _build_comprehensive_prompt,
    _render_lio_template,
    _parse_list_from_llm,
    run_lio_pipeline,
    generate_outreach_with_lio,
    _merge_lio_into_enrichment,
    build_comprehensive_enrichment,
    _build_llm_profile,
    _trim_profile_to_budget,
    _rule_based_enrichment,
)

# ── Main enrichment pipeline ──────────────────────────────────────────────────
from ._enrichment import (
    enrich_company_url,
    enrich_single,
    enrich_single_stream,
    _poll_and_process_snapshot,
    _bd_chunk_size,
    enrich_bulk,
    _process_one_webhook_profile,
    rerun_brightdata_snapshot,
    cancel_brightdata_snapshot,
    process_webhook_profiles,
    regenerate_outreach_for_lead,
    regenerate_company_for_lead,
    regenerate_crm_brief_for_lead,
)

__all__ = [
    # utils
    "_plog", "_pipeline_log", "_k",
    "_bd_api_key", "_bd_profile_dataset", "_bd_company_dataset",
    "_hf_token", "_hf_model", "_apollo_api_key", "_dropcontact_key",
    "_pdl_api_key", "_zerobounce_key",
    "_outreach_threshold", "_outreach_threshold_for_org",
    "_EMOJI_RE", "_strip_emojis", "_clean_lead_strings",
    "_clean_title", "_to_third_person", "_normalise_person_text",
    "_parse_json_safe", "_safe_json", "_safe_int",
    "_parse_name", "_COUNTRY_CODES", "_country_name", "_lead_id",
    "_resolve_tier", "_patch_crm_brief_images", "_patch_crm_brief_scores",
    "_SCORE_LABEL_MAP", "_is_company_url", "_is_person_url",
    # clients
    "BD_BASE", "LIO_RECEIVE_URL", "REDIS_URL", "BD_REQUEST_TIMEOUT",
    "_llm_semaphore", "_enrichment_semaphore", "_in_flight_leads",
    "_bd_trigger_semaphore", "_job_webhook_locks",
    "_get_lio_client", "_get_api_client", "_get_bd_client", "_get_web_client",
    "_bd_circuit_ok", "_bd_circuit_success", "_bd_circuit_failure",
    "_bd_rate_limit_wait", "_bd_backoff",
    # db
    "init_leads_db", "_get_redis", "_push_lead_task",
    "_publish_lead_done", "_publish_job_done",
    "_upsert_lead", "get_lead", "get_lead_by_url", "list_leads", "delete_lead",
    "_update_job", "_create_sub_job", "_update_sub_job", "list_sub_jobs",
    "get_job", "list_jobs",
    "get_lead_lio_results", "save_lead_lio_results",
    "check_existing_lead", "check_existing_leads_batch", "get_stale_leads",
    "mark_snapshot_processed", "is_snapshot_processed",
    "audit_log", "list_audit_log",
    "add_lead_note", "list_lead_notes", "delete_lead_note",
    "_format_linkedin_enrich",
    # llm
    "_call_llm", "_parse_json_from_llm",
    # brightdata
    "_normalize_linkedin_url", "_clean_bd_linkedin_url", "_bd_headers",
    "_normalize_bd_profile", "fetch_profile_sync",
    "trigger_batch_snapshot", "poll_snapshot",
    # contact
    "_SOCIAL_DOMAINS", "_extract_domain",
    "_try_apollo", "_try_dropcontact", "_try_pdl",
    "find_contact_info",
    # company
    "_try_clearbit_logo", "_normalize_apollo_org",
    "_try_apollo_org", "_try_apollo_company_search", "_guess_domain_variants",
    "_normalize_bd_company", "_try_bd_company",
    "_COMPANY_CRM_SYSTEM_PROMPT", "_build_company_crm_user_prompt",
    "_fetch_apollo_company_raw", "_enrich_company_with_llm",
    "enrich_company_waterfall",
    # website
    "_extract_avatar", "scrape_website_intelligence",
    # outreach
    "_LIO_DLQ_KEY", "_lio_dlq_push", "lio_dlq_replay",
    "_validate_crm_brief", "send_to_lio_failed", "send_to_lio",
    "_DEFAULT_CRM_BRIEF_PROMPT", "_build_comprehensive_prompt",
    "_render_lio_template", "_parse_list_from_llm",
    "run_lio_pipeline", "generate_outreach_with_lio",
    "_merge_lio_into_enrichment", "build_comprehensive_enrichment",
    "_build_llm_profile", "_trim_profile_to_budget", "_rule_based_enrichment",
    # enrichment pipeline
    "enrich_company_url", "enrich_single", "enrich_single_stream",
    "_poll_and_process_snapshot", "_bd_chunk_size", "enrich_bulk",
    "_process_one_webhook_profile",
    "rerun_brightdata_snapshot", "cancel_brightdata_snapshot",
    "process_webhook_profiles",
    "regenerate_outreach_for_lead", "regenerate_company_for_lead",
    "regenerate_crm_brief_for_lead",
]
