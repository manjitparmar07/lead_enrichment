"""LIO pipeline, send_to_lio, outreach generation, comprehensive enrichment, rule-based scoring."""

from __future__ import annotations
import asyncio
import copy
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from db import get_pool
from ._utils import (
    _parse_name, _country_name, _safe_int, _parse_json_safe,
    _strip_emojis, _lead_id, _hf_token, _outreach_threshold_for_org,
    _plog, _pipeline_log,
)
from ._clients import _get_lio_client, LIO_RECEIVE_URL
from ._llm import _call_llm, _parse_json_from_llm
from ._contact import _extract_domain
from ._website import scrape_website_intelligence

logger = logging.getLogger(__name__)

# ── LIO Dead-Letter Queue ─────────────────────────────────────────────────────
_LIO_DLQ_KEY = "lio:dlq"

async def _lio_dlq_push(lead_id: str, payload: dict, reason: str = "") -> None:
    """Push a failed LIO delivery to Redis dead-letter queue for later replay."""
    try:
        import redis.asyncio as aioredis
        redis_url = os.getenv("REDIS_URL", "")
        if not redis_url:
            return
        r = aioredis.from_url(redis_url, decode_responses=True)
        entry = json.dumps({
            "lead_id":    lead_id,
            "payload":    payload,
            "reason":     reason,
            "failed_at":  datetime.now(timezone.utc).isoformat(),
        })
        await r.lpush(_LIO_DLQ_KEY, entry)
        await r.close()
        logger.info("[LIO-DLQ] Lead %s pushed to DLQ (reason=%s)", lead_id, reason)
    except Exception as e:
        logger.error("[LIO-DLQ] Failed to push lead %s to DLQ: %s", lead_id, e)


async def lio_dlq_replay(max_items: int = 50) -> dict:
    """Replay up to max_items entries from the LIO dead-letter queue."""
    try:
        import redis.asyncio as aioredis
        redis_url = os.getenv("REDIS_URL", "")
        if not redis_url:
            return {"replayed": 0, "failed": 0, "error": "REDIS_URL not set"}
        r = aioredis.from_url(redis_url, decode_responses=True)
        replayed = failed = 0
        for _ in range(max_items):
            raw = await r.rpop(_LIO_DLQ_KEY)
            if not raw:
                break
            try:
                entry = json.loads(raw)
                payload = entry.get("payload", {})
                lead_id = entry.get("lead_id", "unknown")
                client = _get_lio_client()
                resp = await client.post(LIO_RECEIVE_URL, json=payload)
                resp.raise_for_status()
                logger.info("[LIO-DLQ] Replayed lead %s → HTTP %s", lead_id, resp.status_code)
                replayed += 1
            except Exception as e:
                logger.warning("[LIO-DLQ] Replay failed: %s — re-queuing", e)
                await r.lpush(_LIO_DLQ_KEY, raw)  # put back for next retry
                failed += 1
        await r.close()
        return {"replayed": replayed, "failed": failed}
    except Exception as e:
        logger.error("[LIO-DLQ] Replay error: %s", e)
        return {"replayed": 0, "failed": 0, "error": str(e)}


_CRM_BRIEF_REQUIRED_KEYS = ("who_they_are", "crm_scores", "outreach_blueprint", "crm_import_fields")

def _validate_crm_brief(d: dict) -> bool:
    """
    Returns True if crm_brief has all required top-level keys and is not empty.
    False = treat as invalid → trigger regen.
    """
    if not d or not isinstance(d, dict):
        return False
    for key in _CRM_BRIEF_REQUIRED_KEYS:
        if key not in d or not d[key]:
            return False
    return True


async def send_to_lio_failed(
    linkedin_url: str,
    org_id: str,
    sso_id: str = "",
    reason: str = "enrichment_failed",
    error_message: str = "",
    profile_data: Optional[dict] = None,
) -> None:
    """
    Notify LIO that a LinkedIn URL failed enrichment.
    Sends status=failed with full crm_brief envelope so LIO accepts it.

    profile_data: raw BrightData profile dict — pass whatever BD returned so LIO
                  gets the most complete picture available (name, company, etc.)
    """
    lio_url = LIO_RECEIVE_URL
    if not lio_url:
        logger.warning("[LIO-FAILED] LIO_RECEIVE_URL not set — skipping failure notification for %s", linkedin_url)
        return
    lead_id_val = _lead_id(linkedin_url) if linkedin_url else "unknown"
    p = profile_data or {}

    # Extract whatever identity data BrightData returned (often empty for private profiles)
    raw_name    = p.get("name") or p.get("full_name") or ""
    first_name  = p.get("first_name") or (raw_name.split()[0] if raw_name else "")
    last_name   = p.get("last_name") or (" ".join(raw_name.split()[1:]) if len(raw_name.split()) > 1 else "")

    # Fallback: parse name from LinkedIn URL slug when BD returned no name
    # e.g. /in/omar-aldirini-7130b8141 → first="Omar" last="Aldirini"
    if not raw_name and linkedin_url:
        _slug_match = re.search(r"/in/([^/?#\s]+)", linkedin_url, re.IGNORECASE)
        if _slug_match:
            _slug = _slug_match.group(1).rstrip("/")
            # Strip trailing numeric ID (e.g. "-7130b8141")
            _slug_clean = re.sub(r"-[a-z0-9]{6,}$", "", _slug)
            _slug_parts = [p.capitalize() for p in _slug_clean.split("-") if p]
            if _slug_parts:
                raw_name   = " ".join(_slug_parts)
                first_name = _slug_parts[0]
                last_name  = " ".join(_slug_parts[1:]) if len(_slug_parts) > 1 else ""
    title       = p.get("title") or p.get("headline") or ""
    company     = p.get("current_company_name") or p.get("current_company") or ""
    location    = p.get("city") or p.get("location") or ""
    country     = p.get("country") or p.get("country_code") or ""
    followers   = p.get("followers") or 0
    connections = p.get("connections") or 0
    avatar_url  = p.get("profile_image_url") or p.get("avatar") or ""
    about       = p.get("about") or p.get("summary") or p.get("headline") or ""

    enrichment_data = {
        "status":        "failed",
        "input_url":     linkedin_url,
        "linkedin_url":  linkedin_url,
        "error_reason":  reason,
        "error_message": error_message or reason,
        "who_they_are": {
            "lead_id":      lead_id_val,
            "linkedin_url": linkedin_url,
            "name":         raw_name,
            "first_name":   first_name,
            "last_name":    last_name,
            "title":        title,
            "company":      company,
            "location":     location,
            "country":      country,
            "followers":    followers,
            "connections":  connections,
            "profile_image": avatar_url,
            "about":        about[:500] if about else "",
        },
        "their_company": {
            "name":     company,
            "industry": p.get("industry") or "",
        },
        "contact": {
            "work_email": None,
            "phone":      None,
        },
        "crm_scores": {
            "icp_fit":          0,
            "engagement_score": 0,
            "timing_score":     0,
            "priority":         "cold",
        },
        "crm_import_fields": {
            "analyst_summary": (
                f"Profile is private or hidden on LinkedIn. "
                f"Enrichment failed: {error_message or reason}."
            ),
            "tags": [],
        },
    }
    payload = {
        "enrichment_data": enrichment_data,
        "sso_id":          sso_id,
        "organization_id": org_id,
    }
    logger.info("[LIO-FAILED] Sending failure for %s | reason=%s | org=%s | sso=%s",
                linkedin_url, reason, org_id, sso_id)
    _pipeline_log.info("[LIO-FAILED-PAYLOAD] url=%s\n%s", linkedin_url, json.dumps(payload, indent=2, default=str))
    _LIO_MAX_ATTEMPTS = 3
    for attempt in range(1, _LIO_MAX_ATTEMPTS + 1):
        try:
            client = _get_lio_client()
            resp = await client.post(lio_url, json=payload)
            logger.info("[LIO-FAILED] Response for %s → HTTP %s | body=%s",
                        linkedin_url, resp.status_code, resp.text[:500])
            resp.raise_for_status()
            logger.info("[LIO-FAILED] Successfully sent failure for %s (attempt %d)", linkedin_url, attempt)
            return
        except Exception as e:
            logger.warning("[LIO-FAILED] Attempt %d/%d failed for %s: %s",
                           attempt, _LIO_MAX_ATTEMPTS, linkedin_url, e)
            if attempt < _LIO_MAX_ATTEMPTS:
                await asyncio.sleep(5 * attempt)
    logger.error("[LIO-FAILED] All %d attempts failed for %s — giving up", _LIO_MAX_ATTEMPTS, linkedin_url)


async def send_to_lio(lead: dict, sso_id: str = "", force: bool = False) -> None:
    """
    Fire-and-forget: forward an enriched lead to the LIO service.
    Called as asyncio.create_task() — never blocks enrich_single().
    force=True bypasses idempotency — use when user explicitly re-submits same URL.
    Logs a warning on failure but does not raise.
    """
    url = LIO_RECEIVE_URL
    if not url:
        logger.warning("[LIO] LIO_RECEIVE_URL is not set — skipping forward")
        return

    lead_id = lead.get("lead_id") or lead.get("id", "unknown")

    # Idempotency — skip if already sent, unless force=True (user re-submission)
    if not force:
        try:
            async with get_pool().acquire() as _conn:
                _row = await _conn.fetchrow(
                    "SELECT lio_sent_at FROM enriched_leads WHERE id=$1", lead_id
                )
                if _row and _row["lio_sent_at"]:
                    logger.info("[LIO] Lead %s already sent at %s — skipping (idempotent)", lead_id, _row["lio_sent_at"])
                    return
        except Exception:
            pass  # DB check failed — proceed

    # Redis secondary cache (24h TTL) — fast path if Redis available
    _lio_sent_key = f"lio:sent:{lead_id}"
    if not force:
        try:
            import redis.asyncio as aioredis
            _redis_url = os.getenv("REDIS_URL", "")
            if _redis_url:
                _r = aioredis.from_url(_redis_url, decode_responses=True)
                already_sent = await _r.get(_lio_sent_key)
                await _r.close()
                if already_sent:
                    logger.info("[LIO] Lead %s already sent (Redis) — skipping", lead_id)
                    return
        except Exception:
            pass  # Redis unavailable — proceed without Redis check

    # work_email is the DB column name; email is legacy/nested key — prefer work_email
    raw_email = lead.get("work_email") or lead.get("email", "") or ""
    # Strip Apollo/external placeholder emails — they are internal IDs, not real addresses
    email = raw_email if raw_email and "placeholder" not in raw_email.lower() else ""

    # phone: direct_phone is the DB column; phone is legacy
    phone = lead.get("direct_phone") or lead.get("phone", "") or ""

    # enrichment_data = the full linkedin_enrich structured view
    import copy
    linkedin_enrich = copy.deepcopy(lead.get("linkedin_enrich") or {})

    # Strip emojis from identity fields
    identity = linkedin_enrich.get("identity") or {}
    for f in ("name", "first_name", "last_name", "title", "company", "about", "location"):
        if identity.get(f):
            identity[f] = _strip_emojis(identity[f])
    profile = identity.get("profile") or {}
    for f in ("full_name", "first_name", "last_name", "current_title"):
        if profile.get(f):
            profile[f] = _strip_emojis(profile[f])

    # Clean placeholder + emoji in contact block
    contact_block = linkedin_enrich.get("contact") or {}
    for email_key in ("work_email", "email"):
        v = contact_block.get(email_key)
        if v and "placeholder" in str(v).lower():
            contact_block[email_key] = None

    # crm_brief is stored as JSON string in DB — parse back to dict for LIO
    _crm_brief_raw = lead.get("crm_brief")
    _crm_brief = None
    if _crm_brief_raw:
        try:
            _crm_brief = json.loads(_crm_brief_raw) if isinstance(_crm_brief_raw, str) else _crm_brief_raw
        except Exception:
            pass

    # Patch profile_image and company_logo from DB — LLM never has actual image URLs
    if _crm_brief and isinstance(_crm_brief, dict):
        who = _crm_brief.get("who_they_are")
        if isinstance(who, dict):
            if not who.get("profile_image"):
                who["profile_image"] = lead.get("avatar_url") or ""
            if not who.get("company_logo"):
                who["company_logo"] = lead.get("company_logo") or ""
            # Always override company/title — LLM infers from experience array and picks wrong company.
            # DB values come from BrightData current_company (authoritative).
            _db_company = lead.get("company") or ""
            if _db_company:
                who["company"] = _db_company
            _db_title = lead.get("title") or ""
            if _db_title and not who.get("title"):
                who["title"] = _db_title

        # Patch their_company from DB — LLM populates this based on the wrong company when it
        # infers the wrong current employer from the experience array.
        _tc = _crm_brief.get("their_company")
        if isinstance(_tc, dict):
            if lead.get("industry"):
                _tc["industry"] = lead["industry"]
            if lead.get("employee_count"):
                _tc["company_size"] = str(lead["employee_count"])
            if lead.get("founded_year"):
                _tc["founded"] = str(lead["founded_year"])
            if lead.get("company_website"):
                _tc["website"] = lead["company_website"]
            if lead.get("funding_stage"):
                _tc["stage"] = lead["funding_stage"]
            if lead.get("business_model"):
                _tc["type"] = lead["business_model"]

        # Ensure crm_scores are integers — LLM sometimes returns strings like "High", "80%"
        scores = _crm_brief.get("crm_scores")
        if isinstance(scores, dict):
            for _sf in ("icp_fit", "engagement_score", "timing_score"):
                v = scores.get(_sf)
                if not isinstance(v, int):
                    try:
                        scores[_sf] = int(str(v).replace("%", "").strip())
                    except Exception:
                        scores[_sf] = 0

            # Fallback to DB-calculated scores when LLM returned 0
            # (LLM copies the template placeholder; DB scoring is the ground truth)
            if not scores.get("icp_fit"):
                scores["icp_fit"] = int(lead.get("icp_fit_score") or 0)
            if not scores.get("engagement_score"):
                scores["engagement_score"] = int(lead.get("intent_score") or 0)
            if not scores.get("timing_score"):
                scores["timing_score"] = int(lead.get("timing_score") or 0)

        # Inject lead_id into who_they_are — LLM output never has this
        if isinstance(_crm_brief.get("who_they_are"), dict):
            _crm_brief["who_they_are"].setdefault("lead_id", lead_id)

        # Inject contact block — LLM never has actual email/phone; pull from DB
        if "contact" not in _crm_brief:
            _crm_brief["contact"] = {
                "work_email":       email,
                "phone":            phone,
                "email_source":     lead.get("email_source", ""),
                "email_confidence": lead.get("email_confidence", ""),
                "email_verified":   bool(lead.get("email_verified")),
                "bounce_risk":      lead.get("bounce_risk", ""),
            }

        # Patch crm_import_fields.analyst_summary — build a narrative bio if LLM left it empty
        _cif = _crm_brief.get("crm_import_fields")
        if isinstance(_cif, dict) and not _cif.get("analyst_summary"):
            # Priority 1: score_explanation from scoring engine (already a sentence)
            _db_summary = lead.get("score_explanation") or ""
            if not _db_summary:
                # Priority 2: build narrative from crm_brief's own LLM-generated fields
                _wta = _crm_brief.get("who_they_are") or {}
                _wdt = _crm_brief.get("what_drives_them") or {}
                _ob  = _crm_brief.get("outreach_blueprint") or {}
                _persona    = _wta.get("persona") or _wta.get("seniority") or ""
                _trajectory = _wta.get("trajectory") or ""
                _strategy   = _ob.get("one_line_strategy") or ""
                _pain       = (_wdt.get("pain_points") or [None])[0] or ""
                _name_str   = lead.get("name") or _wta.get("name") or ""
                _company_str = lead.get("company") or _wta.get("company") or ""
                _title_str  = lead.get("title") or _wta.get("title") or ""
                _sentences: list[str] = []
                # Sentence 1: who they are
                if _name_str and _title_str and _company_str:
                    _sentences.append(f"{_name_str} is a {_title_str} at {_company_str}.")
                elif _name_str and _persona and _company_str:
                    _sentences.append(f"{_name_str} is a {_persona} at {_company_str}.")
                elif _trajectory:
                    _sentences.append(_trajectory if _trajectory.endswith(".") else _trajectory + ".")
                # Sentence 2: what makes them notable
                if _pain:
                    _sentences.append(_pain if _pain.endswith(".") else _pain + ".")
                # Sentence 3: sales relevance / strategy
                if _strategy:
                    _sentences.append(_strategy if _strategy.endswith(".") else _strategy + ".")
                elif lead.get("warm_signal"):
                    _sentences.append(lead["warm_signal"] if lead["warm_signal"].endswith(".") else lead["warm_signal"] + ".")
                _db_summary = " ".join(_sentences)
            _cif["analyst_summary"] = _db_summary

        # Inject company_intelligence — LLM schema doesn't include this; pull from DB/website_intel
        if "company_intelligence" not in _crm_brief:
            _wi = _parse_json_safe(lead.get("website_intelligence"), {})
            _prod_offer = _parse_json_safe(lead.get("product_offerings"), [])
            _tgt_custs  = _parse_json_safe(lead.get("target_customers"), [])
            _crm_brief["company_intelligence"] = {
                "description":       lead.get("company_description") or _wi.get("company_description") or "",
                "value_proposition": lead.get("value_proposition") or _wi.get("value_proposition") or "",
                "product_category":  lead.get("product_category") or _wi.get("product_category") or "",
                "product_offerings": _prod_offer[:10],
                "target_customers":  _tgt_custs,
                "pricing_signals":   lead.get("pricing_signals") or _wi.get("pricing_signals") or "",
            }

    # enrichment_data = LLM-structured output (shaped by lio_system_prompt).
    # Fallback maps linkedin_enrich into the same crm_brief JSON structure so LIO
    # always receives a consistent shape regardless of whether LLM ran.
    def _linkedin_enrich_to_crm_brief_shape(le: dict) -> dict:
        ident   = le.get("identity") or {}
        prof    = ident.get("profile") or {}
        scores  = le.get("scores") or {}
        brkdown = scores.get("breakdown") or {}
        bsig    = le.get("behavioural_signals") or {}
        pitch   = le.get("pitch_intelligence") or {}
        act     = le.get("activity") or {}
        co      = ident.get("profile") or {}
        tags    = le.get("tags") or []
        posts   = act.get("linkedin_posts") or []
        feed    = act.get("feed") or []
        return {
            "who_they_are": {
                "name":           ident.get("name", ""),
                "title":          ident.get("title", ""),
                "company":        ident.get("company", ""),
                "lead_id":        lead_id,
                "location":       ident.get("location") or ident.get("country", ""),
                "linkedin_url":   ident.get("linkedin_url", ""),
                "profile_image":  lead.get("avatar_url") or "",
                "company_logo":   lead.get("company_logo") or (le.get("company_profile") or {}).get("company_logo") or "",
                "followers":      ident.get("followers", 0),
                "connections":    ident.get("connections", 0),
                "persona":        prof.get("seniority_level", ""),
                "seniority":      prof.get("seniority_level", ""),
                "trajectory":     "",
                "decision_maker": "Yes" if "c-level" in (prof.get("seniority_level") or "").lower() or "vp" in (prof.get("seniority_level") or "").lower() else "Unknown",
            },
            "their_company": {
                "type":             "",
                "industry":         "",
                "stage":            "",
                "company_size":     "",
                "founded":          "",
                "website":          "",
                "company_tags":     [],
                "relevance_score":  0,
                "relevance_reason": "",
            },
            "what_they_care_about": {
                "primary_interests":  [bsig.get("posts_about", "")] if bsig.get("posts_about") else [],
                "also_interested_in": [bsig.get("engages_with", "")] if bsig.get("engages_with") else [],
                "passion_signals":    [],
            },
            "online_behaviour": {
                "activity_level":   "",
                "style":            bsig.get("communication_style", ""),
                "recurring_themes": [],
                "platform":         "LinkedIn",
            },
            "communication": {
                "tone":          "",
                "writing_style": bsig.get("communication_style", ""),
                "emotional_mode": "",
                "archetype":     "",
                "mirror_tip":    "",
            },
            "what_drives_them": {
                "values":      [],
                "motivators":  [],
                "pain_points": [bsig.get("pain_point_hint", "")] if bsig.get("pain_point_hint") else [],
                "ambitions":   [],
            },
            "buying_signals": {
                "intent_level":   brkdown.get("score_tier", ""),
                "trigger_events": [],
                "tools_used":     [],
                "decision_style": bsig.get("decision_pattern", ""),
                "intent_tags":    tags[:5],
            },
            "smart_tags": tags[:8],
            "outreach_blueprint": {
                "best_channel":      "",
                "best_approach":     pitch.get("best_angle", ""),
                "opening_hook_1":    pitch.get("suggested_cta", ""),
                "opening_hook_2":    "",
                "content_to_send":   "",
                "topics_to_avoid":   pitch.get("do_not_pitch") or [],
                "one_line_strategy": pitch.get("best_value_prop", ""),
            },
            "crm_scores": {
                "icp_fit":    brkdown.get("icp_fit_score", 0),
                "engagement": brkdown.get("intent_score", 0),
                "timing":     brkdown.get("timing_score", 0),
                "priority":   brkdown.get("score_tier", ""),
            },
            "crm_import_fields": {
                "buyer_type":     prof.get("seniority_level", ""),
                "buying_signal":   bsig.get("warm_signal", "") or "",
                "outreach_tone":   "",
                "hook_theme":      pitch.get("best_angle", ""),
                "avoidance":       ", ".join(pitch.get("do_not_pitch") or []),
                "tags":            tags[:5],
                "analyst_summary": (
                    f"{ident.get('name', '')} is a {ident.get('title', '')} at {ident.get('company', '')}. "
                    f"{bsig.get('pain_point_hint', '')}. "
                    f"{pitch.get('best_value_prop', '') or pitch.get('best_angle', '')}"
                ).strip(". ") + "." if ident.get("name") else pitch.get("top_pain_point", ""),
            },
            "recent_posts_summary": [
                {
                    "topic":         (p.get("text") or p.get("title") or "")[:80],
                    "tone":          "",
                    "key_message":   (p.get("text") or "")[:150],
                    "engagement":    str(p.get("likes") or p.get("num_likes") or 0),
                    "intent_signal": "",
                }
                for p in posts[:5]
            ] or [{"topic": "", "tone": "", "key_message": "", "engagement": "", "intent_signal": ""}],
            "recent_interactions_summary": [
                {
                    "type":          f.get("interaction", ""),
                    "content_topic": (f.get("title") or "")[:80],
                    "why_it_matters": "",
                    "intent_signal":  "",
                }
                for f in feed[:5]
            ] or [{"type": "", "content_topic": "", "why_it_matters": "", "intent_signal": ""}],
        }

    # ── Build enrichment_data matching exact LIO system prompt JSON schema ──────
    # _crm_brief (LLM-generated) takes priority if present.
    # Otherwise build from all available DB fields + raw BD data.
    full_data  = _parse_json_safe(lead.get("full_data"), {})
    wi         = _parse_json_safe(lead.get("website_intelligence"), {})
    raw_bd     = _parse_json_safe(lead.get("raw_brightdata"), {})
    tags_raw   = _parse_json_safe(lead.get("tags") or lead.get("auto_tags"), [])
    tags       = tags_raw if isinstance(tags_raw, list) else []
    skills     = _parse_json_safe(lead.get("top_skills"), [])
    tech_stack = _parse_json_safe(lead.get("tech_stack"), [])
    co_tags    = _parse_json_safe(lead.get("company_tags"), [])
    prod_offer = _parse_json_safe(lead.get("product_offerings"), [])
    tgt_custs  = _parse_json_safe(lead.get("target_customers"), [])

    # Raw BD experience + posts + activity
    bd_experience  = raw_bd.get("experience") or []
    bd_posts       = raw_bd.get("posts") or full_data.get("linkedin_posts") or []
    bd_activity    = raw_bd.get("activity") or full_data.get("activity_full") or []
    bd_current_co  = raw_bd.get("current_company") or {}

    # Derived signals
    seniority     = lead.get("seniority_level") or ""
    is_decision_maker = any(x in seniority.lower() for x in ("c-level","vp","director","founder","ceo","cto","coo","cmo","head","president","partner","managing"))
    trigger_events = [s for s in [
        lead.get("recent_funding_event"), lead.get("hiring_signal"),
        lead.get("job_change"), lead.get("news_mention"),
        lead.get("product_launch"), lead.get("competitor_usage"),
        lead.get("review_activity"), lead.get("linkedin_activity"),
    ] if s]

    # Build 3 post objects from BD posts
    def _post_obj(p: dict) -> dict:
        return {
            "topic":       (p.get("title") or p.get("text") or "")[:80],
            "tone":        "professional",
            "key_message": (p.get("text") or p.get("title") or "")[:150],
            "engagement":  str(p.get("num_likes") or p.get("likes") or 0),
            "intent_signal": "thought leadership" if p.get("num_likes", 0) > 50 else "awareness",
        }

    def _interaction_obj(a: dict) -> dict:
        return {
            "interaction_type": a.get("action") or a.get("interaction") or "engagement",
            "topic":            (a.get("title") or a.get("text") or a.get("attribution") or "")[:80],
            "insight":          (a.get("text") or a.get("title") or "")[:120],
            "intent_signal":    "active interest",
        }

    posts_data        = [_post_obj(p) for p in bd_posts[:3]]
    interactions_data = [_interaction_obj(a) for a in bd_activity[:3]]

    # Only include real BrightData posts/interactions — no fake padding

    if _crm_brief:
        enrichment_data = _crm_brief
    else:
        enrichment_data = {
            "who_they_are": {
                "name":           lead.get("name", ""),
                "title":          lead.get("title") or bd_current_co.get("title") or seniority or "",
                "company":        lead.get("company", ""),
                "location":       lead.get("city") or lead.get("country") or "",
                "lead_id":        lead_id,
                "linkedin_url":   lead.get("linkedin_url", ""),
                "profile_image":  lead.get("avatar_url") or "",
                "company_logo":   lead.get("company_logo") or "",
                "followers":      str(lead.get("followers") or 0),
                "connections":    str(lead.get("connections") or 0),
                "persona":        seniority or "",
                "seniority":      seniority or "",
                "trajectory":     lead.get("years_in_role") or "",
                "decision_maker": "Yes" if is_decision_maker else "No",
            },
            "their_company": {
                "type":             lead.get("business_model") or wi.get("business_model") or "",
                "industry":         lead.get("industry") or wi.get("industry") or "",
                "stage":            lead.get("funding_stage") or "",
                "company_size":     str(lead.get("employee_count") or bd_current_co.get("employees") or ""),
                "founded":          lead.get("founded_year") or "",
                "website":          lead.get("company_website") or "",
                "company_tags":     (co_tags or prod_offer)[:5],
                "relevance_score":  str(lead.get("company_score") or lead.get("total_score") or ""),
                "relevance_reason": lead.get("company_description") or wi.get("company_description") or "",
            },
            "contact": {
                "work_email":       email,
                "phone":            phone,
                "email_source":     lead.get("email_source", ""),
                "email_confidence": lead.get("email_confidence", ""),
                "email_verified":   bool(lead.get("email_verified")),
                "bounce_risk":      lead.get("bounce_risk", ""),
            },
            "what_they_care_about": {
                "primary_interests":   (skills or prod_offer)[:5],
                "secondary_interests": tgt_custs[:5],
                "passion_signals":     [t for t in tags if t][:5],
            },
            "online_behaviour": {
                "activity_level":   "active" if (lead.get("followers") or 0) > 500 else "",
                "content_style":    "",
                "recurring_themes": [t for t in (skills + tags) if t][:5],
                "primary_platform": "LinkedIn",
            },
            "communication": {
                "tone":            "",
                "writing_style":   "",
                "emotional_mode":  "",
                "archetype":       seniority or "",
                "mirror_strategy": "",
            },
            "what_drives_them": {
                "core_values":      skills[:5] if skills else [],
                "motivators":       [s for s in trigger_events if s][:5],
                "pain_points":      ([lead["score_explanation"]] if lead.get("score_explanation") else []) + tags[:4],
                "career_ambitions": [],
            },
            "buying_signals": {
                "intent_level":   lead.get("score_tier") or "",
                "trigger_events": [s for s in trigger_events if s][:5],
                "tools_used":     (tech_stack or prod_offer)[:5],
                "decision_style": "",
                "intent_tags":    tags[:5],
            },
            "smart_tags": [t for t in tags if t][:8],
            "outreach_blueprint": {
                "best_channel":        lead.get("best_channel") or "",
                "approach_strategy":   lead.get("outreach_angle") or "",
                "opening_hooks":       [h for h in [
                    lead.get("email_subject"),
                    lead.get("linkedin_note"),
                    lead.get("outreach_angle"),
                ] if h][:5],
                "recommended_content": lead.get("cold_email") or "",
                "avoid_topics":        [],
                "one_line_strategy":   lead.get("outreach_angle") or lead.get("score_explanation") or "",
            },
            "crm_scores": {
                "icp_fit":        str(lead.get("icp_fit_score") or 0),
                "engagement_score": str(lead.get("intent_score") or 0),
                "timing_score":   str(lead.get("timing_score") or 0),
                "priority_level": lead.get("score_tier") or "",
            },
            "crm_import_fields": {
                "buyer_type":      seniority or "",
                "buying_signal":   lead.get("warm_signal") or (trigger_events[0] if trigger_events else ""),
                "outreach_tone":   lead.get("sequence_type") or "",
                "hook_theme":      lead.get("outreach_angle") or "",
                "avoidance":       "",
                "tags":            [t for t in tags if t][:5],
                "analyst_summary": lead.get("score_explanation") or "",
            },
            "recent_activity": {
                "posts":        posts_data,        # only real BD posts, no padding
                "interactions": interactions_data,  # only real BD activity, no padding
            },
            "company_intelligence": {
                "description":       lead.get("company_description") or wi.get("company_description") or "",
                "value_proposition": lead.get("value_proposition") or wi.get("value_proposition") or "",
                "product_category":  lead.get("product_category") or wi.get("product_category") or "",
                "product_offerings": prod_offer[:10],
                "target_customers":  tgt_custs,
                "pricing_signals":   lead.get("pricing_signals") or wi.get("pricing_signals") or "",
            },
        }

    # ── Validate crm_brief — null, empty {}, or partial JSON ─────────────────
    _org_id_for_regen = lead.get("organization_id") or "default"
    _linkedin_url_for_fail = lead.get("linkedin_url", "")

    if not _validate_crm_brief(_crm_brief):
        # Issue 3 & 4: empty {} or partial/missing required keys
        if _crm_brief is not None:
            logger.warning("[LIO] Lead %s crm_brief is empty or partial — attempting regen", lead_id)
        else:
            logger.warning("[LIO] Lead %s crm_brief is null — attempting regen", lead_id)

        if lead.get("name"):
            try:
                # Lazy import to avoid circular dependency
                from ._enrichment import regenerate_crm_brief_for_lead
                # Await regen directly (we're already in a background task)
                regen_lead = await regenerate_crm_brief_for_lead(lead_id, org_id=_org_id_for_regen)
                if regen_lead:
                    regen_brief_raw = regen_lead.get("crm_brief")
                    regen_brief = None
                    if regen_brief_raw:
                        try:
                            regen_brief = json.loads(regen_brief_raw) if isinstance(regen_brief_raw, str) else regen_brief_raw
                        except Exception:
                            pass
                    if _validate_crm_brief(regen_brief):
                        logger.info("[LIO] Regen successful for lead %s — proceeding to send", lead_id)
                        _crm_brief = regen_brief
                    else:
                        raise RuntimeError("Regen produced invalid crm_brief")
                else:
                    raise RuntimeError("Regen returned no lead")
            except Exception as regen_err:
                logger.error("[LIO] Regen also failed for lead %s: %s — notifying LIO as failed", lead_id, regen_err)
                await send_to_lio_failed(_linkedin_url_for_fail, _org_id_for_regen, sso_id, reason="llm_failed")
                return
        else:
            logger.warning("[LIO] Lead %s has no name — cannot regen, notifying LIO as failed", lead_id)
            await send_to_lio_failed(_linkedin_url_for_fail, _org_id_for_regen, sso_id, reason="llm_failed")
            return

    _crm_brief["status"] = "success"
    _crm_brief["input_url"] = lead.get("linkedin_url") or ""
    payload = {
        "enrichment_data": _crm_brief,
        "sso_id":          sso_id,
        "organization_id": lead.get("organization_id", ""),
    }

    logger.info("[LIO] Sending lead %s to %s | sso_id=%s | org=%s",
                lead_id, url, sso_id, lead.get("organization_id", ""))
    _pipeline_log.info("[LIO-PAYLOAD] lead=%s\n%s", lead_id, json.dumps(payload, indent=2, default=str))

    # Retry with exponential backoff: 3 attempts, 5s / 10s delays between them
    _LIO_MAX_ATTEMPTS = 3
    for attempt in range(1, _LIO_MAX_ATTEMPTS + 1):
        try:
            client = _get_lio_client()
            resp = await client.post(url, json=payload)
            logger.info("[LIO] Response for lead %s → HTTP %s | body=%s",
                        lead_id, resp.status_code, resp.text[:500])
            resp.raise_for_status()
            logger.info("[LIO] Successfully forwarded lead %s (attempt %d)", lead_id, attempt)
            # Mark as sent in DB — primary idempotency record (Redis-independent)
            try:
                async with get_pool().acquire() as _conn:
                    await _conn.execute(
                        "UPDATE enriched_leads SET lio_sent_at=NOW() WHERE id=$1", lead_id
                    )
            except Exception:
                pass
            # Mark as sent in Redis (24h TTL) — fast-path cache
            try:
                import redis.asyncio as aioredis
                _redis_url = os.getenv("REDIS_URL", "")
                if _redis_url:
                    _r = aioredis.from_url(_redis_url, decode_responses=True)
                    await _r.set(_lio_sent_key, "1", ex=86400)
                    await _r.close()
            except Exception:
                pass
            return  # success — exit retry loop
        except httpx.TimeoutException:
            logger.warning("[LIO] Timeout lead %s (attempt %d/%d)", lead_id, attempt, _LIO_MAX_ATTEMPTS)
        except httpx.ConnectError as e:
            logger.warning("[LIO] Connection error lead %s (attempt %d/%d): %s", lead_id, attempt, _LIO_MAX_ATTEMPTS, e)
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            logger.warning("[LIO] HTTP %s lead %s (attempt %d/%d) | body=%s",
                           status, lead_id, attempt, _LIO_MAX_ATTEMPTS, e.response.text[:300])
            if status < 500 and status != 429:
                # 4xx errors (except rate-limit) are not retryable — go straight to DLQ
                await _lio_dlq_push(lead_id, payload, reason=f"HTTP {status}")
                return
        except Exception as e:
            logger.warning("[LIO] Unexpected error lead %s (attempt %d/%d): %s", lead_id, attempt, _LIO_MAX_ATTEMPTS, e)

        if attempt < _LIO_MAX_ATTEMPTS:
            await asyncio.sleep(1 * attempt)  # 1s then 2s before final attempt

    # All attempts exhausted — push to dead-letter queue for manual replay
    logger.error("[LIO] All %d attempts failed for lead %s — pushing to DLQ", _LIO_MAX_ATTEMPTS, lead_id)
    await _lio_dlq_push(lead_id, payload, reason="max_retries_exceeded")


# ─────────────────────────────────────────────────────────────────────────────
# Default CRM Brief system prompt — used when lio_system_prompt is not set in DB
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_CRM_BRIEF_PROMPT = """You are a high-performance B2B sales intelligence analyst.

Your task is to analyze LinkedIn raw data (profile + posts + interactions + company context) and generate structured, insight-rich lead intelligence.

STRICT RULES:
- Return ONLY valid JSON — no markdown, no code fences, no extra text before or after.
- Use ONLY data explicitly present in the input. Do NOT invent, infer, or fabricate any values.
- If data is missing for a field → use "" for strings, [] for arrays, 0 for numbers. Never pad with invented items.
- Arrays may have 0 items when the input provides no evidence — do NOT fill to reach a minimum count.
- Synthesize insights from actual signals only — do not copy verbatim, but only when the signal genuinely exists in the data.
- Avoid generic B2B filler phrases (e.g. "results-driven", "passionate about", "career growth", "business impact", "innovative").

IDENTITY RULES (NO HALLUCINATION):
- who_they_are.company MUST be taken EXACTLY from company_context.name in the input. Do NOT use any company from the experience array. Do NOT infer or invent a company name. If company_context.name is present, use it verbatim.
- who_they_are.title MUST be taken from the headline/title field in the input. Only infer from experience[0].title if headline is completely absent.
- their_company fields (industry, website, stage, etc.) MUST be derived from company_context in the input. Do NOT substitute with any other company from experience history.

ANALYSIS LOGIC:
- authored_posts → define expertise, identity, and authority.
- interactions (likes/comments/reposts) → indicate buying intent and interest areas.
- company_context → derive business challenges, growth stage, and priorities.
- Pain points MUST be specific and derived from the data — not generic.
- Buying signals MUST come from observed behaviour, not assumptions.
- Trigger events = recent role changes, company news, posts about challenges.
- Seniority and decision_maker fields MUST be inferred from title + experience.
- crm_scores MUST use INTEGER values (0–100) for icp_fit, engagement_score, timing_score. priority_level MUST be one of: "High", "Medium", "Low".
- crm_import_fields.analyst_summary MUST be 2–3 sentences in third person: (1) who they are professionally and what they do, (2) what makes them engaged or notable — expertise, community role, recent achievements, (3) why they are a valuable sales prospect. Be specific to THIS person. Do NOT use phrases like "results-driven" or "passionate about". Example: "Sarah Chen is a VP of Engineering at a Series B fintech startup scaling their payments infrastructure globally. She actively publishes about distributed systems challenges and engages with DevOps tooling content, signalling active vendor evaluation. Her decision-making authority and rapid growth context make her a high-priority prospect for infrastructure solutions."

OUTPUT — return ONLY this JSON with ALL fields fully populated (arrays must have 4–5 items each):
{"who_they_are":{"name":"","title":"","company":"","location":"","linkedin_url":"","profile_image":"","company_logo":"","followers":"","connections":"","persona":"","seniority":"","trajectory":"","decision_maker":""},"their_company":{"type":"","industry":"","stage":"","company_size":"","founded":"","website":"","company_tags":["","","","",""],"relevance_score":"","relevance_reason":""},"what_they_care_about":{"primary_interests":["","","","",""],"secondary_interests":["","","","",""],"passion_signals":["","","","",""]},"online_behaviour":{"activity_level":"","content_style":"","recurring_themes":["","","","",""],"primary_platform":""},"communication":{"tone":"","writing_style":"","emotional_mode":"","archetype":"","mirror_strategy":""},"what_drives_them":{"core_values":["","","","",""],"motivators":["","","","",""],"pain_points":["","","","",""],"career_ambitions":["","","","",""]},"buying_signals":{"intent_level":"","trigger_events":["","","","",""],"tools_used":["","","","",""],"decision_style":"","intent_tags":["","","","",""]},"smart_tags":["","","","",""],"outreach_blueprint":{"best_channel":"","approach_strategy":"","opening_hooks":["","","","",""],"recommended_content":"","avoid_topics":["","","","",""],"one_line_strategy":""},"crm_scores":{"icp_fit":0,"engagement_score":0,"timing_score":0,"priority_level":""},"crm_import_fields":{"buyer_type":"","buying_signal":"","outreach_tone":"","hook_theme":"","avoidance":"","tags":["","","","",""],"analyst_summary":""},"recent_activity":{"posts":[{"topic":"","tone":"","key_message":"","engagement":"","intent_signal":""},{"topic":"","tone":"","key_message":"","engagement":"","intent_signal":""},{"topic":"","tone":"","key_message":"","engagement":"","intent_signal":""}],"interactions":[{"interaction_type":"","topic":"","insight":"","intent_signal":""},{"interaction_type":"","topic":"","insight":"","intent_signal":""},{"interaction_type":"","topic":"","insight":"","intent_signal":""}]}}"""


# ─────────────────────────────────────────────────────────────────────────────
# Comprehensive 8-stage LLM enrichment
# ─────────────────────────────────────────────────────────────────────────────

def _build_comprehensive_prompt(
    linkedin_url: str,
    profile: dict,
    contact: dict,
    now_iso: str,
    website_intel: dict = None,
) -> str:
    name        = profile.get("name", "")
    title       = profile.get("position") or profile.get("headline", "")
    company     = profile.get("current_company_name", "")
    website     = profile.get("current_company_link", "")
    location    = profile.get("location") or profile.get("city", "")
    about       = (profile.get("about") or "")[:600]
    followers   = profile.get("followers", 0)
    connections = profile.get("connections", 0)
    skills      = json.dumps(profile.get("skills") or [])
    experience  = json.dumps((profile.get("experience") or [])[:5])
    education   = json.dumps((profile.get("education") or profile.get("educations_details") or [])[:5])
    certs       = json.dumps(profile.get("certifications") or [])
    langs       = json.dumps(profile.get("languages") or [])
    email       = contact.get("email") or ""
    phone       = contact.get("phone") or ""
    email_src   = contact.get("source") or ""
    email_conf  = contact.get("confidence") or ""

    # Company extras from waterfall (injected into profile by enrich_single)
    company_extras = profile.get("_company_extras") or {}

    # Website intelligence section
    wi = website_intel or {}
    wi_section = f"""
Website Intelligence (Stage 3 — Scraped):
  Company Description: {wi.get('company_description', '')}
  Product Offerings: {json.dumps(wi.get('product_offerings', []))}
  Value Proposition: {wi.get('value_proposition', '')}
  Target Customers: {json.dumps(wi.get('target_customers', []))}
  Business Model: {wi.get('business_model', '')}
  Product Category: {wi.get('product_category', '')}
  Pricing Signals: {wi.get('pricing_signals', '')}
  Market Positioning: {wi.get('market_positioning', '')}
  Problem Solved: {wi.get('problem_solved', '')}
  Hiring Signals: {wi.get('hiring_signals', '')}
  Pages Scraped: {json.dumps(wi.get('pages_scraped', []))}""" if wi else "Website Intelligence: Not available"

    return f"""You are a B2B lead intelligence analyst for Worksbuddy, a CRM & productivity platform.

Generate a COMPREHENSIVE 8-stage lead enrichment report for the following profile.
Use the provided data. For fields not in the data, use your knowledge and reasonable inference.
Be specific, professional, and accurate.

--- RAW DATA ---
LinkedIn URL: {linkedin_url}
Name: {name}
Title: {title}
Company: {company}
Website: {website}
Location: {location}
About: {about}
Followers: {followers}  |  Connections: {connections}
Email: {email} ({email_src}, {email_conf})
Phone: {phone}
Skills: {skills}
Experience: {experience}
Education: {education}
Certifications: {certs}
Languages: {langs}
Enrichment date: {now_iso}
Company Logo: {company_extras.get('company_logo', '')}
Company Email: {company_extras.get('company_email', '')}
Company Phone: {company_extras.get('company_phone', '')}
Company Description: {(company_extras.get('company_description') or '')[:300]}
Tech Stack: {json.dumps(company_extras.get('tech_stack', []))}
Employee Count: {company_extras.get('employee_count', 0)}
Industry: {company_extras.get('industry', '')}
{wi_section}
---

[Stage 1] Person Profile
[Stage 2] Company Identity
[Stage 3] Website Intelligence (use scraped data above verbatim where available)
[Stage 4] Company Profile
[Stage 5] Market Signals
[Stage 6] Intent Signals
[Stage 7] Scoring
[Stage 8] Outreach

Return ONLY a valid JSON object with ALL of these exact keys.
Use empty string "" for unknown text, 0 for unknown numbers, [] for unknown arrays, null for unknown optional fields.

{{
  "person_profile": {{
    "full_name": "{name}",
    "first_name": "",
    "last_name": "",
    "current_title": "",
    "seniority_level": "",
    "department": "",
    "years_in_role": "",
    "career_history": [],
    "top_skills": [],
    "education": "",
    "certifications": [],
    "languages": [],
    "city": "",
    "country": "",
    "timezone": "",
    "linkedin_activity": "",
    "followers": 0,
    "connections": 0,
    "work_email": "",
    "personal_email": null,
    "direct_phone": null,
    "twitter": null,
    "about": ""
  }},
  "company_identity": {{
    "name": "",
    "domain": "",
    "website": "",
    "linkedin_url": ""
  }},
  "website_intelligence": {{
    "company_description": "",
    "product_offerings": [],
    "value_proposition": "",
    "target_customers": [],
    "use_cases": [],
    "business_model": "",
    "product_category": "",
    "market_positioning": "",
    "pricing_signals": "",
    "key_messaging": [],
    "hiring_signals": "",
    "open_roles": [],
    "problem_solved": ""
  }},
  "company_profile": {{
    "name": "",
    "industry": "",
    "employee_count": 0,
    "hq_location": "",
    "founded_year": null,
    "annual_revenue_est": "",
    "tech_stack": [],
    "company_logo": null,
    "company_email": null,
    "company_phone": null,
    "company_twitter": null
  }},
  "company_intelligence": {{
    "funding_stage": "",
    "total_funding": "",
    "last_funding_date": "",
    "lead_investor": "",
    "hiring_velocity": "",
    "department_hiring_trends": [],
    "news_mention": "",
    "product_launch": "",
    "competitor_usage": ""
  }},
  "intent_signals": {{
    "recent_funding_event": "",
    "hiring_signal": "",
    "job_change": "",
    "linkedin_activity": "",
    "news_mention": "",
    "product_launch": "",
    "competitor_usage": "",
    "review_activity": "",
    "pain_indicators": []
  }},
  "lead_scoring": {{
    "icp_fit_score": 0,
    "intent_score": 0,
    "timing_score": 0,
    "data_completeness_score": 0,
    "overall_score": 0,
    "score_tier": "HOT|WARM|COOL|COLD",
    "score_explanation": "",
    "icp_match_tier": "",
    "disqualification_flags": []
  }},
  "outreach": {{
    "email_subject": "",
    "cold_email": "",
    "linkedin_note": "",
    "best_channel": "",
    "best_send_time": "",
    "outreach_angle": "",
    "sequence_type": "",
    "sequence": {{
      "day1": "Send cold email",
      "day2": "LinkedIn connection request with note",
      "day4": "Follow-up email — value proof",
      "day7": "LinkedIn message or content engage",
      "day14": "Breakup email"
    }},
    "last_contacted": null,
    "email_status": ""
  }},
  "crm": {{
    "lead_source": "LinkedIn URL",
    "enrichment_source": "Bright Data + Apollo",
    "enrichment_date": "{now_iso}",
    "enrichment_depth": "Deep",
    "data_completeness": 0,
    "last_re_enriched": "{now_iso}",
    "assigned_owner": null,
    "crm_stage": "",
    "tags": []
  }}
}}

SCORING GUIDE:
- icp_fit_score (0-40): C-Level/Founder=35+, VP=28-35, Director=20-27, Manager=12-19, IC=0-11; company size fit adds up to 10
- intent_score (0-30): recent funding=15, active hiring surge=10, job change in 90 days=8, linkedin active=5
- timing_score (0-20): funding in last 6mo=15, job change in 30 days=12, product launch=10, blog/news active=5
- data_completeness_score (0-10): % of key fields filled (email=3, phone=1, company=2, title=1, linkedin=1, skills=1, edu=1)
- overall_score = icp + intent + timing + data_completeness (max 100)
- score_tier: HOT (80+), WARM (55-79), COOL (30-54), COLD (<30)
- icp_match_tier: "Tier 1 — Hot" / "Tier 2 — Warm" / "Tier 3 — Cool" / "Tier 4 — Cold"
- crm_stage: based on score — "MQL → Ready for outreach" / "Nurture" / "Monitor" / "Disqualify"
- data_completeness (crm section): % of fields filled (0-100)
- OUTREACH: Use website_intelligence.value_proposition and website_intelligence.problem_solved as primary angle
- Cold email: max 120 words, natural tone, reference real company context
- LinkedIn note: max 300 chars
- email_status: "Valid — verified" if apollo found it, "Pattern guess — unverified" otherwise
- tags: 3-6 relevant tags like ["SaaS","VP-Eng","Series-B","Hot","San Francisco"]"""


def _render_lio_template(template: str, vars: dict) -> str:
    """Fill {placeholders} in a LIO user_template; missing keys become empty string."""
    class _SafeDict(dict):
        def __missing__(self, key):
            return ""
    return template.format_map(_SafeDict(vars))


def _parse_list_from_llm(raw: str) -> list:
    """Extract first JSON array from LLM response."""
    try:
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if m:
            result = json.loads(m.group())
            if isinstance(result, list):
                return result
    except Exception:
        pass
    return []


async def run_lio_pipeline(
    profile: dict,
    contact: dict,
    org_id: str = "default",
) -> dict:
    """
    Run all 7 LIO stages using the org's configured prompts + workspace context.

    Stages:
      0 — Company Intelligence
      1 — Auto Tags
      2 — Behavioural Signals
      3 — Buying Signals        (needs 0 + 2)
      4 — Pitch Intelligence    (needs 0 + 2 + 3 + workspace context)
      5 — Outreach Generator    (needs 2 + 4 + workspace context)
      6 — Lead Score            (needs 2 + 3)

    Returns dict with keys:
      company_intel, tags, behavioural_signals, buying_signals,
      pitch_intelligence, outreach, lead_score
    """
    from config.enrichment_config_service import get_lio_prompts, get_workspace_context

    prompts = await get_lio_prompts(org_id)
    ctx     = await get_workspace_context(org_id)

    name    = profile.get("name", "")
    first, last = _parse_name(name)
    title   = profile.get("position") or profile.get("headline", "")
    company = profile.get("current_company_name", "")
    company_extras = profile.get("_company_extras") or {}
    industry = company_extras.get("industry", "")
    city     = profile.get("city", "") or profile.get("location", "")
    country  = profile.get("country", "")
    email    = contact.get("email") or ""

    # Compact profile for LLM — strip heavy raw fields to stay within token limits
    compact = {k: v for k, v in profile.items() if k not in ("_activity_full", "_posts")}
    raw_json = json.dumps(compact, default=str)[:6000]

    results: dict = {}

    # ── Stage 0: Company Intelligence ─────────────────────────────────────────
    p = prompts[0]
    try:
        user = _render_lio_template(p["user_template"], {"raw_brightdata_json": raw_json})
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=400, temperature=p.get("temperature", 0.2), model_override=p.get("model"),
        )
        results["company_intel"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:0] company_intel failed: %s", e)
        results["company_intel"] = {}

    # ── Stage 1: Auto Tags ─────────────────────────────────────────────────────
    p = prompts[1]
    try:
        user = _render_lio_template(p["user_template"], {"raw_brightdata_json": raw_json})
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=200, temperature=p.get("temperature", 0.3), model_override=p.get("model"),
        )
        results["tags"] = _parse_list_from_llm(raw) if raw else []
    except Exception as e:
        logger.warning("[LIO:1] auto_tags failed: %s", e)
        results["tags"] = []

    # ── Stage 2: Behavioural Signals ──────────────────────────────────────────
    p = prompts[2]
    try:
        user = _render_lio_template(p["user_template"], {"raw_brightdata_json": raw_json})
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=400, temperature=p.get("temperature", 0.3), model_override=p.get("model"),
        )
        results["behavioural_signals"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:2] behavioural_signals failed: %s", e)
        results["behavioural_signals"] = {}

    # ── Stage 3: Buying Signals ────────────────────────────────────────────────
    p = prompts[3]
    try:
        user = _render_lio_template(p["user_template"], {
            "raw_brightdata_json":  raw_json,
            "behavioural_signals":  json.dumps(results["behavioural_signals"]),
            "company_intel":        json.dumps(results["company_intel"]),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=300, temperature=p.get("temperature", 0.2), model_override=p.get("model"),
        )
        results["buying_signals"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:3] buying_signals failed: %s", e)
        results["buying_signals"] = {}

    # ── Stage 4: Pitch Intelligence ────────────────────────────────────────────
    p = prompts[4]
    try:
        user = _render_lio_template(p["user_template"], {
            "first_name":          first,
            "last_name":           last,
            "inferred_title":      title,
            "company":             company,
            "industry":            industry,
            "behavioural_signals": json.dumps(results["behavioural_signals"]),
            "buying_signals":      json.dumps(results["buying_signals"]),
            "company_intel":       json.dumps(results["company_intel"]),
            "product_name":        ctx.get("product_name", ""),
            "value_proposition":   ctx.get("value_proposition", ""),
            "tone":                ctx.get("tone", "professional"),
            "banned_phrases":      ctx.get("banned_phrases", ""),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=500, temperature=p.get("temperature", 0.4), model_override=p.get("model"),
        )
        results["pitch_intelligence"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:4] pitch_intelligence failed: %s", e)
        results["pitch_intelligence"] = {}

    # ── Stage 5: Outreach Generator ────────────────────────────────────────────
    p = prompts[5]
    try:
        user = _render_lio_template(p["user_template"], {
            "first_name":          first,
            "last_name":           last,
            "inferred_title":      title,
            "company":             company,
            "city":                city,
            "country":             country,
            "pitch_intelligence":  json.dumps(results["pitch_intelligence"]),
            "behavioural_signals": json.dumps(results["behavioural_signals"]),
            "product_name":        ctx.get("product_name", ""),
            "tone":                ctx.get("tone", "professional"),
            "cta_style":           ctx.get("cta_style", "question"),
            "banned_phrases":      ctx.get("banned_phrases", ""),
            "case_study":          ctx.get("case_study", ""),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=600, temperature=p.get("temperature", 0.7), model_override=p.get("model"),
        )
        results["outreach"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:5] outreach failed: %s", e)
        results["outreach"] = {}

    # ── Stage 6: Lead Score ────────────────────────────────────────────────────
    p = prompts[6]
    try:
        buying = results.get("buying_signals", {})
        warm_count = len(profile.get("_activity_full") or [])
        user = _render_lio_template(p["user_template"], {
            "first_name":          first,
            "last_name":           last,
            "inferred_title":      title,
            "company":             company,
            "industry":            industry,
            "icp_fit_score":       0,
            "intent_level":        buying.get("intent_level", "medium"),
            "timing_score":        buying.get("timing_score", 0),
            "warm_signal_count":   warm_count,
            "email_status":        "found" if email else "not_found",
            "company_stage":       results["company_intel"].get("company_stage", ""),
            "behavioural_signals": json.dumps(results["behavioural_signals"]),
            "buying_signals":      json.dumps(buying),
        })
        raw  = await _call_llm(
            [{"role": "system", "content": p["system"]}, {"role": "user", "content": user}],
            max_tokens=300, temperature=p.get("temperature", 0.2), model_override=p.get("model"),
        )
        results["lead_score"] = _parse_json_from_llm(raw) if raw else {}
    except Exception as e:
        logger.warning("[LIO:6] lead_score failed: %s", e)
        results["lead_score"] = {}

    logger.info(
        "[LIO Pipeline] org=%s  company_intel=%s  tags=%d  behavioural=%s  "
        "buying=%s  pitch=%s  outreach=%s  score=%s",
        org_id,
        bool(results["company_intel"]), len(results["tags"]),
        bool(results["behavioural_signals"]), bool(results["buying_signals"]),
        bool(results["pitch_intelligence"]), bool(results["outreach"]),
        bool(results["lead_score"]),
    )
    return results


async def generate_outreach_with_lio(lead: dict, org_id: str = "default") -> dict:
    """
    Generate outreach for a lead using LIO Stage 5 (Outreach Generator) system + user prompt
    from the org's LIO config.  Uses profile data already stored on the lead row.

    Returns:
        {
          "cold_email":    {"subject": ..., "body": ...},
          "linkedin_note": ...,
          "linkedin_follow_up": ...,
        }
    """
    from config.enrichment_config_service import get_lio_prompts, get_workspace_context

    prompts = await get_lio_prompts(org_id)
    ctx     = await get_workspace_context(org_id)
    p5      = prompts[5]   # Stage 5 — Outreach Generator

    # ── Build vars from stored lead fields ────────────────────────────────────
    name = lead.get("name") or ""
    first, last = _parse_name(name)
    title   = lead.get("title")   or ""
    company = lead.get("company") or ""
    city    = lead.get("city")    or ""
    country = lead.get("country") or ""

    # ── Pull enrichment fields from top-level lead columns (primary) and full_data (fallback) ──
    full = {}
    try:
        raw_fd = lead.get("full_data")
        full = json.loads(raw_fd) if isinstance(raw_fd, str) else (raw_fd or {})
    except Exception:
        pass

    def _try_json(val):
        if not val:
            return {}
        if isinstance(val, dict):
            return val
        try:
            return json.loads(val)
        except Exception:
            return {}

    # pitch_intelligence is a top-level DB column (JSON string) — preferred over full_data
    pitch_intel = (
        _try_json(lead.get("pitch_intelligence")) or
        full.get("lead_scoring", {}).get("pitch_intelligence") or
        full.get("pitch_intelligence") or
        {}
    )

    # behavioural_signals is a top-level DB column (JSON string)
    behavioural_sigs = (
        _try_json(lead.get("behavioural_signals")) or
        full.get("behavioural_signals") or
        {}
    )

    # activity_feed — top 5 posts for personalization context
    activity_feed_raw = lead.get("activity_feed") or full.get("activity_feed") or []
    if isinstance(activity_feed_raw, str):
        try:
            activity_feed_raw = json.loads(activity_feed_raw)
        except Exception:
            activity_feed_raw = []
    recent_posts = [
        a.get("title", "")[:200]
        for a in (activity_feed_raw[:5] if isinstance(activity_feed_raw, list) else [])
        if a.get("title")
    ]

    # about / bio
    about = lead.get("about") or full.get("person_profile", {}).get("about") or ""

    vars_map = {
        "first_name":          first,
        "last_name":           last,
        "inferred_title":      title,
        "company":             company,
        "city":                city,
        "country":             country,
        "about":               about[:300] if about else "",
        "recent_posts":        json.dumps(recent_posts),
        "pitch_intelligence":  json.dumps(pitch_intel),
        "behavioural_signals": json.dumps(behavioural_sigs),
        "product_name":        ctx.get("product_name", ""),
        "tone":                ctx.get("tone", "professional"),
        "cta_style":           ctx.get("cta_style", "question"),
        "banned_phrases":      ctx.get("banned_phrases", ""),
        "case_study":          ctx.get("case_study", ""),
    }

    user_prompt = _render_lio_template(p5["user_template"], vars_map)

    logger.info(
        "[LIO Outreach] Running Stage 5 for lead=%s org=%s name=%r company=%r",
        lead.get("id"), org_id, name, company,
    )

    raw = await _call_llm(
        [
            {"role": "system", "content": p5["system"]},
            {"role": "user",   "content": user_prompt},
        ],
        max_tokens=2000,
        temperature=p5.get("temperature", 0.7),
        model_override=p5.get("model"),
    )

    if not raw:
        raise RuntimeError(
            "LLM unavailable — outreach could not be generated. "
            "Check that HF_TOKEN or WB_LLM_HOST is configured."
        )

    result = _parse_json_from_llm(raw)
    if not result:
        raise RuntimeError(f"LLM returned unparseable response: {raw[:200]}")

    cold_email_block = result.get("cold_email", {}) or {}
    follow_up_block  = result.get("follow_up",  {}) or {}

    logger.info(
        "[LIO Outreach] Generated for lead=%s — subject=%r note_len=%d",
        lead.get("id"),
        cold_email_block.get("subject", ""),
        len(result.get("linkedin_note", "") or ""),
    )

    full_email = cold_email_block.get("full_email") or ""
    body       = cold_email_block.get("body")       or full_email  # body → full_email fallback

    return {
        "cold_email": {
            "subject":    cold_email_block.get("subject")  or "",
            "greeting":   cold_email_block.get("greeting") or "",
            "opening":    cold_email_block.get("opening")  or "",
            "body":       body,
            "cta":        cold_email_block.get("cta")      or "",
            "sign_off":   cold_email_block.get("sign_off") or "",
            "full_email": full_email or body,   # full_email → body fallback
        },
        "linkedin_note": result.get("linkedin_note") or "",
        "follow_up": {
            "day3": follow_up_block.get("day3") or "",
            "day7": follow_up_block.get("day7") or "",
        },
    }


def _merge_lio_into_enrichment(enrichment: dict, lio: dict) -> dict:
    """
    Overlay LIO pipeline outputs onto the base enrichment dict.
    LIO results take priority for: outreach, scoring, tags, signals, pitch intel.
    """
    # ── Outreach (Stage 5) ─────────────────────────────────────────────────────
    lio_out = lio.get("outreach", {})
    lio_email = lio_out.get("email", {})
    lio_linkedin = lio_out.get("linkedin", {})
    if lio_email or lio_linkedin:
        outreach = enrichment.setdefault("outreach", {})
        if lio_email.get("subject"):
            outreach["email_subject"] = lio_email["subject"]
        if lio_email.get("body"):
            outreach["cold_email"] = lio_email["body"]
        if lio_linkedin.get("connection_note"):
            outreach["linkedin_note"] = lio_linkedin["connection_note"]
        if lio_linkedin.get("follow_up"):
            outreach["linkedin_follow_up"] = lio_linkedin["follow_up"]

    # ── Lead Score (Stage 6) ───────────────────────────────────────────────────
    lio_score = lio.get("lead_score", {})
    if lio_score:
        scoring = enrichment.setdefault("lead_scoring", {})
        if lio_score.get("lead_score") is not None:
            scoring["total_score"] = lio_score["lead_score"]
        if lio_score.get("priority"):
            scoring["icp_match_tier"] = lio_score["priority"]
        if lio_score.get("reason"):
            scoring["score_explanation"] = lio_score["reason"]
        if lio_score.get("next_best_action"):
            scoring["next_best_action"] = lio_score["next_best_action"]

    # ── Pitch Intelligence (Stage 4) ────────────────────────────���─────────────
    if lio.get("pitch_intelligence"):
        enrichment.setdefault("lead_scoring", {})["pitch_intelligence"] = lio["pitch_intelligence"]

    # ── Tags (Stage 1) ─────────────────────────────────────────────────────────
    if lio.get("tags"):
        enrichment.setdefault("crm", {})["tags"] = lio["tags"]

    # ── Behavioural Signals (Stage 2) ──────────────────────────────────────────
    if lio.get("behavioural_signals"):
        enrichment["behavioural_signals"] = lio["behavioural_signals"]

    # ── Buying Signals → Intent Signals (Stage 3) ──────────────────────────────
    buying = lio.get("buying_signals", {})
    if buying:
        intent = enrichment.setdefault("intent_signals", {})
        intent["intent_level"]       = buying.get("intent_level")
        intent["trigger_events"]     = buying.get("trigger_events", [])
        intent["timing_score"]       = buying.get("timing_score")
        intent["recommended_action"] = buying.get("recommended_action")

    # ── Company Intel (Stage 0) ────────────────────────────────────────────────
    if lio.get("company_intel"):
        enrichment["company_intelligence"] = lio["company_intel"]

    return enrichment


async def build_comprehensive_enrichment(
    linkedin_url: str,
    profile: dict,
    contact: dict,
    website_intel: dict = None,
    org_id: str = "default",
    system_prompt_override: Optional[str] = None,
) -> dict:
    """
    Main LLM call to produce the full 8-stage enrichment JSON.
    System prompt and model are read from the org's LIO config (workspace_configs).
    Falls back to a rule-based assembly if LLM is unavailable.
    If system_prompt_override is provided it takes precedence over the config value.
    """
    from config.enrichment_config_service import get_workspace_config, get_scoring_config

    now    = datetime.now(timezone.utc).isoformat()

    # ── LLM title inference — only when no title could be extracted ───────────
    if profile.get("_needs_title_inference"):
        name    = profile.get("name") or ""
        company = profile.get("current_company_name") or ""
        about   = (profile.get("about") or "")[:400]
        exp     = profile.get("experience") or []
        exp_ctx = ""
        if exp and isinstance(exp, list) and isinstance(exp[0], dict):
            e0 = exp[0]
            exp_ctx = f"{e0.get('title','')} at {e0.get('company','')}".strip(" at")
        try:
            inferred = await _call_llm(
                [
                    {"role": "system", "content": "You extract professional job titles. Reply with ONLY the job title — 2-6 words, no punctuation, no first-person language."},
                    {"role": "user", "content": (
                        f"Person: {name}\nCompany: {company}\nRecent role: {exp_ctx}\nAbout: {about}\n\n"
                        "What is this person's professional job title? Reply with the title only."
                    )},
                ],
                max_tokens=20,
                temperature=0.1,
            )
            if inferred and len(inferred.strip()) > 2:
                profile = dict(profile)  # don't mutate caller's dict
                profile["position"] = inferred.strip().strip('"').strip("'")
                logger.info("[TitleInfer] Generated title for %s: %s", name, profile["position"])
        except Exception as _tie:
            logger.debug("[TitleInfer] Failed: %s", _tie)

    try:
        cfg, scoring_cfg = await asyncio.gather(
            get_workspace_config(org_id),
            get_scoring_config(org_id),
            return_exceptions=True,
        )
        if isinstance(cfg, Exception):
            cfg = {}
        if isinstance(scoring_cfg, Exception):
            scoring_cfg = None
        model_override   = cfg.get("lio_model", "").strip() or None
        crm_brief_prompt = cfg.get("lio_system_prompt", "").strip()
    except Exception:
        model_override   = None
        crm_brief_prompt = ""
        scoring_cfg      = None

    logger.debug(
        "[Enrichment] org=%s  crm_brief_prompt=%s  model=%s",
        org_id,
        "custom" if crm_brief_prompt else "none",
        model_override or "default",
    )

    # Base enrichment from rule-based assembly (scoring, ICP, signals, etc.)
    data = _rule_based_enrichment(linkedin_url, profile, contact, now, website_intel=website_intel, scoring_cfg=scoring_cfg)

    # ── CRM Brief — single LLM call using lio_system_prompt + raw profile ────
    # system: lio_system_prompt from workspace_configs (set in LIO Config UI)
    # user:   raw BrightData profile JSON
    # model:  lio_model from workspace_configs (wb-pro / qwen by default)
    effective_crm_prompt = (
        system_prompt_override.strip() if (system_prompt_override and system_prompt_override.strip())
        else crm_brief_prompt
        or _DEFAULT_CRM_BRIEF_PROMPT
    )
    if effective_crm_prompt:
        try:
            # Send FULL profile — no trimming. Qwen2.5-7B has 128K context window.
            # HF Qwen handles full data with 128K context window.
            _optimized = _build_llm_profile(profile)
            raw_profile_str = json.dumps(_optimized, separators=(",", ":"), ensure_ascii=False, default=str)
            crm_brief_user = f"Analyze this LinkedIn prospect data and return the JSON exactly as specified:\n\n{raw_profile_str}"
            hf_ok = bool(_hf_token())
            logger.info("[Enrichment] CRM brief LLM call — hf=%s org=%s model=Qwen2.5-7B-Instruct", hf_ok, org_id)
            # HuggingFace (Qwen2.5-7B) — hf_first=True skips WB LLM and goes directly to HF router
            brief = await _call_llm([
                {"role": "system", "content": effective_crm_prompt},
                {"role": "user",   "content": crm_brief_user},
            ], max_tokens=4500, temperature=0.3, model_override=model_override, wb_llm_model_override=model_override,
               hf_first=True)
            if not brief:
                logger.warning("[Enrichment] CRM brief — HuggingFace returned None (hf=%s)", hf_ok)
            else:
                import re as _re
                # Strip ALL <think>...</think> blocks (qwen3, deepseek, etc.)
                brief = _re.sub(r"<think>.*?</think>", "", brief, flags=_re.DOTALL)
                # Strip any unclosed <think> block at end
                brief = _re.sub(r"<think>.*", "", brief, flags=_re.DOTALL).strip()
                # Strip any [inferred] markers the model may have added
                brief = brief.replace("[inferred]", "").replace("[Inferred]", "")
                # Extract first complete JSON object using brace counting
                _start = brief.find("{")
                if _start != -1:
                    _depth = 0
                    _end = _start
                    for _i, _ch in enumerate(brief[_start:], _start):
                        if _ch == "{": _depth += 1
                        elif _ch == "}":
                            _depth -= 1
                            if _depth == 0:
                                _end = _i
                                break
                    brief = brief[_start:_end + 1]
                # Try to parse as JSON — store as dict if valid, else raw string
                try:
                    _crm = json.loads(brief)

                    # ── Authoritative sources ─────────────────────────────────
                    _bd_company = (
                        profile.get("current_company_name")
                        or (profile.get("current_company") or {}).get("name")
                        or ""
                    )
                    _bd_title = profile.get("position") or profile.get("headline") or ""
                    _co_extras = profile.get("_company_extras") or {}

                    # ── who_they_are — force company + title from BrightData ──
                    # LLM reads experience array and picks wrong company/title.
                    # current_company.name is the ONLY authoritative source.
                    if isinstance(_crm.get("who_they_are"), dict):
                        if _bd_company:
                            _crm["who_they_are"]["company"] = _bd_company
                        if _bd_title:
                            _crm["who_they_are"]["title"] = _bd_title

                    # ── their_company — override with real waterfall data ─────
                    # If waterfall found no data, store empty — never fake values.
                    if isinstance(_crm.get("their_company"), dict):
                        _tc = _crm["their_company"]
                        _tc["website"]      = _co_extras.get("company_website") or ""
                        _tc["industry"]     = _co_extras.get("industry") or ""
                        _tc["company_size"] = str(_co_extras.get("employee_count") or "")
                        _tc["founded"]      = str(_co_extras.get("founded_year") or "")
                        _tc["stage"]        = _co_extras.get("funding_stage") or ""

                    data["crm_brief"] = _crm
                except Exception:
                    data["crm_brief"] = brief
                logger.info("[Enrichment] CRM brief generated (%d chars)", len(brief))
        except Exception as _be:
            logger.warning("[Enrichment] CRM brief failed: %s", _be)

    return data


def _build_llm_profile(profile: dict) -> dict:
    """
    Extract and clean all fields meaningful for CRM/LLM analysis.
    Strips pure noise (people_also_viewed, HTML duplicates, logo URLs, internal IDs)
    but keeps ALL signal content — experience descriptions, posts, activity, recommendations,
    volunteer work, languages, groups — to maximise model context utilisation.
    Qwen2.5-7B has a 128K context window; we target ~30-40K chars of signal-dense content.
    """
    # ── Experience — full descriptions, drop HTML + logo URLs + internal IDs ───────
    def _clean_exp(items):
        out = []
        for e in (items or []):
            entry = {
                "title":       e.get("title", ""),
                "company":     e.get("company", ""),
                "start_date":  e.get("start_date", ""),
                "end_date":    e.get("end_date", ""),
                "location":    e.get("location", ""),
                "description": (e.get("description") or "")[:2000],  # plain text, cap long essays
            }
            # include sub_positions (career progression within same company)
            subs = e.get("sub_positions") or []
            if subs:
                entry["sub_positions"] = [
                    {
                        "title":       s.get("title", ""),
                        "start_date":  s.get("start_date", ""),
                        "end_date":    s.get("end_date", ""),
                        "description": (s.get("description") or "")[:500],
                    }
                    for s in subs
                ]
            out.append(entry)
        return out

    # ── Activity — full titles, drop image URLs, cap at 50 items ─────────────────
    def _clean_activity(items):
        out = []
        for a in (items or [])[:50]:
            title = (a.get("title") or "").strip()
            if not title:
                continue
            out.append({
                "interaction": a.get("interaction") or "liked",
                "title":       title[:400],
                "link":        a.get("link", ""),
            })
        return out

    # ── Posts — author's own posts (strongest intent signals) ────────────────────
    def _clean_posts(items):
        out = []
        for p in (items or [])[:30]:
            text = (p.get("text") or p.get("content") or p.get("title") or "").strip()
            if not text:
                continue
            out.append({
                "text":        text[:600],
                "likes":       p.get("num_likes") or p.get("likes", 0),
                "comments":    p.get("num_comments") or p.get("comments", 0),
                "posted_at":   p.get("posted_at") or p.get("date", ""),
            })
        return out

    # ── Education — full details ──────────────────────────────────────────────────
    def _clean_edu(items):
        out = []
        for e in (items or []):
            if isinstance(e, dict):
                out.append({
                    "school":       e.get("title") or e.get("school") or e.get("name", ""),
                    "degree":       e.get("subtitle") or e.get("degree", ""),
                    "field":        e.get("field_of_study") or e.get("field", ""),
                    "dates":        f"{e.get('start_year','')}-{e.get('end_year','')}".strip("-"),
                    "description":  (e.get("description") or "")[:300],
                })
        return out

    # ── Recommendations — full text, higher cap ────────────────────────────────────
    def _clean_recs(items):
        out = []
        for r in (items or []):
            if isinstance(r, str):
                text = r
            else:
                text = r.get("description") or r.get("text") or r.get("title") or ""
            if text:
                out.append(str(text)[:600])
        return out

    # ── Volunteer work ────────────────────────────────────────────────────────────
    def _clean_volunteer(items):
        out = []
        for v in (items or []):
            if isinstance(v, dict):
                out.append({
                    "role":        v.get("role") or v.get("title", ""),
                    "org":         v.get("organization") or v.get("company", ""),
                    "cause":       v.get("cause", ""),
                    "description": (v.get("description") or "")[:300],
                })
        return out

    # ── Current company — clean, no logo URLs ────────────────────────────────────
    def _clean_company(co: dict) -> dict:
        if not co:
            return {}
        return {
            "name":        co.get("name", ""),
            "industry":    co.get("industry", ""),
            "size":        co.get("company_size") or co.get("size", ""),
            "founded":     co.get("founded", ""),
            "website":     co.get("website", ""),
            "description": (co.get("description") or "")[:500],
            "specialties": co.get("specialities") or co.get("specialties", []),
        }

    # ── Build maximally signal-dense profile ──────────────────────────────────────
    # Keys are named to match the prompt's ANALYSIS LOGIC terminology:
    #   authored_posts      → expertise, identity, authority
    #   interactions        → buying intent (liked/commented/reposted)
    #   company_context     → challenges, stage, priorities
    _current_co = profile.get("current_company") or {}
    # Authoritative company name: prefer current_company_name (set by enrichment pipeline
    # from BrightData company_identity) over the nested current_company dict name.
    # This prevents the LLM from picking a company from the experience array when
    # the current_company dict is empty but the company name IS known.
    _authoritative_company_name = (
        profile.get("current_company_name")
        or _current_co.get("name")
        or ""
    )
    return {
        # ── Identity ────────────────────────────────────────────────────────────
        "name":            profile.get("name", ""),
        "headline":        profile.get("headline") or profile.get("position") or profile.get("title", ""),
        "location":        profile.get("location") or profile.get("city", ""),
        "country":         profile.get("country_code") or profile.get("country", ""),
        "linkedin_url":    profile.get("url") or profile.get("input_url", ""),
        "profile_image":   profile.get("avatar_url") or profile.get("profile_pic_url", ""),
        "about":           profile.get("about") or "",
        "followers":       profile.get("followers", 0),
        "connections":     profile.get("connections", 0),
        # ── Company context → derive challenges, stage, priorities ───────────
        # name is always set to the authoritative company name so the LLM never
        # falls back to inferring it from the experience array.
        "company_context": {
            **_clean_company(_current_co),
            "name": _authoritative_company_name,   # always override — authoritative
            "logo": _current_co.get("logo") or _current_co.get("logo_url", ""),
        },
        # ── Career history ───────────────────────────────────────────────────
        # company field stripped from each experience entry — LLM must use
        # company_context.name (from BrightData current_company) exclusively.
        "experience":      [
            {k: v for k, v in e.items() if k != "company"}
            for e in _clean_exp(profile.get("experience"))
        ],
        "education":       _clean_edu(profile.get("education") or []),
        "skills":          [
            (s.get("name") if isinstance(s, dict) else s)
            for s in (profile.get("skills") or [])
        ][:40],
        "certifications":  [
            (c.get("title") or str(c))[:120]
            for c in (profile.get("certifications") or [])
        ][:15],
        "languages": [
            {"lang": (l.get("title") or l.get("name") or str(l)), "level": l.get("subtitle", "")}
            if isinstance(l, dict) else {"lang": str(l)}
            for l in (profile.get("languages") or [])
        ],
        "groups":          [
            g.get("name") or g.get("title") or str(g)
            for g in (profile.get("groups") or [])
        ][:20],
        "volunteer":       _clean_volunteer(profile.get("volunteer_experience") or profile.get("volunteer") or []),
        "recommendations": _clean_recs(profile.get("recommendations")),
        # ── Authored posts → expertise, identity, authority (HIGHEST SIGNAL) ─
        "authored_posts":  _clean_posts(profile.get("posts") or profile.get("recent_posts") or []),
        # ── Interactions → buying intent (liked / commented / reposted) ──────
        "interactions":    _clean_activity(profile.get("activity")),
    }


def _trim_profile_to_budget(optimized: dict, char_budget: int) -> dict:
    """
    Progressively reduce the optimized profile until it fits within char_budget.
    Order: remove low-signal sections first, then reduce caps on high-signal ones.
    The JSON structure is never changed — only content lengths/counts are reduced.
    """
    import copy
    p = copy.deepcopy(optimized)

    steps = [
        # Step 1 — drop lowest signal sections entirely
        lambda d: d.update({"volunteer": [], "groups": [], "honors_awards": None}) or d,
        # Step 2 — halve recommendations
        lambda d: d.update({"recommendations": d.get("recommendations", [])[:3]}) or d,
        # Step 3 — halve certifications + languages
        lambda d: d.update({"certifications": d.get("certifications", [])[:5], "languages": d.get("languages", [])[:3]}) or d,
        # Step 4 — trim interactions (buying intent signals)
        lambda d: d.update({"interactions": d.get("interactions", [])[:15]}) or d,
        # Step 5 — trim authored posts (identity signals)
        lambda d: d.update({"authored_posts": d.get("authored_posts", [])[:10]}) or d,
        # Step 6 — trim experience descriptions
        lambda d: d.update({"experience": [{**e, "description": (e.get("description") or "")[:400]} for e in d.get("experience", [])]}) or d,
        # Step 7 — drop recommendations entirely
        lambda d: d.update({"recommendations": []}) or d,
        # Step 8 — trim interactions further
        lambda d: d.update({"interactions": d.get("interactions", [])[:8]}) or d,
        # Step 9 — trim authored posts further
        lambda d: d.update({"authored_posts": d.get("authored_posts", [])[:5]}) or d,
        # Step 10 — trim about
        lambda d: d.update({"about": (d.get("about") or "")[:500]}) or d,
        # Step 11 — trim experience descriptions hard
        lambda d: d.update({"experience": [{**e, "description": (e.get("description") or "")[:150]} for e in d.get("experience", [])]}) or d,
        # Step 12 — cap skills
        lambda d: d.update({"skills": d.get("skills", [])[:15]}) or d,
    ]

    for step in steps:
        current = json.dumps(p, separators=(",", ":"), ensure_ascii=False, default=str)
        if len(current) <= char_budget:
            break
        step(p)

    return p


def _rule_based_enrichment(
    linkedin_url: str, profile: dict, contact: dict, now: str,
    website_intel: dict = None,
    scoring_cfg: dict = None,
) -> dict:
    """Produce the 8-stage structure from raw data without LLM."""
    name    = profile.get("name", "")
    title   = profile.get("position") or profile.get("headline", "")
    company = profile.get("current_company_name", "")
    loc     = profile.get("location") or profile.get("city", "")
    about   = (profile.get("about") or "")[:600]
    exp     = profile.get("experience") or []
    edu     = profile.get("education") or []
    skills  = profile.get("skills") or []
    certs   = profile.get("certifications") or []
    langs   = profile.get("languages") or []
    email   = contact.get("email") or ""

    # Company extras from waterfall
    company_extras = profile.get("_company_extras") or {}

    # Website intel
    wi = website_intel or {}

    # Seniority from title
    title_l = title.lower()
    if any(k in title_l for k in ["ceo","cto","coo","cfo","chief","founder","president"]):
        seniority = "C-Level / Executive"
    elif any(k in title_l for k in ["vp","vice president"]):
        seniority = "VP / Director"
    elif "director" in title_l:
        seniority = "Director"
    elif "manager" in title_l or "head of" in title_l:
        seniority = "Manager / Head"
    elif any(k in title_l for k in ["senior","sr.","lead","principal"]):
        seniority = "Senior Individual Contributor"
    else:
        seniority = "Individual Contributor"

    # Department from title
    if any(k in title_l for k in ["engineer","tech","developer","software","data","it","cloud","devops","cto"]):
        dept = "Engineering / Technology"
    elif any(k in title_l for k in ["sales","revenue","account","bd","business development"]):
        dept = "Sales"
    elif any(k in title_l for k in ["marketing","growth","content","brand","seo","demand"]):
        dept = "Marketing"
    elif any(k in title_l for k in ["hr","people","talent","recruit","culture"]):
        dept = "People / HR"
    elif any(k in title_l for k in ["product","pm","ux","design","ui"]):
        dept = "Product"
    elif any(k in title_l for k in ["finance","cfo","account","controller","treasury"]):
        dept = "Finance"
    elif any(k in title_l for k in ["ops","operations","supply","logistics","coo"]):
        dept = "Operations"
    else:
        dept = "General"

    # Years in role from first experience entry
    years_in_role = ""
    if exp and isinstance(exp[0], dict):
        dur = exp[0].get("duration") or ""
        if dur:
            years_in_role = dur

    # Previous companies
    prev = [e.get("company", "") for e in (exp[1:4] if isinstance(exp, list) else [])]
    prev = [c for c in prev if c]

    # Education string
    edu_str = ""
    if edu and isinstance(edu[0], dict):
        e0 = edu[0]
        edu_str = f"{e0.get('degree','')}, {e0.get('school','')}".strip(", ")

    # ICP scoring (0-40)
    icp = 0
    _tw = {"ceo":35,"cto":35,"founder":35,"co-founder":35,"vp":30,"vice president":30,
           "director":22,"head of":18,"chief":35,"president":35,"owner":28,
           "senior":8,"lead":8,"principal":10,"manager":14}
    for kw, pts in _tw.items():
        if kw in title_l:
            icp += pts
            break
    icp = min(icp, 40)

    # Intent score (0-30) — dynamic from real profile signals
    intent = 0
    followers_n = _safe_int(profile.get("followers"))
    connections_n = _safe_int(profile.get("connections"))
    if email: intent += 6                        # reachable contact found
    if about: intent += 4                        # has active profile summary
    if skills: intent += 3                       # skills listed = maintained profile
    if followers_n > 5000: intent += 8
    elif followers_n > 1000: intent += 5
    elif followers_n > 200: intent += 2
    if connections_n > 500: intent += 4
    elif connections_n > 100: intent += 2
    if exp and len(exp) >= 2: intent += 3        # career history = real person
    intent = min(intent, 30)

    # Timing score (0-20) — dynamic from data availability
    timing = 0
    if email: timing += 6                        # can reach them now
    if company: timing += 4                      # confirmed company = B2B qualified
    if title: timing += 3                        # role clarity for personalization
    _ce_domain = company_extras.get("_verified_domain") or company_extras.get("company_website") or ""
    if _ce_domain: timing += 3                   # real verified domain found
    if edu_str: timing += 2                      # education = complete profile
    if prev: timing += 2                         # career progression visible
    timing = min(timing, 20)

    # Data completeness score (0-10)
    dc_score = 0
    if email: dc_score += 3
    if contact.get("phone"): dc_score += 1
    if company: dc_score += 2
    if title: dc_score += 1
    if linkedin_url: dc_score += 1
    if skills: dc_score += 1
    if edu_str: dc_score += 1
    dc_score = min(dc_score, 10)

    total = icp + intent + timing + dc_score
    _sc = scoring_cfg or {}
    _hot  = _sc.get("hot",  80)
    _warm = _sc.get("warm", 55)
    _cool = _sc.get("cool", 30)
    if total >= _hot:
        tier, stage, score_tier_str = "Tier 1 — Hot", "MQL → Ready for outreach", "HOT"
    elif total >= _warm:
        tier, stage, score_tier_str = "Tier 2 — Warm", "Nurture sequence", "WARM"
    elif total >= _cool:
        tier, stage, score_tier_str = "Tier 3 — Cool", "Awareness / Monitor", "COOL"
    else:
        tier, stage, score_tier_str = "Tier 4 — Cold", "Disqualify or low priority", "COLD"

    # Tags
    tags = []
    if "saas" in (company + title_l + about).lower():
        tags.append("SaaS")
    if icp >= 18:
        tags.append("Decision-Maker")
    if email:
        tags.append("Email-Found")
    if loc:
        country = loc.split(",")[-1].strip()
        if country:
            tags.append(country)

    # Email status
    src = contact.get("source") or ""
    email_status = ("Valid — verified" if src in ("apollo",)
                    else "Pattern guess — unverified" if src == "pattern_guess"
                    else "Not found")

    # Completeness
    filled = sum(1 for v in [name, title, company, email, loc, about, edu_str, seniority] if v)
    completeness = int((filled / 8) * 100)

    # Basic cold email — use website intel value prop if available
    fn = name.split()[0] if name else "there"
    vp = wi.get("value_proposition") or ""
    problem = wi.get("problem_solved") or ""
    category = wi.get("product_category") or dept.split("/")[0].strip()

    if vp:
        outreach_angle = f"Website intelligence: {vp[:120]}"
    elif problem:
        outreach_angle = f"Pain point: {problem[:120]}"
    else:
        outreach_angle = "Role & company alignment"

    # Rule-based fallback subject: 10-20 words, specific to role + company
    _subj_role = title[:40] if title else f"{dept.split('/')[0].strip()} leader"
    cold_email_subject = f"Quick idea for how {company} could save 10+ hours a week on prospecting"

    _body_product_line = (
        f"Worksbuddy helps {category} teams {vp[:100].lower().rstrip('.')} — cutting manual prospecting time by over 10 hours per week."
        if vp else
        f"Worksbuddy helps {dept.split('/')[0].strip()} teams automate lead research, score prospects, and launch personalised outreach — all from a single platform, saving over 10 hours per week."
    )
    _body_pain = (
        f"We've seen that {dept.split('/')[0].strip()} teams at companies like {company} often spend too much time on manual data work that could be automated."
    )
    _body_proof = (
        f"One of our customers — a team similar in size to yours — reduced their prospecting cycle by 40% within the first month of using Worksbuddy."
    )
    _body_relevance = (
        f"Given your role as {_subj_role} at {company}, I thought this might be directly relevant to what you're working on right now."
    )

    cold_email = (
        f"Hi {fn},\n\n"
        f"Came across your profile and your work as {title} at {company} immediately caught my attention.\n\n"
        f"{_body_pain} {_body_product_line} "
        f"{_body_proof} {_body_relevance}\n\n"
        f"Would you be open to a quick 15-minute call this week to see if it could work for your team?\n\n"
        f"Best,\nWorksbuddy Team"
    )

    linkedin_note = (
        f"Hi {fn} — I noticed your work at {company} and thought Worksbuddy might be relevant to your team. "
        f"We help {dept.split('/')[0].strip()} teams cut manual prospecting time significantly. "
        f"Would love to connect — are you currently exploring ways to scale outreach?"
    )[:300]

    # Build career history from experience
    career_history = []
    for e in (exp[:5] if isinstance(exp, list) else []):
        if isinstance(e, dict):
            career_history.append({
                "title": e.get("title", ""),
                "company": e.get("company", ""),
                "duration": e.get("duration", ""),
            })

    # Compose 8-stage result — also include legacy keys for backwards compatibility
    result = {
        # ── Stage 1: Person Profile ────────────────────────────────────────
        "person_profile": {
            "full_name": name,
            "first_name": name.split()[0] if name else "",
            "last_name": " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "",
            "current_title": title,
            "seniority_level": seniority,
            "department": dept,
            "years_in_role": years_in_role,
            "career_history": career_history,
            "top_skills": skills if isinstance(skills, list) else [],
            # education_list → full structured list for UI display
            "education_list": edu if isinstance(edu, list) else (
                [{"school": edu_str, "degree": "", "years": ""}] if edu_str else []
            ),
            "education": edu_str,  # kept as string for scoring/DB compat
            "certifications": certs,  # full objects: {name, issuer, date, credential_url}
            "languages": langs,       # full objects: {name, proficiency}
            "city": (profile.get("city") or loc or "").split(",")[0].strip(),
            "country": _country_name(
                profile.get("country_code") or profile.get("country")
                or (loc.split(",")[-1].strip() if "," in loc else "")
            ),
            "timezone": profile.get("timezone") or "",
            "linkedin_activity": f"{profile.get('followers', 0):,} followers" if profile.get("followers") else "",
            "followers": _safe_int(profile.get("followers")),
            "connections": _safe_int(profile.get("connections")),
            "work_email": email if src == "apollo" else "",
            "personal_email": None,
            "direct_phone": contact.get("phone"),
            "twitter": contact.get("twitter"),
            "about": about,
        },
        # ── Stage 2: Company Identity ──────────────────────────────────────
        "company_identity": {
            "name": company,
            "domain": _extract_domain(company, company_extras.get("company_website") or profile.get("current_company_link", "")),
            "website": company_extras.get("company_website") or profile.get("current_company_link", ""),
            "linkedin_url": company_extras.get("company_linkedin") or "",
        },
        # ── Stage 3: Website Intelligence ─────────────────────────────────
        "website_intelligence": {
            "company_description": wi.get("company_description") or company_extras.get("company_description") or "",
            "product_offerings": wi.get("product_offerings") or [],
            "value_proposition": wi.get("value_proposition") or "",
            "target_customers": wi.get("target_customers") or [],
            "use_cases": wi.get("use_cases") or [],
            "business_model": wi.get("business_model") or "",
            "product_category": wi.get("product_category") or "",
            "market_positioning": wi.get("market_positioning") or "",
            "pricing_signals": wi.get("pricing_signals") or "",
            "key_messaging": wi.get("key_messaging") or [],
            "hiring_signals": wi.get("hiring_signals") or "",
            "open_roles": wi.get("open_roles") or [],
            "problem_solved": wi.get("problem_solved") or "",
        },
        # ── Stage 4: Company Profile ───────────────────────────────────────
        "company_profile": {
            "name": company,
            "industry": company_extras.get("industry") or "",
            "employee_count": company_extras.get("employee_count") or 0,
            "hq_location": company_extras.get("hq_location") or loc,
            "founded_year": company_extras.get("founded_year") or None,
            "annual_revenue_est": company_extras.get("annual_revenue") or "",
            "tech_stack": company_extras.get("tech_stack") or [],
            "company_logo": company_extras.get("company_logo") or None,
            "company_email": company_extras.get("company_email") or None,
            "company_phone": company_extras.get("company_phone") or None,
            "company_twitter": company_extras.get("company_twitter") or None,
        },
        # ── Stage 5: Market Signals (Company Intelligence) ─────────────────
        "company_intelligence": {
            "funding_stage": company_extras.get("funding_stage") or "",
            "total_funding": company_extras.get("total_funding") or "",
            "last_funding_date": "",
            "lead_investor": "",
            "hiring_velocity": company_extras.get("hiring_velocity") or "",
            "department_hiring_trends": [],
            "news_mention": "",
            "product_launch": "",
            "competitor_usage": "",
        },
        # ── Stage 6: Intent Signals ────────────────────────────────────────
        "intent_signals": {
            "recent_funding_event": "",
            "hiring_signal": company_extras.get("hiring_velocity") or "",
            "job_change": f"Currently at {company}" if company else "",
            "linkedin_activity": f"{profile.get('followers', 0):,} followers" if profile.get("followers") else "",
            "news_mention": "",
            "product_launch": "",
            "competitor_usage": "",
            "review_activity": "",
            "pain_indicators": [],
        },
        # ── Stage 7: Lead Scoring ──────────────────────────────────────────
        "lead_scoring": {
            "icp_fit_score": icp,
            "intent_score": intent,
            "timing_score": timing,
            "data_completeness_score": dc_score,
            "overall_score": total,
            "score_tier": score_tier_str,
            "score_explanation": f"{seniority} at {company} — {dept}",
            "icp_match_tier": tier,
            "disqualification_flags": [],
        },
        # ── Stage 8: Outreach ──────────────────────────────────────────────
        "outreach": {
            "email_subject": cold_email_subject,
            "cold_email": cold_email,
            "linkedin_note": linkedin_note,
            "best_channel": "Email" if email else "LinkedIn",
            "best_send_time": "Tuesday or Thursday, 1 PM recipient time",
            "outreach_angle": outreach_angle,
            "sequence_type": ("Hot Lead — 4-touch multichannel" if total >= 70
                              else "Warm — nurture sequence" if total >= 40
                              else "Cold — awareness only"),
            "sequence": {
                "day1": "Send cold email",
                "day2": "LinkedIn connection request with note",
                "day4": "Follow-up email — value proof",
                "day7": "LinkedIn message if connected",
                "day14": "Breakup email",
            },
            "last_contacted": None,
            "email_status": email_status,
        },
        # ── CRM metadata (legacy key, still needed for crm_stage/tags/completeness)
        "crm": {
            "lead_source": "LinkedIn URL",
            "enrichment_source": f"Bright Data + {src.title() if src else 'Waterfall'}",
            "enrichment_date": now,
            "enrichment_depth": "Deep",
            "data_completeness": completeness,
            "last_re_enriched": now,
            "assigned_owner": None,
            "crm_stage": stage,
            "tags": tags,
        },
        # ── Legacy compatibility keys ──────────────────────────────────────
        "identity": {
            "full_name": name,
            "work_email": email if src == "apollo" else "",
            "personal_email": None,
            "direct_phone": contact.get("phone"),
            "linkedin_url": linkedin_url,
            "twitter": contact.get("twitter"),
            "city": (profile.get("city") or loc or "").split(",")[0].strip(),
            "country": _country_name(
                profile.get("country_code") or profile.get("country")
                or (loc.split(",")[-1].strip() if "," in loc else "")
            ),
            "timezone": profile.get("timezone") or "",
        },
        "professional": {
            "current_title": title,
            "seniority_level": seniority,
            "department": dept,
            "years_in_role": years_in_role,
            "career_history": career_history,
            "previous_companies": prev,
            "top_skills": skills if isinstance(skills, list) else [],
            "education_list": edu if isinstance(edu, list) else (
                [{"school": edu_str, "degree": "", "years": ""}] if edu_str else []
            ),
            "education": edu_str,
            "certifications": certs,  # full objects: {name, issuer, date, credential_url}
            "languages": langs,       # full objects: {name, proficiency}
        },
        "company": {
            "name": company,
            "website": company_extras.get("company_website") or profile.get("current_company_link", ""),
            "industry": company_extras.get("industry") or "",
            "employee_count": company_extras.get("employee_count") or 0,
            "hq_location": company_extras.get("hq_location") or loc,
            "founded_year": company_extras.get("founded_year") or None,
            "funding_stage": company_extras.get("funding_stage") or "",
            "total_funding": company_extras.get("total_funding") or "",
            "last_funding_date": "",
            "lead_investor": "",
            "annual_revenue_est": company_extras.get("annual_revenue") or "",
            "tech_stack": company_extras.get("tech_stack") or [],
            "hiring_velocity": company_extras.get("hiring_velocity") or "",
        },
        "scoring": {
            "icp_fit_score": icp,
            "intent_score": intent,
            "timing_score": timing,
            "overall_score": total,
            "score_explanation": f"{seniority} at {company} — {dept}",
            "icp_match_tier": tier,
            "disqualification_flags": [],
        },
    }
    return result
