"""
ai_analysis.py
---------------
AI Analysis Layer for Lead Enrichment.

Takes raw LinkedIn profile + workspace config and produces:
  1. auto_tags          — 6-8 professional interest pills
  2. behavioural_signals — posts_about, engages_with, style, decision_pattern,
                           pain_point_hint, warm_signal
  3. pitch_intelligence  — top_pain_point, best_value_prop, do_not_pitch,
                           best_angle, suggested_cta

Uses the same LLM stack as the rest of the pipeline:
  WB_LLM_HOST (primary) → HuggingFace Qwen2.5-7B (fallback) → rule-based (offline)

All outputs are JSON strings ready to be stored in DB columns.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def _call_llm_sync_import():
    """Lazy import to avoid circular import — same _call_llm as the main service."""
    from lead_enrichment_brightdata_service import _call_llm, _parse_json_from_llm
    return _call_llm, _parse_json_from_llm


def _build_activity_summary(profile: dict) -> str:
    """
    Summarise the profile's activity feed into a concise text block
    for the AI prompt. Caps at ~2000 chars.
    """
    lines: list[str] = []

    about = (profile.get("about") or "")[:500]
    if about:
        lines.append(f"ABOUT: {about}")

    posts = profile.get("_posts") or []
    if posts:
        lines.append("AUTHORED POSTS:")
        for p in posts[:8]:
            if isinstance(p, dict):
                title = p.get("title") or p.get("text") or p.get("content") or ""
                lines.append(f"  - {str(title)[:150]}")
            elif isinstance(p, str):
                lines.append(f"  - {p[:150]}")

    activity = profile.get("_activity_full") or []
    if activity:
        lines.append("ACTIVITY (liked/shared/commented):")
        for a in activity[:10]:
            if isinstance(a, dict):
                text = a.get("title") or a.get("text") or a.get("action") or ""
                lines.append(f"  - {str(text)[:120]}")
            elif isinstance(a, str):
                lines.append(f"  - {a[:120]}")

    skills = profile.get("skills") or []
    if skills:
        skill_str = ", ".join(str(s) for s in skills[:15])
        lines.append(f"SKILLS: {skill_str}")

    return "\n".join(lines)[:2000]


def _rule_based_analysis(profile: dict, ws: dict) -> dict:
    """
    Rule-based fallback when LLM is unavailable.
    Produces deterministic tags and basic signals from profile fields.
    """
    title   = (profile.get("position") or profile.get("headline") or "").lower()
    about   = (profile.get("about") or "").lower()
    skills  = [str(s).lower() for s in (profile.get("skills") or [])]
    company = (profile.get("current_company_name") or "").lower()
    combined = f"{title} {about} {' '.join(skills)}"

    # Tag generation from keywords
    tag_map = [
        (["ai", "machine learning", "llm", "gpt", "artificial intelligence"], "AI / ML"),
        (["automation", "workflow", "n8n", "zapier", "make.com"],             "Workflow Automation"),
        (["saas", "b2b software", "product-led"],                             "SaaS Builder"),
        (["cto", "vp engineering", "head of engineering"],                    "Engineering Leader"),
        (["sales", "crm", "pipeline", "revenue"],                             "Sales Ops Aware"),
        (["startup", "founder", "co-founder", "bootstrapped"],                "Startup Founder"),
        (["data", "analytics", "bi", "dashboard", "metrics"],                 "Data-Driven"),
        (["product", "product manager", "pm ", "roadmap"],                    "Product Thinker"),
        (["cloud", "aws", "gcp", "azure", "devops", "kubernetes"],            "Cloud / DevOps"),
        (["growth", "marketing", "seo", "content", "demand gen"],             "Growth Focused"),
        (["leadership", "team", "culture", "hiring", "management"],           "People Leader"),
        (["api", "integration", "developer", "engineering", "backend"],       "Tech Architecture"),
    ]
    tags = []
    for keywords, label in tag_map:
        if any(k in combined for k in keywords):
            tags.append(label)
        if len(tags) >= 10:
            break

    # Pad to minimum 4 using title/company/seniority fallbacks
    if len(tags) < 4:
        seniority_map = [
            (["cto", "chief technology"], "C-Suite Tech"),
            (["ceo", "chief executive", "founder", "co-founder"], "Founder / CEO"),
            (["coo", "chief operating"], "Operations Leader"),
            (["vp ", "vice president"], "VP Level"),
            (["director"], "Director Level"),
            (["manager", "head of"], "Team Manager"),
        ]
        for keywords, label in seniority_map:
            if label not in tags and any(k in title for k in keywords):
                tags.append(label)
                if len(tags) >= 4:
                    break

    if len(tags) < 4:
        # Add company-based tag
        if company and len(tags) < 4:
            tags.append(f"{company[:20].title()} Team")
        # Final fallback generic tags
        generic = ["B2B Professional", "LinkedIn Active", "Decision Maker", "Industry Expert"]
        for g in generic:
            if len(tags) >= 4:
                break
            if g not in tags:
                tags.append(g)

    tags = tags[:10]

    # Warm signal detection
    warm = None
    posts   = profile.get("_posts") or []
    activity = profile.get("_activity_full") or []
    ws_name  = (ws.get("product_name") or "").lower()
    if ws_name:
        for item in posts + activity:
            text = str(item.get("title") or item.get("text") or item if isinstance(item, str) else "").lower()
            if ws_name in text:
                warm = f"Engaged with {ws.get('product_name')} content"
                break

    signals = {
        "posts_about":        f"Topics related to {title}" if title else "Not determined",
        "engages_with":       ", ".join(tags[:3]) if tags else "General professional content",
        "communication_style": "Peer-to-peer, practical" if "leader" in title or "cto" in title else "Professional",
        "decision_pattern":   "Data-driven, prefers proven solutions",
        "pain_point_hint":    f"Scaling {company} efficiently" if company else "Operational efficiency",
        "warm_signal":        warm or "No warm signal detected",
    }

    pitch = {
        "top_pain_point":  f"Managing growth and efficiency as {title}" if title else "Operational efficiency",
        "best_value_prop": ws.get("value_proposition") or "Automate manual work and focus on what matters",
        "do_not_pitch":    ws.get("banned_phrases") or ["generic buzzwords", "complex demos"],
        "best_angle":      "Pain-based" if not warm else "Social proof — already engaged with your brand",
        "suggested_cta":   "Ask a discovery question, not a demo request",
    }

    return {
        "auto_tags_json":           json.dumps(tags),
        "behavioural_signals_json": json.dumps(signals),
        "pitch_intelligence_json":  json.dumps(pitch),
        "warm_signal":              warm,
    }


async def run_ai_analysis(profile: dict, ws_config: dict) -> dict:
    """
    Run the full AI analysis layer on a LinkedIn profile.

    Returns a dict with:
      auto_tags_json           — JSON string (array of pills)
      behavioural_signals_json — JSON string (object)
      pitch_intelligence_json  — JSON string (object)
      warm_signal              — plain string or None
    """
    activity_text = _build_activity_summary(profile)

    name    = profile.get("name") or profile.get("first_name", "")
    title   = profile.get("position") or profile.get("headline") or ""
    company = profile.get("current_company_name") or ""

    product_ctx = ""
    if ws_config.get("product_name"):
        product_ctx = f"""
Selling product: {ws_config['product_name']}
Value proposition: {ws_config.get('value_proposition', '')}
Target titles: {', '.join(ws_config.get('target_titles') or [])}
Target industries: {', '.join(ws_config.get('target_industries') or [])}
Tone: {ws_config.get('tone', 'peer')}
CTA style: {ws_config.get('cta_style', 'question')}
Banned phrases: {', '.join(ws_config.get('banned_phrases') or [])}
Case studies: {'; '.join(ws_config.get('case_studies') or [])}"""

    prompt = f"""Analyse this LinkedIn prospect and return ONLY a JSON object with 3 keys.

Prospect: {name} | {title} | {company}

Activity and profile data:
{activity_text or 'No activity data available.'}
{product_ctx}

Return ONLY valid JSON with exactly these 3 keys:

{{
  "auto_tags": ["tag1", "tag2", ...],
  "behavioural_signals": {{
    "posts_about": "1 sentence — what they write about",
    "engages_with": "1 sentence — what they like/share/comment on",
    "communication_style": "1 sentence — how they communicate",
    "decision_pattern": "1 sentence — how they make decisions",
    "pain_point_hint": "1 sentence — their likely current pain",
    "warm_signal": "1 sentence or null — any engagement with your brand/product"
  }},
  "pitch_intelligence": {{
    "top_pain_point": "their #1 pain right now in 1 sentence",
    "best_value_prop": "which value prop will resonate most, in 1 sentence",
    "do_not_pitch": ["thing to avoid 1", "thing to avoid 2", "thing to avoid 3"],
    "best_angle": "best outreach angle: social proof / pain / curiosity / trigger event",
    "suggested_cta": "low-friction CTA — a question or micro-commitment, not a demo request"
  }}
}}

Rules:
- auto_tags: MINIMUM 4, MAXIMUM 10 short tags (2-3 words max), describing professional interests, tech focus, mindset. Count before returning — must have at least 4.
- All values must be non-null strings based ONLY on the data above
- If no warm signal found, set warm_signal to null
- Return ONLY the JSON object, no explanation, no markdown fences"""

    try:
        _call_llm, _parse_json = _call_llm_sync_import()

        raw = await _call_llm(
            [
                {"role": "system", "content": "You are a B2B sales intelligence AI. Analyse LinkedIn profiles and return structured JSON only."},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=800,
            temperature=0.3,
        )

        if raw:
            data = _parse_json(raw)
            if data and "auto_tags" in data:
                tags    = (data.get("auto_tags") or [])[:10]  # cap at 10
                signals = data.get("behavioural_signals") or {}
                pitch   = data.get("pitch_intelligence") or {}
                # If LLM returned fewer than 4, pad with rule-based tags
                if len(tags) < 4:
                    rb = _rule_based_analysis(profile, ws_config)
                    rb_tags = json.loads(rb["auto_tags_json"])
                    for t in rb_tags:
                        if t not in tags:
                            tags.append(t)
                        if len(tags) >= 4:
                            break

                warm = signals.get("warm_signal")
                if warm and str(warm).lower() in ("null", "none", "no warm signal"):
                    warm = None

                logger.info(
                    "[AIAnalysis] Done for %s — tags=%d warm=%s",
                    name, len(tags), bool(warm),
                )
                return {
                    "auto_tags_json":           json.dumps(tags),
                    "behavioural_signals_json": json.dumps(signals),
                    "pitch_intelligence_json":  json.dumps(pitch),
                    "warm_signal":              warm,
                }

    except Exception as e:
        logger.warning("[AIAnalysis] LLM failed (%s) — using rule-based fallback", e)

    # Rule-based fallback
    return _rule_based_analysis(profile, ws_config)
