"""
_utils.py
---------
Pure utility functions — no internal project imports.

Covers:
  - Pipeline logger (_plog)
  - API key accessors (_k, _bd_api_key, etc.)
  - Emoji stripping (_strip_emojis, _clean_lead_strings)
  - Text normalisation (_clean_title, _to_third_person, _normalise_person_text)
  - Safe helpers (_safe_json, _safe_int, _parse_json_safe)
  - Lead helpers (_parse_name, _lead_id, _country_name, _COUNTRY_CODES)
  - Scoring helpers (_resolve_tier, _patch_crm_brief_images, _patch_crm_brief_scores)
  - URL classifiers (_is_company_url, _is_person_url)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline live log
# ─────────────────────────────────────────────────────────────────────────────

_pipeline_log = logging.getLogger("pipeline")
_pipeline_log.setLevel(logging.DEBUG)
if not _pipeline_log.handlers:
    _fh = logging.FileHandler("/tmp/pipeline.log")
    _fh.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S")
    )
    _pipeline_log.addHandler(_fh)
    _pipeline_log.propagate = False


def _plog(job_id: str, url: str, stage: str, msg: str, level: str = "info") -> None:
    """Structured per-lead pipeline log entry."""
    short_url = url.rstrip("/").split("/")[-1] if url else "?"
    short_job = (job_id or "")[-8:]
    entry = f"[job={short_job}] [{short_url}] [{stage}] {msg}"
    getattr(_pipeline_log, level)(entry)


# ─────────────────────────────────────────────────────────────────────────────
# API key accessors
# ─────────────────────────────────────────────────────────────────────────────

def _k(name: str, default: str = "") -> str:
    """Fetch key from keys_service (admin store) with os.getenv fallback."""
    try:
        from auth import keys_service as _ks
        return _ks.get(name) or os.getenv(name, default)
    except Exception:
        return os.getenv(name, default)


def _bd_api_key()         -> str:  return _k("BRIGHT_DATA_API_KEY")
def _bd_profile_dataset() -> str:  return _k("BD_PROFILE_DATASET_ID", "gd_l1viktl72bvl7bjuj0")
def _bd_company_dataset() -> str:  return _k("BD_COMPANY_DATASET_ID", "gd_l1vikfnt1wgvvqz95w")
def _apollo_api_key()     -> str:  return _k("APOLLO_API_KEY")
def _dropcontact_key()    -> str:  return _k("DROPCONTACT_API_KEY")
def _pdl_api_key()        -> str:  return _k("PDL_API_KEY")
def _zerobounce_key()     -> str:  return _k("ZEROBOUNCE_API_KEY")
def _wb_llm_host()        -> str:  return _k("WB_LLM_HOST", "http://ai-llm.worksbuddy.ai")
def _wb_llm_key()         -> str:  return _k("WB_LLM_API_KEY")
def _wb_llm_model()       -> str:  return _k("WB_LLM_DEFAULT_MODEL", "wb-pro")
def _hf_token()           -> str:  return _k("HF_TOKEN")
def _hf_model()           -> str:  return _k("HF_MODEL", "Qwen/Qwen2.5-7B-Instruct")


def _outreach_threshold() -> int:
    try:
        return int(_k("LEAD_OUTREACH_THRESHOLD", "50"))
    except ValueError:
        return 50


async def _outreach_threshold_for_org(org_id: str) -> int:
    """Read outreach threshold from workspace_configs for org; fall back to env/default."""
    try:
        from config.enrichment_config_service import get_scoring_config
        sc = await get_scoring_config(org_id)
        return int(sc.get("outreach_threshold", 50))
    except Exception:
        return _outreach_threshold()


# ─────────────────────────────────────────────────────────────────────────────
# Emoji stripping
# ─────────────────────────────────────────────────────────────────────────────

_EMOJI_RE = re.compile(
    "[\U0001F600-\U0001F64F"   # emoticons
    "\U0001F300-\U0001F5FF"   # symbols & pictographs
    "\U0001F680-\U0001F6FF"   # transport & map symbols
    "\U0001F700-\U0001F77F"   # alchemical symbols
    "\U0001F780-\U0001F7FF"   # geometric shapes extended
    "\U0001F800-\U0001F8FF"   # supplemental arrows
    "\U0001F900-\U0001F9FF"   # supplemental symbols & pictographs
    "\U0001FA00-\U0001FA6F"   # chess symbols
    "\U0001FA70-\U0001FAFF"   # symbols & pictographs extended-A
    "\U00002702-\U000027B0"   # dingbats
    "\U000024C2-\U0001F251"   # enclosed characters
    "\U0001F1E0-\U0001F1FF"   # flags
    "\U00010000-\U0010FFFF"   # supplementary multilingual plane
    "]+",
    flags=re.UNICODE,
)


def _strip_emojis(value: str) -> str:
    """Remove emoji characters and clean up extra whitespace."""
    if not value or not isinstance(value, str):
        return value
    return _EMOJI_RE.sub("", value).strip()


def _clean_lead_strings(lead: dict, fields: list) -> None:
    """Strip emojis from specified string fields of a lead dict in-place."""
    for f in fields:
        v = lead.get(f)
        if v and isinstance(v, str):
            lead[f] = _strip_emojis(v)


# ─────────────────────────────────────────────────────────────────────────────
# First-person → third-person normalisation
# ─────────────────────────────────────────────────────────────────────────────

def _clean_title(title: str) -> str:
    """
    Convert first-person LinkedIn headlines into clean professional job titles.

    Examples:
      "I lead the International Cooperation Division"  → "International Cooperation Division Lead"
      "I'm a Senior Product Designer at Figma"         → "Senior Product Designer"
      "I help B2B companies scale their outbound"       → "B2B Growth Specialist"
      "I am Head of Sales"                              → "Head of Sales"
    """
    if not title or not isinstance(title, str):
        return title
    t = title.strip()

    # "I am X" / "I'm X" — extract X directly (already a title noun)
    m = re.match(r"^I(?:'m| am)\s+(?:a\s+|an\s+|the\s+)?(.+)", t, re.IGNORECASE)
    if m:
        rest = m.group(1).strip()
        # Drop trailing "at/with/of Company..." clause
        rest = re.split(r"\s+(?:at|with|of|@)\s+[A-Z]", rest)[0].strip().rstrip(",–-").strip()
        # If it still reads like a sentence (has a verb word), skip — handled below
        if not re.search(r"\b(help|work|lead|run|build|create|drive|grow|focus|speciali)\b", rest, re.IGNORECASE):
            return rest

    # "I lead/run/head/own/manage/oversee the X" → "X Lead/Manager/..."
    m = re.match(
        r"^I\s+(lead|run|head|own|manage|oversee|direct|build|drive|grow)\s+(?:the\s+|a\s+|an\s+)?(.+)",
        t, re.IGNORECASE,
    )
    if m:
        verb = m.group(1).lower()
        obj  = m.group(2).strip()
        obj  = re.split(r"\s+(?:at|with|for|@)\s+", obj)[0].strip().rstrip(".,–-").strip()
        suffix_map = {
            "lead": "Lead", "run": "Lead", "head": "Head",
            "own": "Owner", "manage": "Manager", "oversee": "Manager",
            "direct": "Director", "build": "Lead", "drive": "Lead", "grow": "Lead",
        }
        suffix = suffix_map.get(verb, "Lead")
        return f"{obj} {suffix}"

    # "I help X do Y" → too vague — return sanitised
    m = re.match(r"^I\s+help\s+(.+?)\s+(?:to\s+)?\w+", t, re.IGNORECASE)
    if m:
        return t  # can't reliably extract a title — leave for LLM enrichment

    # Any other first-person opener — strip the "I [verb] " prefix
    m = re.match(r"^I\s+\w+\s+(.*)", t, re.IGNORECASE)
    if m:
        rest = m.group(1).strip().rstrip(".,–-").strip()
        if len(rest) > 3:
            return rest

    return t


def _to_third_person(text: str, first_name: str) -> str:
    """
    Rewrite a first-person LinkedIn 'about' section into third-person.

    "I lead..."     → "Firstname leads..."
    "I am / I'm"    → "Firstname is"
    "I've / I have" → "Firstname has"
    "I was"         → "Firstname was"
    "My "           → "Their "
    "me"            → "them"
    "we / our"      → "the team / their"
    """
    if not text or not isinstance(text, str):
        return text

    name = (first_name or "").strip() or "They"

    subs = [
        # Contractions first (order matters)
        (r"\bI'm\b",            f"{name} is"),
        (r"\bI've\b",           f"{name} has"),
        (r"\bI'd\b",            f"{name} would"),
        (r"\bI'll\b",           f"{name} will"),
        (r"\bI was\b",          f"{name} was"),
        (r"\bI have\b",         f"{name} has"),
        (r"\bI am\b",           f"{name} is"),
        # "I lead/run/help..." → conjugate verb (add s)
        (r"\bI (lead|run|help|work|build|drive|grow|focus|head|own|manage|oversee|create|speciali[sz]e)\b",
         lambda m, n=name: f"{n} {m.group(1)}s"),
        # Generic "I <verb>" → "Name <verb>s"  (only for simple present verbs)
        (r"\bI ([a-z]+)\b",     lambda m, n=name: f"{n} {m.group(1)}s"),
        # Possessive / object
        (r"\bMy\b",             "Their"),
        (r"\bmy\b",             "their"),
        (r"\bMe\b",             "Them"),
        (r"\bme\b",             "them"),
        # Plural first-person
        (r"\bWe\b",             "The team"),
        (r"\bwe\b",             "the team"),
        (r"\bOur\b",            "Their"),
        (r"\bour\b",            "their"),
    ]

    result = text
    for pattern, repl in subs:
        if callable(repl):
            result = re.sub(pattern, repl, result)
        else:
            result = re.sub(pattern, repl, result)

    return result.strip()


def _normalise_person_text(lead: dict) -> None:
    """
    Apply title cleaning and about→third-person rewrite in-place before DB save.
    - Cleans first-person titles and about text
    - Clears title if it's empty or duplicates the about text
    """
    first_name = (lead.get("first_name") or "").strip()

    title = lead.get("title") or ""
    if title and re.match(r"^I\b", title.strip(), re.IGNORECASE):
        title = _clean_title(title)
        lead["title"] = title

    about = lead.get("about") or ""
    if about and re.search(r"\bI\b|\bmy\b|\bwe\b", about, re.IGNORECASE):
        about = _to_third_person(about, first_name)
        lead["about"] = about

    # Clear title if it's effectively the same as about (e.g. full sentence stored as title)
    if title and about:
        title_clean = title.strip().lower()
        about_start = about.strip().lower()[:len(title_clean) + 10]
        if title_clean in about_start or about_start.startswith(title_clean):
            lead["title"] = ""


# ─────────────────────────────────────────────────────────────────────────────
# Safe helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_json_safe(raw, default):
    """Parse a JSON string field, returning default on any failure."""
    if not raw:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default


def _safe_json(v) -> str:
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return str(v) if v else ""


def _safe_int(v, max_val: int = 2_000_000_000) -> int:
    """
    Safely convert any BD field value to a SQLite-safe integer.

    Handles:
    - Numeric strings with commas/plus signs: "1,234", "500+"
    - Range strings — takes the lower bound: "10,001-50,000" → 10001
    - Dicts/lists (employee range objects) — take first numeric value found
    - Very large numbers — capped at max_val (default 2 billion)
    - None / empty string → 0
    """
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return min(int(v), max_val)
    if isinstance(v, dict):
        # BD sometimes returns employee ranges as {"start": 10001, "end": 50000}
        for k in ("start", "from", "min", "low", "count"):
            if k in v:
                return _safe_int(v[k], max_val)
        # Fall back to first numeric value in dict
        for val in v.values():
            if isinstance(val, (int, float)):
                return min(int(val), max_val)
        return 0
    if isinstance(v, list):
        return _safe_int(v[0], max_val) if v else 0
    # String handling
    s = str(v).strip()
    if not s:
        return 0
    m = re.search(r"[\d,]+", s)
    if not m:
        return 0
    digits = m.group(0).replace(",", "")
    try:
        return min(int(digits), max_val)
    except (ValueError, OverflowError):
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# Lead / name helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_name(full_name: str) -> tuple[str, str]:
    parts = (full_name or "").split()
    return parts[0] if parts else "", " ".join(parts[1:]) if len(parts) > 1 else ""


# ISO 3166-1 alpha-2 → full country name (most common codes for B2B lead enrichment)
_COUNTRY_CODES: dict[str, str] = {
    "AF": "Afghanistan", "AL": "Albania", "DZ": "Algeria", "AR": "Argentina",
    "AU": "Australia", "AT": "Austria", "BE": "Belgium", "BR": "Brazil",
    "CA": "Canada", "CL": "Chile", "CN": "China", "CO": "Colombia",
    "CZ": "Czech Republic", "DK": "Denmark", "EG": "Egypt", "FI": "Finland",
    "FR": "France", "DE": "Germany", "GH": "Ghana", "GR": "Greece",
    "HK": "Hong Kong", "HU": "Hungary", "IN": "India", "ID": "Indonesia",
    "IE": "Ireland", "IL": "Israel", "IT": "Italy", "JP": "Japan",
    "KE": "Kenya", "KR": "South Korea", "MY": "Malaysia", "MX": "Mexico",
    "MA": "Morocco", "NL": "Netherlands", "NZ": "New Zealand", "NG": "Nigeria",
    "NO": "Norway", "PK": "Pakistan", "PE": "Peru", "PH": "Philippines",
    "PL": "Poland", "PT": "Portugal", "RO": "Romania", "RU": "Russia",
    "SA": "Saudi Arabia", "ZA": "South Africa", "ES": "Spain", "SE": "Sweden",
    "CH": "Switzerland", "TW": "Taiwan", "TH": "Thailand", "TR": "Turkey",
    "UA": "Ukraine", "AE": "United Arab Emirates", "GB": "United Kingdom",
    "US": "United States", "VN": "Vietnam",
}


def _country_name(code: str) -> str:
    """Map ISO country code to full name; return as-is if already a full name or unknown."""
    if not code:
        return ""
    code = code.strip()
    if len(code) <= 3 and code.upper() == code:
        return _COUNTRY_CODES.get(code.upper(), code)
    return code


def _lead_id(linkedin_url: str) -> str:
    # Hash only the profile slug (/in/username) so all URL variants map to the same lead:
    # www vs no-www, country domains, query params, trailing slashes, protocol — all ignored
    m = re.search(r"/in/([^/?#\s]+)", linkedin_url.strip(), re.IGNORECASE)
    slug = m.group(1).lower().rstrip("/") if m else linkedin_url.strip().lower()
    return hashlib.md5(slug.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# Scoring / CRM brief helpers
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_tier(tier_raw: str) -> str:
    """Normalize score tier to lowercase hot/warm/cool/cold for DB storage."""
    t = (tier_raw or "").lower()
    if "hot" in t or t == "hot":
        return "hot"
    if "warm" in t or t == "warm":
        return "warm"
    if "cool" in t or t == "cool":
        return "cool"
    return "cold"


def _patch_crm_brief_images(
    crm_brief: Optional[dict], avatar_url: str, company_logo: str
) -> Optional[dict]:
    """Inject profile_image + company_logo into crm_brief.who_they_are before saving to DB.
    LLM never has real image URLs — must be patched from BD profile data.
    """
    if not crm_brief or not isinstance(crm_brief, dict):
        return crm_brief
    who = crm_brief.get("who_they_are")
    if isinstance(who, dict):
        # Always overwrite — LLM generates fake LinkedIn CDN tokens if given empty URL.
        # BD avatar_url is authoritative; never trust the LLM-generated value.
        if avatar_url:
            who["profile_image"] = avatar_url
        if company_logo:
            who["company_logo"] = company_logo
    return crm_brief


_SCORE_LABEL_MAP = {
    "very high": 90, "high": 75, "medium high": 65,
    "medium": 50,    "low medium": 35,
    "low": 20,       "very low": 10,
}


def _patch_crm_brief_scores(
    crm_brief: Optional[dict],
    icp_fit_score: int = 0,
    intent_score: int = 0,
    timing_score: int = 0,
) -> Optional[dict]:
    """Normalize crm_scores to integers before saving to DB.
    LLM sometimes returns 'High', 'Very High' etc. instead of 0-100 integers.
    Falls back to DB-calculated scores when LLM returned 0.
    """
    if not crm_brief or not isinstance(crm_brief, dict):
        return crm_brief

    scores = crm_brief.get("crm_scores")
    if not isinstance(scores, dict):
        return crm_brief

    def _to_int(v) -> int:
        if isinstance(v, int):
            return v
        s = str(v).lower().replace("%", "").strip()
        if s in _SCORE_LABEL_MAP:
            return _SCORE_LABEL_MAP[s]
        try:
            return int(float(s))
        except Exception:
            return 0

    for field in ("icp_fit", "engagement_score", "timing_score"):
        scores[field] = _to_int(scores.get(field))

    # Fallback to DB scoring when LLM returned 0
    if not scores.get("icp_fit"):
        scores["icp_fit"] = icp_fit_score
    if not scores.get("engagement_score"):
        scores["engagement_score"] = intent_score
    if not scores.get("timing_score"):
        scores["timing_score"] = timing_score

    return crm_brief


# ─────────────────────────────────────────────────────────────────────────────
# URL classifiers
# ─────────────────────────────────────────────────────────────────────────────

def _is_company_url(url: str) -> bool:
    return "linkedin.com/company/" in url.lower()


def _is_person_url(url: str) -> bool:
    return "linkedin.com/in/" in url.lower()
