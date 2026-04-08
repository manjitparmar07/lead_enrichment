"""
_contact.py
-----------
Contact discovery waterfall: Apollo → Dropcontact → PDL → Pattern → ZeroBounce.

Covers:
  - _extract_domain
  - _try_apollo, _try_dropcontact, _try_pdl, _try_zerobounce
  - find_contact_info (5-step waterfall)
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

from ._utils import _apollo_api_key, _dropcontact_key, _pdl_api_key, _zerobounce_key
from ._clients import _get_api_client

try:
    from analytics import api_usage_service as _usage
except ImportError:
    class _usage:  # type: ignore
        @staticmethod
        async def track(*args, **kwargs): pass

logger = logging.getLogger(__name__)

_SOCIAL_DOMAINS = {"linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com"}


def _extract_domain(company_name: str, website: str = "") -> str:
    """Extract company domain. Skips LinkedIn/social URLs — those are not the company website."""
    if website:
        m = re.search(r"https?://(?:www\.)?([^/?\s]+)", website)
        if m:
            host = m.group(1).lower()
            if not any(host == s or host.endswith("." + s) for s in _SOCIAL_DOMAINS):
                return host
    words = re.sub(r"\s+", " ", (company_name or "").strip()).split()
    for word in words:
        slug = re.sub(r"[^a-z0-9]", "", word.lower())
        if slug and len(slug) >= 3 and slug not in {"the", "inc", "ltd", "llc", "pvt", "private", "limited", "solutions", "services", "technologies", "tech", "group", "and", "for"}:
            return f"{slug}.com"
    return ""


async def _try_apollo(first: str, last: str, domain: str, linkedin_url: str = "") -> Optional[dict]:
    if not _apollo_api_key() or (not domain and not linkedin_url):
        return None
    try:
        payload: dict = {"first_name": first, "last_name": last, "reveal_personal_emails": True}
        if domain:
            payload["domain"] = domain
        if linkedin_url:
            payload["linkedin_url"] = linkedin_url
        c = _get_api_client()
        r = await c.post(
            "https://api.apollo.io/v1/people/match",
            headers={"Content-Type": "application/json", "X-Api-Key": _apollo_api_key()},
            json=payload,
        )
        await _usage.track("apollo", "person_match")
        body = r.json()
        p   = body.get("person") or {}
        org = body.get("organization") or {}
        email   = p.get("email")
        phone   = p.get("phone") or (p.get("phone_numbers") or [{}])[0].get("sanitized_number")
        photo   = p.get("photo_url")
        twitter = p.get("twitter_url") or p.get("twitter")
        if email and "@" in email and "placeholder" not in email.lower():
            logger.info("[Apollo] Found: %s", email)
            _org_linkedin = org.get("linkedin_url") or ""
            if _org_linkedin and "linkedin.com/company/" not in _org_linkedin:
                _org_linkedin = ""
            return {
                "email":            email,
                "phone":            phone,
                "source":           "apollo",
                "confidence":       "high",
                "avatar_url":       photo,
                "twitter":          twitter,
                "company_linkedin": _org_linkedin or None,
                "_apollo_raw": {
                    "person":       p,
                    "organization": org,
                },
            }
        if email and "placeholder" in email.lower():
            logger.warning("[Apollo] Rejected placeholder email: %s", email)
        error_msg = body.get("error") or ""
        if r.status_code == 422 and "insufficient credits" in error_msg.lower():
            logger.warning("[Apollo] Credit exhausted: %s", error_msg)
            return {"_credit_exhausted": True, "source": "apollo_credit_exhausted"}
    except Exception as e:
        logger.warning("[Apollo] %s", e)
    return None


async def _try_dropcontact(linkedin_url: str, first: str, last: str, domain: str) -> Optional[dict]:
    """Dropcontact — enrich by LinkedIn URL or name+domain."""
    key = _dropcontact_key()
    if not key:
        return None
    try:
        payload = {
            "data": [{"linkedin": linkedin_url or "", "first_name": first, "last_name": last, "website": domain}],
            "siren": False, "language": "en",
        }
        c = _get_api_client()
        r = await c.post(
            "https://api.dropcontact.com/batch",
            json=payload, headers={"X-Access-Token": key, "Content-Type": "application/json"},
        )
        if r.status_code == 200:
            d = r.json()
            for item in (d.get("data") or []):
                email = (
                    item.get("email") and item["email"][0].get("email")
                    if isinstance(item.get("email"), list) else item.get("email")
                )
                if email:
                    logger.info("[Dropcontact] Found: %s", email)
                    return {"email": email, "phone": item.get("phone"), "source": "dropcontact", "confidence": "high"}
    except Exception as e:
        logger.debug("[Dropcontact] %s", e)
    return None


async def _try_pdl(first: str, last: str, linkedin_url: str, domain: str) -> Optional[dict]:
    """People Data Labs — person enrichment by LinkedIn URL or name+domain."""
    key = _pdl_api_key()
    if not key:
        return None
    try:
        params: dict = {"api_key": key, "pretty": "false"}
        if linkedin_url:
            params["profile"] = linkedin_url
        elif first and last and domain:
            params["first_name"] = first
            params["last_name"]  = last
            params["company"]    = domain
        else:
            return None
        c = _get_api_client()
        r = await c.get("https://api.peopledatalabs.com/v5/person/enrich", params=params)
        if r.status_code == 200:
            d = r.json()
            emails = d.get("data", {}).get("emails") or []
            email  = next((e.get("address") for e in emails if e.get("address")), None)
            phone  = next((p.get("number")  for p in (d.get("data", {}).get("phone_numbers") or [])), None)
            if email:
                logger.info("[PDL] Found: %s", email)
                return {"email": email, "phone": phone, "source": "pdl", "confidence": "high"}
    except Exception as e:
        logger.debug("[PDL] %s", e)
    return None


async def _try_zerobounce(email: str) -> dict:
    """ZeroBounce — email verification. Falls through gracefully if key not set."""
    key = _zerobounce_key()
    if not key or not email:
        return {"email": email, "verified": False, "bounce_risk": None}
    try:
        c = _get_api_client()
        r = await c.get(
            "https://api.zerobounce.net/v2/validate",
            params={"api_key": key, "email": email, "ip_address": ""},
        )
        if r.status_code == 200:
            d = r.json()
            status = d.get("status", "")
            sub_status = d.get("sub_status", "")
            verified = status == "valid"
            if status in ("invalid", "do_not_mail", "abuse"):
                bounce_risk = "high"
            elif status == "catch-all" or sub_status in ("role_based", "global_suppression"):
                bounce_risk = "medium"
            elif verified:
                bounce_risk = "low"
            else:
                bounce_risk = "medium"
            logger.info("[ZeroBounce] %s → status=%s bounce_risk=%s", email, status, bounce_risk)
            return {"email": email, "verified": verified, "bounce_risk": bounce_risk,
                    "zb_status": status, "zb_sub_status": sub_status}
    except Exception as e:
        logger.debug("[ZeroBounce] %s", e)
    return {"email": email, "verified": False, "bounce_risk": None}


async def find_contact_info(
    first: str,
    last: str,
    domain: str,
    linkedin_url: str = "",
    skip_apollo: bool = False,
) -> dict:
    """
    5-step email discovery waterfall:
      1. Apollo.io   — person match
      2. Dropcontact — LinkedIn URL or name+domain
      3. PDL         — People Data Labs person enrichment
      4. Pattern guess — first.last@domain, flast@domain, first@domain
      5. ZeroBounce  — verify whichever email was found

    Returns: {email, phone, source, confidence, verified, bounce_risk}
    """
    result: Optional[dict] = None

    async def _noop() -> None:
        return None

    # Step 1 — Apollo
    if not skip_apollo and _apollo_api_key() and domain:
        apollo_res = await _try_apollo(first, last, domain)
        if isinstance(apollo_res, dict) and apollo_res.get("email"):
            result = apollo_res
            logger.info("[ContactWaterfall] Step 1 Apollo hit: %s", result.get("email"))
        else:
            logger.info("[ContactWaterfall] Step 1 Apollo skipped (disabled/no credits)")

    # Steps 2 + 3 — Dropcontact and PDL in parallel
    if not result:
        run_dc  = bool(_dropcontact_key())
        run_pdl = bool(_pdl_api_key())
        if run_dc or run_pdl:
            dc_coro  = _try_dropcontact(linkedin_url, first, last, domain) if run_dc  else _noop()
            pdl_coro = _try_pdl(first, last, linkedin_url, domain)         if run_pdl else _noop()
            dc_res, pdl_res = await asyncio.gather(dc_coro, pdl_coro, return_exceptions=True)
            if isinstance(dc_res, dict) and dc_res.get("email"):
                result = dc_res
                logger.info("[ContactWaterfall] Step 2 Dropcontact hit: %s", result.get("email"))
            elif isinstance(pdl_res, dict) and pdl_res.get("email"):
                result = pdl_res
                logger.info("[ContactWaterfall] Step 3 PDL hit: %s", result.get("email"))

    # Step 4 — Pattern guess
    if not result and first and last and domain:
        last_clean = last.lower().replace(" ", "")
        guess = f"{first.lower()}.{last_clean}@{domain}"
        result = {"email": guess, "phone": None, "source": "pattern_guess", "confidence": "low"}
        logger.info("[ContactWaterfall] Step 4 Pattern guess: %s", guess)

    if not result:
        return {"email": None, "phone": None, "source": None, "confidence": None,
                "verified": False, "bounce_risk": None}

    # Reject placeholder emails from external services
    if result.get("email") and "placeholder" in result["email"].lower():
        logger.warning("[ContactWaterfall] Dropping placeholder email from %s: %s",
                       result.get("source"), result.get("email"))
        result["email"] = None

    # Step 5 — ZeroBounce verification
    email = result.get("email")
    if email:
        zb = await _try_zerobounce(email)
        result["verified"]    = zb.get("verified", False)
        result["bounce_risk"] = zb.get("bounce_risk")

    return result
