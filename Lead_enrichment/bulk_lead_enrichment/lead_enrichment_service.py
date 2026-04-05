"""
lead_enrichment_service.py
--------------------------
Screenshot + OCR pipeline for Lead Enrichment (P1-L7).

Flow:
  1. Open the given URL (LinkedIn profile, company page, or any web URL) in
     headless Chromium via Playwright and take a full-page screenshot.
  2. Extract visible text from the screenshot using OCR
     (pytesseract preferred — lightweight; easyocr as automatic fallback).
  3. Return a formatted "Live Research Data" block ready to be injected into
     the P1-L7 system prompt for AI enrichment.

Install requirements:
  pip install playwright pytesseract Pillow
  playwright install chromium
  # Optional fallback OCR:
  pip install easyocr
  # Tesseract system binary (for pytesseract):
  # macOS:  brew install tesseract
  # Ubuntu: sudo apt install tesseract-ocr
"""

import io
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Vision AI image size limits ───────────────────────────────────────────────
_VISION_MAX_WIDTH  = 1440   # px — resize wider screenshots
_VISION_MAX_HEIGHT = 4000   # px — crop taller screenshots to this height
_VISION_JPEG_QUALITY = 82   # JPEG compression quality

# ── Stealth user-agent (reduces bot-detection on LinkedIn etc.) ───────────────
_STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# ─────────────────────────────────────────────────────────────────────────────
# OCR — pytesseract (fast, needs tesseract binary) → easyocr fallback (pure Python)
# ─────────────────────────────────────────────────────────────────────────────

def _ocr_pytesseract(png_bytes: bytes) -> str:
    """Extract text with pytesseract. Raises ImportError if not installed."""
    import pytesseract
    from PIL import Image

    img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
    # PSM 1 = auto page segmentation with OSD; OEM 3 = default Tesseract+LSTM
    return pytesseract.image_to_string(img, config="--psm 1 --oem 3").strip()


def _ocr_easyocr(png_bytes: bytes) -> str:
    """Extract text with easyocr. Raises ImportError if not installed."""
    import numpy as np
    import easyocr
    from PIL import Image

    img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    results = reader.readtext(np.array(img), detail=0, paragraph=True)
    return "\n".join(str(r) for r in results if r).strip()


def extract_text_via_ocr(png_bytes: bytes) -> tuple[str, str]:
    """
    Run OCR on a PNG screenshot.
    Tries pytesseract first; falls back to easyocr automatically.

    Returns:
        (extracted_text, engine_name)  — engine_name is 'pytesseract',
        'easyocr', or 'none' when both are unavailable.
    """
    # ── pytesseract ───────────────────────────────────────────────────────────
    try:
        text = _ocr_pytesseract(png_bytes)
        if text:
            return text, "pytesseract"
    except ImportError:
        logger.info("[OCR] pytesseract not available, trying easyocr...")
    except Exception as e:
        logger.warning(f"[OCR] pytesseract error: {e}")

    # ── easyocr ───────────────────────────────────────────────────────────────
    try:
        text = _ocr_easyocr(png_bytes)
        if text:
            return text, "easyocr"
    except ImportError:
        logger.warning(
            "[OCR] Neither pytesseract nor easyocr is installed.\n"
            "  Install: pip install pytesseract && brew install tesseract\n"
            "  OR:      pip install easyocr"
        )
    except Exception as e:
        logger.warning(f"[OCR] easyocr error: {e}")

    return "", "none"


# ─────────────────────────────────────────────────────────────────────────────
# Screenshot — Playwright headless Chromium
# ─────────────────────────────────────────────────────────────────────────────

async def capture_screenshot(url: str, timeout_ms: int = 35_000) -> Optional[bytes]:
    """
    Open *url* in a headless Chromium browser and return a full-page PNG
    screenshot as bytes.  Returns None on any failure.

    Anti-detection measures applied:
      - navigator.webdriver hidden via init script
      - Realistic User-Agent, Accept-Language, and Sec-Fetch-* headers
      - Slow scroll to trigger lazy-loaded profile sections
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error(
            "[Screenshot] playwright not installed.\n"
            "  Run: pip install playwright && playwright install chromium"
        )
        return None

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--lang=en-US",
                ],
            )
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=_STEALTH_UA,
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;"
                        "q=0.9,image/webp,*/*;q=0.8"
                    ),
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Upgrade-Insecure-Requests": "1",
                },
            )
            page = await ctx.new_page()

            # Hide automation fingerprint
            await page.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                # Wait for dynamic JS content (React / Vue profiles)
                await page.wait_for_timeout(3500)
                # Scroll mid-page to trigger lazy sections (LinkedIn profile cards)
                await page.evaluate(
                    "window.scrollTo({top: document.body.scrollHeight * 0.5, behavior: 'smooth'})"
                )
                await page.wait_for_timeout(1500)
                # Scroll to bottom for full content
                await page.evaluate(
                    "window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'})"
                )
                await page.wait_for_timeout(1000)
            except Exception as nav_err:
                # Capture whatever partial content loaded
                logger.warning(f"[Screenshot] Navigation warning ({url}): {nav_err}")

            png = await page.screenshot(full_page=True, type="png")
            await browser.close()
            logger.info(f"[Screenshot] Captured {len(png):,} bytes from: {url}")
            return png

    except Exception as e:
        logger.error(f"[Screenshot] Failed for {url}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_url_to_text(url: str) -> dict:
    """
    Full pipeline: screenshot → OCR → structured result dict.

    Returns::

        {
            "url":              str,
            "screenshot_taken": bool,
            "ocr_engine":       str,   # 'pytesseract' | 'easyocr' | 'none'
            "ocr_text":         str,
            "char_count":       int,
            "error":            str | None,
        }
    """
    result: dict = {
        "url": url,
        "screenshot_taken": False,
        "ocr_engine": "none",
        "ocr_text": "",
        "char_count": 0,
        "error": None,
    }

    # ── Step 1: Screenshot ────────────────────────────────────────────────────
    logger.info(f"[LeadEnrichment] Capturing screenshot: {url}")
    png_bytes = await capture_screenshot(url)

    if not png_bytes:
        result["error"] = (
            "Screenshot capture failed. "
            "Ensure playwright is installed: "
            "pip install playwright && playwright install chromium"
        )
        return result

    result["screenshot_taken"] = True

    # ── Step 2: OCR ───────────────────────────────────────────────────────────
    logger.info("[LeadEnrichment] Running OCR on screenshot...")
    ocr_text, engine = extract_text_via_ocr(png_bytes)

    result["ocr_engine"] = engine
    result["ocr_text"] = ocr_text
    result["char_count"] = len(ocr_text)

    if not ocr_text:
        result["error"] = (
            "Screenshot taken but OCR returned empty text. "
            "Install an OCR engine: "
            "pip install pytesseract && brew install tesseract  "
            "OR  pip install easyocr"
        )
    else:
        logger.info(
            f"[LeadEnrichment] OCR complete — {len(ocr_text):,} chars via {engine}"
        )

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Prompt builder — formats scraped data for the P1-L7 system prompt
# ─────────────────────────────────────────────────────────────────────────────

def build_live_research_block(scrape_result: dict, extra_params: dict) -> str:
    """
    Format OCR-extracted page content + user-supplied params into the
    ``## Live Research Data`` block that the P1-L7 system prompt expects.

    Args:
        scrape_result : dict returned by :func:`scrape_url_to_text`
        extra_params  : full request.params dict (person_name, email, etc.)

    Returns:
        Formatted markdown string ready for injection into the user message.
    """
    url = scrape_result.get("url", "")
    ocr_text = scrape_result.get("ocr_text", "")
    engine = scrape_result.get("ocr_engine", "none")
    error = scrape_result.get("error")

    lines: list[str] = ["## Live Research Data", f"**Source URL**: {url}"]

    if ocr_text:
        lines += [
            f"**Extraction Method**: Full-page screenshot + OCR ({engine})",
            f"**Characters Extracted**: {len(ocr_text):,}",
            "",
            "### Extracted Page Content",
            ocr_text,
        ]
    else:
        lines.append(
            f"**Status**: Content extraction failed — {error or 'unknown error'}"
        )
        lines.append(
            "Note: Enrich using any other context provided below "
            "and publicly known information about the lead."
        )

    # Append manually-provided params as additional context
    _SKIP = {"linkedin_url", "url", "profile_url"}
    extra_lines = [
        f"- **{k.replace('_', ' ').title()}**: {v}"
        for k, v in extra_params.items()
        if k not in _SKIP and str(v).strip()
    ]
    if extra_lines:
        lines += ["", "### Additional Context Provided"] + extra_lines

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Vision AI pipeline — P1-L9 LinkedIn Vision Scraper
# Screenshot → base64 image → Vision LLM → structured JSON
# ─────────────────────────────────────────────────────────────────────────────

async def capture_screenshot_for_vision(
    url: str,
    timeout_ms: int = 35_000,
) -> Optional[bytes]:
    """
    Open *url* in headless Chromium, take a full-page screenshot,
    resize/compress to JPEG for Vision AI, and return the bytes.
    Returns None on failure.
    """
    from playwright.async_api import async_playwright

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage", "--disable-gpu"],
            )
            page = await browser.new_page(
                viewport={"width": 1440, "height": 900},
                user_agent=_STEALTH_UA,
            )

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                await page.wait_for_timeout(3000)
                # Scroll down to trigger lazy-loaded content
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)
            except Exception as nav_err:
                logger.warning(f"[Vision] Navigation warning ({url}): {nav_err}")

            png = await page.screenshot(full_page=True, type="png")
            await browser.close()

        # ── Resize + compress for Vision AI ──────────────────────────────────
        from PIL import Image
        img = Image.open(io.BytesIO(png)).convert("RGB")
        w, h = img.size

        if w > _VISION_MAX_WIDTH:
            scale = _VISION_MAX_WIDTH / w
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
            w, h = img.size

        if h > _VISION_MAX_HEIGHT:
            img = img.crop((0, 0, w, _VISION_MAX_HEIGHT))

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=_VISION_JPEG_QUALITY, optimize=True)
        jpeg_bytes = buf.getvalue()
        logger.info(f"[Vision] Screenshot ready: {w}x{min(h, _VISION_MAX_HEIGHT)}px, {len(jpeg_bytes):,} bytes")
        return jpeg_bytes

    except Exception as e:
        logger.error(f"[Vision] Screenshot failed for {url}: {e}")
        return None


def screenshot_to_base64(jpeg_bytes: bytes) -> str:
    """Encode JPEG bytes as base64 string for Vision AI message content."""
    import base64
    return base64.b64encode(jpeg_bytes).decode("utf-8")


def build_vision_messages(system_prompt: str, b64_image: str) -> list:
    """
    Build the messages list for a Vision AI call.
    Uses OpenAI-compatible image_url format (supported by Groq, HuggingFace, Ollama/llava).
    """
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{b64_image}"},
                },
                {
                    "type": "text",
                    "text": (
                        "Extract all LinkedIn profile data from this screenshot "
                        "and return as a JSON object exactly as specified in your instructions."
                    ),
                },
            ],
        },
    ]
