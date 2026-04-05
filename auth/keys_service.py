"""
keys_service.py — Centralized key/secret management for WorksBuddy AI Brain

Single source of truth: backend/configs/system_keys.json
All API keys, credentials, and service configs are stored there and managed
via the Admin Panel. Services call keys_service.get("KEY_NAME") to fetch
values — callers never need to pass keys in API requests.

Priority order for key resolution:
  1. configs/system_keys.json  (admin-managed, persisted)
  2. os.environ / .env         (fallback for CI/CD or Docker secrets)
  3. provided default          (hard-coded safe fallback)

Hot-reload: call keys_service.reload() or use the admin API — in-memory cache
is refreshed and os.environ is synced so all lazy-reading code picks up new
values instantly without a server restart.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_KEYS_FILE = Path(__file__).parent / "configs" / "system_keys.json"

# ── In-memory cache ────────────────────────────────────────────────────────────
_store: dict = {}
_loaded = False


# ── Internal helpers ───────────────────────────────────────────────────────────

def _load() -> None:
    global _store, _loaded
    try:
        if _KEYS_FILE.exists():
            _store = json.loads(_KEYS_FILE.read_text(encoding="utf-8"))
            _loaded = True
            total = sum(
                len(v.get("keys", {}))
                for k, v in _store.items()
                if k != "_meta" and isinstance(v, dict)
            )
            logger.info(f"[KeysService] Loaded {total} keys from {_KEYS_FILE.name}")
        else:
            logger.warning(f"[KeysService] {_KEYS_FILE} not found — using env fallback only")
            _store = {}
    except Exception as exc:
        logger.error(f"[KeysService] Load failed: {exc}")
        _store = {}


def _save() -> None:
    try:
        if "_meta" in _store:
            _store["_meta"]["updated_at"] = datetime.now(timezone.utc).isoformat()
        _KEYS_FILE.write_text(json.dumps(_store, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        logger.error(f"[KeysService] Save failed: {exc}")


def _flat_map() -> dict[str, str]:
    """Return a flat {KEY_NAME: value} dict from the JSON store (non-empty values only)."""
    result: dict[str, str] = {}
    for section_key, section_data in _store.items():
        if section_key == "_meta" or not isinstance(section_data, dict):
            continue
        for key_name, key_data in section_data.get("keys", {}).items():
            val = key_data.get("value", "")
            if val:
                result[key_name] = val
    return result


def _mask(val: str) -> str:
    if not val:
        return ""
    if len(val) <= 8:
        return "••••••••"
    return val[:4] + "••••••••" + val[-4:]


# ── Public API ─────────────────────────────────────────────────────────────────

def get(key_name: str, default: str = "") -> str:
    """
    Fetch a key value.

    Checks JSON store first, then os.environ, then the provided default.
    Safe to call at any time — always returns a string.
    """
    if not _loaded:
        _load()

    # 1. JSON store
    for section_key, section_data in _store.items():
        if section_key == "_meta" or not isinstance(section_data, dict):
            continue
        keys = section_data.get("keys", {})
        if key_name in keys:
            val = keys[key_name].get("value", "")
            if val:
                return val

    # 2. Environment variable
    env_val = os.getenv(key_name, "")
    if env_val:
        return env_val

    return default


def sync_to_env() -> int:
    """
    Push all non-empty JSON store values into os.environ.

    Called once at startup so all services that read os.getenv() at module
    level pick up admin-configured values. Returns count of keys synced.
    """
    if not _loaded:
        _load()
    synced = 0
    for key_name, value in _flat_map().items():
        os.environ[key_name] = value
        synced += 1
    logger.info(f"[KeysService] Synced {synced} keys → os.environ")
    return synced


def reload() -> None:
    """Reload from disk and re-sync to os.environ (called by admin API after save)."""
    global _loaded
    _loaded = False
    _load()
    sync_to_env()
    logger.info("[KeysService] Hot-reloaded")


def get_all_sections() -> dict:
    """
    Return all sections with masked values for the admin API.
    Sensitive keys show a masked value; non-sensitive keys show the real value.
    """
    if not _loaded:
        _load()

    result: dict[str, Any] = {}
    for section_key, section_data in _store.items():
        if section_key == "_meta" or not isinstance(section_data, dict):
            continue
        keys_out: dict[str, Any] = {}
        for key_name, key_data in section_data.get("keys", {}).items():
            val = key_data.get("value", "")
            sensitive = key_data.get("sensitive", True)
            keys_out[key_name] = {
                "label":        key_data.get("label", key_name),
                "description":  key_data.get("description", ""),
                "sensitive":    sensitive,
                "masked_value": _mask(val) if sensitive else val,
                "is_set":       bool(val),
            }
        result[section_key] = {
            "label": section_data.get("label", section_key),
            "icon":  section_data.get("icon", "key"),
            "keys":  keys_out,
        }
    return result


def update_key(section: str, key_name: str, value: str) -> bool:
    """
    Update a key value in the JSON store and sync to os.environ.
    Returns True on success, False if section/key not found.
    """
    if not _loaded:
        _load()
    if section not in _store:
        return False
    keys = _store[section].get("keys", {})
    if key_name not in keys:
        return False
    keys[key_name]["value"] = value
    _save()
    # Sync this one key to env immediately
    if value:
        os.environ[key_name] = value
    elif key_name in os.environ:
        del os.environ[key_name]
    logger.info(f"[KeysService] Updated {section}/{key_name}")
    return True


def add_key(
    section: str,
    key_name: str,
    value: str,
    label: str = "",
    description: str = "",
    sensitive: bool = True,
) -> bool:
    """
    Add a new key to an existing section.
    Returns False if key already exists (use update_key to modify it).
    """
    if not _loaded:
        _load()
    if section not in _store:
        return False
    keys = _store[section].setdefault("keys", {})
    if key_name in keys:
        return False
    keys[key_name] = {
        "value":       value,
        "label":       label or key_name,
        "description": description,
        "sensitive":   sensitive,
    }
    _save()
    if value:
        os.environ[key_name] = value
    logger.info(f"[KeysService] Added {section}/{key_name}")
    return True


def delete_key(section: str, key_name: str) -> bool:
    """
    Remove a key from the JSON store.
    Returns True on success, False if not found.
    """
    if not _loaded:
        _load()
    if section not in _store:
        return False
    keys = _store[section].get("keys", {})
    if key_name not in keys:
        return False
    del keys[key_name]
    _save()
    os.environ.pop(key_name, None)
    logger.info(f"[KeysService] Deleted {section}/{key_name}")
    return True


def reveal_key(section: str, key_name: str) -> str | None:
    """
    Return the raw (unmasked) value of a key for the admin reveal endpoint.
    Returns None if not found.
    """
    if not _loaded:
        _load()
    if section not in _store:
        return None
    keys = _store[section].get("keys", {})
    if key_name not in keys:
        return None
    return keys[key_name].get("value", "")


# ── Load on import ─────────────────────────────────────────────────────────────
_load()
