"""Credential storage helpers for the ax Hermes plugin."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_SERVER_URL = "http://8.153.200.8:8787"
PLUGIN_VERSION = "0.1.0"


def hermes_home() -> Path:
    try:
        from hermes_constants import get_hermes_home

        return Path(get_hermes_home())
    except Exception:
        return Path(os.path.expanduser("~/.hermes"))


def credentials_path() -> Path:
    return hermes_home() / "ax-plugin" / "credentials.json"


def load_credentials() -> Dict[str, Any]:
    path = credentials_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def save_credentials(data: Dict[str, Any]) -> None:
    path = credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def clear_credentials() -> bool:
    path = credentials_path()
    if not path.exists():
        return False
    path.unlink()
    return True


def resolve_server_url(extra: Optional[Dict[str, Any]] = None) -> str:
    extra = extra or {}
    value = (
        os.getenv("AX_SERVER_URL")
        or str(extra.get("server_url") or "").strip()
        or str(load_credentials().get("serverUrl") or "").strip()
        or DEFAULT_SERVER_URL
    )
    return value.rstrip("/")


def resolve_credentials(extra: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    extra = extra or {}
    stored = load_credentials()
    installation_id = (
        os.getenv("AX_INSTALLATION_ID")
        or str(extra.get("installation_id") or "").strip()
        or str(stored.get("installationId") or "").strip()
    )
    device_token = (
        os.getenv("AX_DEVICE_TOKEN")
        or str(extra.get("device_token") or "").strip()
        or str(stored.get("deviceToken") or "").strip()
    )
    server_url = resolve_server_url(extra)
    if not installation_id or not device_token:
        return {}
    return {
        "installationId": installation_id,
        "deviceToken": device_token,
        "serverUrl": server_url,
    }
