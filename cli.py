"""CLI commands for the ax Hermes plugin."""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict

from .storage import (
    PLUGIN_VERSION,
    clear_credentials,
    credentials_path,
    load_credentials,
    resolve_credentials,
    resolve_server_url,
    save_credentials,
)


def register_cli(parser: argparse.ArgumentParser) -> None:
    subcommands = parser.add_subparsers(dest="ax_command")

    bind = subcommands.add_parser("bind", help="Bind this Hermes gateway to ax")
    bind.add_argument("--server-url", default=None, help="ax server URL")
    bind.add_argument("--timeout", type=int, default=600, help="Seconds to wait for approval")

    subcommands.add_parser("status", help="Show ax binding status")
    subcommands.add_parser("logout", aliases=["unbind"], help="Clear local ax binding credentials")

    parser.set_defaults(func=dispatch)


def dispatch(args: argparse.Namespace) -> int:
    command = getattr(args, "ax_command", None)
    if command == "bind":
        return _cmd_bind(args)
    if command == "status":
        return _cmd_status()
    if command in {"logout", "unbind"}:
        return _cmd_logout()
    print("usage: hermes ax {bind,status,logout}")
    return 2


def _cmd_bind(args: argparse.Namespace) -> int:
    server_url = (args.server_url or resolve_server_url()).rstrip("/")
    started = _post_json(
        f"{server_url}/v1/runtime-plugins/hermes/bind/start",
        {
            "deviceName": platform.node() or "Hermes Gateway",
            "pluginVersion": PLUGIN_VERSION,
            "platform": platform.system().lower(),
            "arch": platform.machine(),
            "nodeVersion": f"python {platform.python_version()}",
        },
    )
    bind_session_id = str(started["bindSessionId"])
    bind_url = str(started["bindUrl"])
    code = str(started["code"])
    poll_interval = max(1.0, float(started.get("pollIntervalMs") or 2000) / 1000)

    print("Bind this Hermes gateway to ax")
    print(f"Code: {code}")
    print(f"Open: {bind_url}")
    print()
    print("Waiting for approval in ax...")

    deadline = time.time() + max(1, int(args.timeout))
    while time.time() < deadline:
        poll_url = (
            f"{server_url}/v1/runtime-plugins/hermes/bind/poll?"
            f"bindSessionId={urllib.parse.quote(bind_session_id)}"
        )
        result = _get_json(poll_url)
        status = result.get("status")
        if status == "approved":
            credentials = {
                "provider": "hermes",
                "serverUrl": str(result.get("serverUrl") or server_url).rstrip("/"),
                "installationId": str(result["installationId"]),
                "deviceToken": str(result["deviceToken"]),
                "ownerUserId": result.get("ownerUserId"),
                "agentId": result.get("agentId"),
                "boundAt": _now_iso(),
            }
            save_credentials(credentials)
            print(f"Bound to ax. Credentials saved to {credentials_path()}")
            print("Restart or start the Hermes gateway to connect:")
            print("  hermes gateway restart")
            return 0
        if status in {"expired", "rejected"}:
            print(f"Binding {status}. Run `hermes ax bind` again.")
            return 1
        time.sleep(poll_interval)

    print("Timed out waiting for approval. Run `hermes ax bind` again.")
    return 1


def _cmd_status() -> int:
    credentials = resolve_credentials()
    stored = load_credentials()
    server_url = resolve_server_url()
    print("ax Hermes plugin")
    print(f"  serverUrl: {server_url}")
    print(f"  credentials: {credentials_path()}")
    if credentials:
        print("  bound: true")
        print(f"  installationId: {credentials.get('installationId')}")
        if stored.get("agentId"):
            print(f"  agentId: {stored.get('agentId')}")
    else:
        print("  bound: false")
        print("  next: hermes ax bind")
    return 0


def _cmd_logout() -> int:
    removed = clear_credentials()
    print("ax credentials cleared." if removed else "No ax credentials found.")
    print("Restart the Hermes gateway to disconnect any existing session.")
    return 0


def _post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"content-type": "application/json", "user-agent": "ax-hermes-plugin/0.1.0"},
    )
    return _open_json(request)


def _get_json(url: str) -> Dict[str, Any]:
    request = urllib.request.Request(url, method="GET", headers={"user-agent": "ax-hermes-plugin/0.1.0"})
    return _open_json(request)


def _open_json(request: urllib.request.Request) -> Dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            text = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"ax request failed: HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"ax request failed: {exc}") from exc
    data = json.loads(text)
    if not isinstance(data, dict):
        raise SystemExit("ax returned a non-object response")
    return data


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
