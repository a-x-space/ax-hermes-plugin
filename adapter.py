"""Hermes gateway platform adapter for ax."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover - aiohttp is a Hermes gateway dependency.
    aiohttp = None
    AIOHTTP_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

from .storage import PLUGIN_VERSION, clear_credentials, resolve_credentials, resolve_server_url

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 16000


def check_requirements() -> bool:
    return AIOHTTP_AVAILABLE


def validate_config(cfg: PlatformConfig) -> bool:
    return bool(resolve_credentials(getattr(cfg, "extra", {}) or {}))


def is_connected(cfg: PlatformConfig) -> bool:
    return validate_config(cfg)


def env_enablement() -> Optional[dict]:
    credentials = resolve_credentials()
    if not credentials:
        return None
    result = {
        "server_url": credentials["serverUrl"],
        "installation_id": credentials["installationId"],
        "device_token": credentials["deviceToken"],
    }
    return result


class AxAdapter(BasePlatformAdapter):
    """Bidirectional bridge between ax and a local Hermes gateway."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("ax"))
        extra = config.extra or {}
        credentials = resolve_credentials(extra)
        self._server_url = resolve_server_url(extra)
        self._installation_id = credentials.get("installationId", "")
        self._device_token = credentials.get("deviceToken", "")
        self._session: Optional["aiohttp.ClientSession"] = None
        self._ws: Optional["aiohttp.ClientWebSocketResponse"] = None
        self._runner_task: Optional[asyncio.Task] = None
        self._status_task: Optional[asyncio.Task] = None
        self._closing = False
        self._request_by_chat_id: Dict[str, str] = {}

    async def connect(self) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.warning("[ax] aiohttp is not available")
            return False
        if not self._installation_id or not self._device_token:
            logger.warning("[ax] not bound. Run: hermes ax bind")
            return False
        self._closing = False
        self._session = aiohttp.ClientSession()
        self._runner_task = asyncio.create_task(self._run_forever())
        logger.info("[ax] connecting to %s", self._server_url)
        return True

    async def disconnect(self) -> None:
        self._closing = True
        for task in (self._status_task, self._runner_task):
            if task and not task.done():
                task.cancel()
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()
        self._ws = None
        self._session = None
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to, metadata
        request_id = self._request_by_chat_id.pop(chat_id, None) or chat_id.replace("ax:", "", 1)
        payload = {
            "type": "agent.done",
            "messageId": _message_id("hmsg"),
            "requestId": request_id,
            "message": {"text": content},
            "completedAt": _now_iso(),
        }
        try:
            await self._send_json(payload)
            return SendResult(success=True, message_id=payload["messageId"])
        except Exception as exc:
            logger.warning("[ax] send failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"chat_id": chat_id, "name": chat_id, "type": "dm"}

    async def _run_forever(self) -> None:
        backoff = 1.0
        while not self._closing:
            try:
                await self._connect_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[ax] connection failed: %s", exc)
            self._mark_disconnected()
            if self._closing:
                break
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 1.7, 30.0)

    async def _connect_once(self) -> None:
        if not self._session:
            raise RuntimeError("aiohttp session is not initialized")
        ws_url = _ws_url(self._server_url, self._installation_id)
        headers = {
            "authorization": f"Bearer {self._device_token}",
            "x-ax-installation-id": self._installation_id,
            "user-agent": "ax-hermes-plugin/0.1.0",
        }
        async with self._session.ws_connect(ws_url, headers=headers, heartbeat=30) as ws:
            self._ws = ws
            self._mark_connected()
            await self._send_hello()
            self._status_task = asyncio.create_task(self._status_loop())
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_server_message(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        if self._status_task and not self._status_task.done():
            self._status_task.cancel()
        self._ws = None

    async def _handle_server_message(self, raw: str) -> None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(message, dict):
            return
        message_type = message.get("type")
        if message_type == "ping":
            await self._send_json(
                {
                    "type": "pong",
                    "messageId": _message_id("hpong"),
                    "requestId": message.get("requestId"),
                    "sentAt": _now_iso(),
                }
            )
            return
        if message_type == "binding.revoked":
            clear_credentials()
            if self._ws and not self._ws.closed:
                await self._ws.close()
            return
        if message_type == "agent.cancel":
            return
        if message_type != "agent.request":
            return
        await self._handle_agent_request(message)

    async def _handle_agent_request(self, message: Dict[str, Any]) -> None:
        request_id = str(message.get("requestId") or "")
        if not request_id:
            return
        chat_id = f"ax:{message.get('conversationId') or request_id}"
        self._request_by_chat_id[chat_id] = request_id
        await self._send_json(
            {
                "type": "agent.started",
                "messageId": _message_id("hstart"),
                "requestId": request_id,
                "localSessionKey": chat_id,
                "sentAt": _now_iso(),
            }
        )

        input_data = message.get("input") if isinstance(message.get("input"), dict) else {}
        text = str(input_data.get("text") or _content_to_text(input_data.get("content")) or "")
        source = self.build_source(
            chat_id=chat_id,
            chat_name="ax",
            chat_type="dm",
            user_id=str(message.get("userId") or "ax"),
            user_name="ax",
            message_id=request_id,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=message,
            message_id=request_id,
            timestamp=datetime.now(timezone.utc),
        )
        await self.handle_message(event)

    async def _status_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(30)
            await self._send_json(
                {
                    "type": "plugin.status",
                    "messageId": _message_id("hstat"),
                    "installationId": self._installation_id,
                    "state": "online",
                    "activeTurns": len(self._request_by_chat_id),
                    "sentAt": _now_iso(),
                }
            )

    async def _send_hello(self) -> None:
        await self._send_json(
            {
                "type": "plugin.hello",
                "messageId": _message_id("hhello"),
                "installationId": self._installation_id,
                "pluginVersion": PLUGIN_VERSION,
                "capabilities": ["agent.request", "agent.done"],
                "sentAt": _now_iso(),
            }
        )

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        if not self._ws or self._ws.closed:
            raise RuntimeError("ax websocket is not connected")
        await self._ws.send_str(json.dumps(payload, separators=(",", ":")))


def register(ctx) -> None:
    from .cli import dispatch, register_cli

    ctx.register_platform(
        name="ax",
        label="ax",
        adapter_factory=lambda cfg: AxAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[],
        install_hint="Run: hermes ax bind",
        env_enablement_fn=env_enablement,
        cron_deliver_env_var="AX_HOME_CHANNEL",
        allowed_users_env="AX_ALLOWED_USERS",
        allow_all_env="AX_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="AX",
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are communicating through ax. Reply in concise plain text unless "
            "the user asks for structure. ax will deliver your response back to "
            "the user's active ax conversation."
        ),
    )
    ctx.register_cli_command(
        name="ax",
        help="Bind and manage the ax Hermes integration",
        setup_fn=register_cli,
        handler_fn=dispatch,
    )


def _content_to_text(content: Any) -> str:
    if not isinstance(content, list):
        return ""
    parts = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text" and item.get("text"):
            parts.append(str(item.get("text")))
        elif item.get("type") == "markdown" and item.get("markdown"):
            parts.append(str(item.get("markdown")))
    return "\n".join(parts)


def _ws_url(server_url: str, installation_id: str) -> str:
    base = server_url.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://") :]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://") :]
    return f"{base}/v1/runtime-plugins/hermes/connect?installationId={installation_id}"


def _message_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
