"""Hermes gateway platform adapter for ax."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

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
AX_TOOL_EVENT_PREFIX = "__ax_tool_event__:"
STREAM_MESSAGE_PREFIX = "hstream"
TOOL_MESSAGE_PREFIX = "htool"

STREAM_CAPABILITIES = [
    "agent.request",
    "agent.started",
    "agent.done",
    "agent.delta",
    "agent.input_required",
    "agent.tool.started",
    "agent.tool.completed",
]


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
    SUPPORTS_MESSAGE_EDITING = True
    REQUIRES_EDIT_FINALIZE = True

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
        self._last_request_by_chat_id: Dict[str, str] = {}
        self._sequence_by_request_id: Dict[str, int] = {}
        self._stream_text_by_request_id: Dict[str, str] = {}
        self._message_chunk_seen_request_ids: Set[str] = set()
        self._completed_request_ids: Set[str] = set()
        self._last_edit_text_by_message_id: Dict[str, str] = {}
        self._pending_done_tasks: Dict[str, asyncio.Task] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect(self) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.warning("[ax] aiohttp is not available")
            return False
        if not self._installation_id or not self._device_token:
            logger.warning("[ax] not bound. Run: hermes ax bind")
            return False
        self._closing = False
        self._loop = asyncio.get_running_loop()
        self._session = aiohttp.ClientSession()
        self._runner_task = asyncio.create_task(self._run_forever())
        logger.info("[ax] connecting to %s", self._server_url)
        return True

    async def disconnect(self) -> None:
        self._closing = True
        for task in (self._status_task, self._runner_task):
            if task and not task.done():
                task.cancel()
        for task in self._pending_done_tasks.values():
            if not task.done():
                task.cancel()
        self._pending_done_tasks.clear()
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()
        self._ws = None
        self._session = None
        self._loop = None
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to
        metadata = metadata or {}
        request_id = self._request_id_for_chat(chat_id)
        try:
            tool_event = _decode_tool_event(content)
            if tool_event:
                message_id = await self._send_tool_event(chat_id, tool_event)
                return SendResult(success=True, message_id=message_id)

            if metadata.get("expect_edits"):
                if _looks_like_streaming_preview(content):
                    message_id = await self._send_stream_delta(chat_id, content, accumulated=True)
                    return SendResult(success=True, message_id=message_id or _stream_message_id(request_id))
                await self._send_stream_delta(chat_id, content, accumulated=True)
                self._schedule_done(chat_id, request_id, content)
                return SendResult(success=True, message_id=_stream_message_id(request_id))

            if self._is_active_chat(chat_id) and not metadata.get("notify"):
                tool_event = _tool_event_from_progress_text(content)
                if tool_event:
                    message_id = await self._send_tool_event(chat_id, tool_event)
                    return SendResult(success=True, message_id=message_id)
                message_id = await self._send_stream_delta(chat_id, content, accumulated=None)
                return SendResult(success=True, message_id=message_id or _stream_message_id(request_id))

            if request_id in self._completed_request_ids:
                return SendResult(success=True, message_id=_message_id("hdup"))
            message_id = await self._send_done(chat_id, content)
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            logger.warning("[ax] send failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del metadata
        try:
            tool_event = _decode_tool_event(content)
            if not tool_event and _is_tool_message_id(message_id):
                tool_event = _tool_event_from_progress_text(content)
            if _is_tool_message_id(message_id) or tool_event:
                if content != self._last_edit_text_by_message_id.get(message_id):
                    self._last_edit_text_by_message_id[message_id] = content
                    if tool_event:
                        await self._send_tool_event(chat_id, tool_event)
                return SendResult(success=True, message_id=message_id)

            if finalize:
                request_id = self._request_id_for_chat(chat_id)
                await self._send_stream_delta(chat_id, content, accumulated=True, cancel_pending=False)
                self._schedule_done(chat_id, request_id, content)
                return SendResult(success=True, message_id=_stream_message_id(request_id))

            delta_message_id = await self._send_stream_delta(chat_id, content, accumulated=True)
            return SendResult(success=True, message_id=delta_message_id or message_id)
        except Exception as exc:
            logger.warning("[ax] edit failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_exec_approval(
        self,
        chat_id: str,
        command: str,
        session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del session_key, metadata
        prefix = getattr(self, "typed_command_prefix", "/") or "/"
        cmd_preview = command[:200] + "..." if len(command) > 200 else command
        prompt = (
            "⚠️ **Dangerous command requires approval:**\n"
            f"```\n{cmd_preview}\n```\n"
            f"Reason: {description}\n\n"
            f"Reply `{prefix}approve` to execute, `{prefix}approve session` to approve this pattern "
            f"for the session, `{prefix}approve always` to approve permanently, or `{prefix}deny` to cancel."
        )
        return await self._send_input_required(
            chat_id,
            prompt,
            kind="exec_approval",
            commands=[
                f"{prefix}approve",
                f"{prefix}approve session",
                f"{prefix}approve always",
                f"{prefix}deny",
            ],
        )

    async def send_slash_confirm(
        self,
        chat_id: str,
        title: str,
        message: str,
        session_key: str,
        confirm_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del title, session_key, confirm_id, metadata
        prefix = getattr(self, "typed_command_prefix", "/") or "/"
        return await self._send_input_required(
            chat_id,
            message,
            kind="slash_confirm",
            commands=[f"{prefix}approve", f"{prefix}always", f"{prefix}cancel"],
        )

    def render_message_event(self, event: Any, sink: Any) -> None:
        """Map Hermes structured text events to AX live delta frames."""
        chat_id = str(getattr(sink, "chat_id", "") or "")
        if not chat_id:
            return
        event_name = type(event).__name__
        text = str(getattr(event, "text", "") or "")
        if event_name == "MessageChunk" and text:
            self._schedule_stream_delta(chat_id, text, from_message_chunk=True)
        elif event_name == "Commentary" and text:
            self._schedule_stream_delta(chat_id, text)

    def format_tool_event(self, event: Any, *, mode: str = "all", preview_max_len: int = 40) -> Optional[str]:
        """Encode Hermes structured tool events for send_progress_messages."""
        del mode
        event_name = type(event).__name__
        tool_name = str(getattr(event, "tool_name", "") or "")
        if not tool_name:
            return None
        if event_name == "ToolCallChunk":
            preview = str(getattr(event, "preview", "") or "")
            if preview and preview_max_len > 0 and len(preview) > preview_max_len:
                preview = preview[: max(0, preview_max_len - 3)] + "..."
            return AX_TOOL_EVENT_PREFIX + json.dumps(
                {
                    "event": "started",
                    "tool": tool_name,
                    "preview": preview,
                },
                separators=(",", ":"),
            )
        if event_name == "ToolCallFinished":
            duration = float(getattr(event, "duration", 0.0) or 0.0)
            ok = bool(getattr(event, "ok", True))
            return AX_TOOL_EVENT_PREFIX + json.dumps(
                {
                    "event": "completed",
                    "tool": tool_name,
                    "durationMs": int(duration * 1000),
                    "error": not ok,
                },
                separators=(",", ":"),
            )
        return None

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
        self._last_request_by_chat_id[chat_id] = request_id
        self._sequence_by_request_id[request_id] = 0
        self._stream_text_by_request_id[request_id] = ""
        self._message_chunk_seen_request_ids.discard(request_id)
        self._completed_request_ids.discard(request_id)
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
                "capabilities": STREAM_CAPABILITIES,
                "sentAt": _now_iso(),
            }
        )

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        if not self._ws or self._ws.closed:
            raise RuntimeError("ax websocket is not connected")
        await self._ws.send_str(json.dumps(payload, separators=(",", ":")))

    def _request_id_for_chat(self, chat_id: str) -> str:
        return (
            self._request_by_chat_id.get(chat_id)
            or self._last_request_by_chat_id.get(chat_id)
            or chat_id.replace("ax:", "", 1)
        )

    def _is_active_chat(self, chat_id: str) -> bool:
        return chat_id in self._request_by_chat_id

    def _next_sequence(self, request_id: str) -> int:
        sequence = self._sequence_by_request_id.get(request_id, 0) + 1
        self._sequence_by_request_id[request_id] = sequence
        return sequence

    async def _send_stream_delta(
        self,
        chat_id: str,
        content: str,
        *,
        accumulated: Optional[bool],
        cancel_pending: bool = True,
        from_message_chunk: bool = False,
    ) -> Optional[str]:
        request_id = self._request_id_for_chat(chat_id)
        if request_id in self._completed_request_ids:
            return None
        if from_message_chunk:
            self._message_chunk_seen_request_ids.add(request_id)
        if cancel_pending:
            self._cancel_pending_done(request_id)
        clean_content = _strip_stream_cursor(content)
        previous = self._stream_text_by_request_id.get(request_id, "")
        if accumulated is True:
            if request_id in self._message_chunk_seen_request_ids:
                delta, next_text = _delta_from_snapshot_after_chunks(previous, clean_content)
            else:
                delta, next_text = _delta_from_accumulated(previous, clean_content)
        elif accumulated is False:
            delta, next_text = clean_content, previous + clean_content
        else:
            delta, next_text = _delta_from_unknown_stream_update(previous, clean_content)
        self._stream_text_by_request_id[request_id] = next_text
        if not delta:
            return None
        payload = {
            "type": "agent.delta",
            "messageId": _message_id("hdelta"),
            "requestId": request_id,
            "sequence": self._next_sequence(request_id),
            "delta": delta,
            "sentAt": _now_iso(),
        }
        await self._send_json(payload)
        return _stream_message_id(request_id)

    async def _send_input_required(
        self,
        chat_id: str,
        prompt: str,
        *,
        kind: str,
        commands: list[str],
    ) -> SendResult:
        request_id = self._request_id_for_chat(chat_id)
        if request_id in self._completed_request_ids:
            return SendResult(success=True, message_id=_message_id("hdup"))
        self._cancel_pending_done(request_id)
        payload: Dict[str, Any] = {
            "type": "agent.input_required",
            "messageId": _message_id("hinput"),
            "requestId": request_id,
            "kind": kind,
            "prompt": prompt,
            "commands": commands,
            "sentAt": _now_iso(),
        }
        await self._send_json(payload)
        return SendResult(success=True, message_id=payload["messageId"])

    def _schedule_stream_delta(
        self,
        chat_id: str,
        content: str,
        *,
        from_message_chunk: bool = False,
    ) -> None:
        if not content:
            return
        self._schedule_coro(
            self._send_stream_delta(
                chat_id,
                content,
                accumulated=False,
                from_message_chunk=from_message_chunk,
            )
        )

    async def _send_tool_event(self, chat_id: str, event: Dict[str, Any]) -> str:
        request_id = self._request_id_for_chat(chat_id)
        if request_id in self._completed_request_ids:
            return _tool_message_id(request_id)
        self._cancel_pending_done(request_id)
        kind = str(event.get("event") or "started")
        tool = str(event.get("tool") or "tool")
        if kind == "completed":
            payload = {
                "type": "agent.tool.completed",
                "messageId": _message_id("htooldone"),
                "requestId": request_id,
                "sequence": self._next_sequence(request_id),
                "tool": tool,
                "durationMs": int(event.get("durationMs") or 0),
                "error": bool(event.get("error")),
                "sentAt": _now_iso(),
            }
        else:
            payload = {
                "type": "agent.tool.started",
                "messageId": _message_id("htoolstart"),
                "requestId": request_id,
                "sequence": self._next_sequence(request_id),
                "tool": tool,
                "sentAt": _now_iso(),
            }
            preview = str(event.get("preview") or "").strip()
            if preview:
                payload["preview"] = preview[:240]
        await self._send_json(payload)
        return _tool_message_id(request_id)

    async def _send_done(self, chat_id: str, content: str, *, cancel_pending: bool = True) -> str:
        request_id = self._request_id_for_chat(chat_id)
        if request_id in self._completed_request_ids:
            return _message_id("hdup")
        if cancel_pending:
            self._cancel_pending_done(request_id)
        payload = {
            "type": "agent.done",
            "messageId": _message_id("hmsg"),
            "requestId": request_id,
            "message": {"text": _strip_stream_cursor(content)},
            "completedAt": _now_iso(),
        }
        await self._send_json(payload)
        self._complete_request(chat_id, request_id)
        return payload["messageId"]

    def _complete_request(self, chat_id: str, request_id: str) -> None:
        if self._request_by_chat_id.get(chat_id) == request_id:
            self._request_by_chat_id.pop(chat_id, None)
        self._last_request_by_chat_id[chat_id] = request_id
        self._completed_request_ids.add(request_id)
        self._sequence_by_request_id.pop(request_id, None)
        self._stream_text_by_request_id.pop(request_id, None)
        self._message_chunk_seen_request_ids.discard(request_id)
        self._cancel_pending_done(request_id)

    def _schedule_done(self, chat_id: str, request_id: str, content: str) -> None:
        self._cancel_pending_done(request_id)
        loop = self._loop
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if not loop or loop.is_closed():
            loop = running_loop
        if not loop or not loop.is_running():
            logger.debug("[ax] dropping delayed done: event loop is not running")
            return

        async def _delayed_done() -> None:
            try:
                await asyncio.sleep(1.0)
                if request_id not in self._completed_request_ids:
                    self._pending_done_tasks.pop(request_id, None)
                    await self._send_done(chat_id, content, cancel_pending=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("[ax] delayed done failed: %s", exc)

        if running_loop is loop:
            task = loop.create_task(_delayed_done())
        else:
            task = asyncio.run_coroutine_threadsafe(_delayed_done(), loop)
        self._pending_done_tasks[request_id] = task

    def _cancel_pending_done(self, request_id: str) -> None:
        task = self._pending_done_tasks.pop(request_id, None)
        if task and not task.done():
            task.cancel()

    def _schedule_coro(self, coro: Any) -> None:
        loop = self._loop
        if not loop or loop.is_closed():
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                coro.close()
                logger.debug("[ax] dropping stream event: no running event loop")
                return
        if loop.is_running():
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None
            if running_loop is loop:
                task = loop.create_task(coro)
                task.add_done_callback(_log_future_exception)
            else:
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                future.add_done_callback(_log_future_exception)
            return
        coro.close()
        logger.debug("[ax] dropping stream event: event loop is not running")


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


def _stream_message_id(request_id: str) -> str:
    return f"{STREAM_MESSAGE_PREFIX}_{request_id}"


def _tool_message_id(request_id: str) -> str:
    return f"{TOOL_MESSAGE_PREFIX}_{request_id}"


def _is_tool_message_id(message_id: str) -> bool:
    return str(message_id or "").startswith(f"{TOOL_MESSAGE_PREFIX}_")


def _decode_tool_event(content: str) -> Optional[Dict[str, Any]]:
    text = str(content or "").strip()
    if not text.startswith(AX_TOOL_EVENT_PREFIX):
        return None
    try:
        decoded = json.loads(text[len(AX_TOOL_EVENT_PREFIX) :])
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _tool_event_from_progress_text(content: str) -> Optional[Dict[str, Any]]:
    text = str(content or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        return {"event": "started", "tool": "terminal", "preview": _preview_from_text(text)}
    first_line = text.splitlines()[0].strip()
    parts = first_line.split(maxsplit=1)
    if not parts:
        return None
    candidate = first_line
    if len(parts) > 1 and not parts[0][:1].isalnum():
        candidate = parts[1].strip()
    match = re.match(r"([A-Za-z][A-Za-z0-9_.-]{0,80})(?:\(|:|\.{3}|\s|$)", candidate)
    if not match:
        return None
    tool = match.group(1)
    preview = ""
    if ":" in candidate:
        preview = candidate.split(":", 1)[1].strip().strip("\"'")
    return {"event": "started", "tool": tool, "preview": preview[:240]}


def _preview_from_text(text: str) -> str:
    lines = [line for line in text.splitlines() if line.strip("`").strip()]
    return " ".join(lines)[:240]


def _looks_like_streaming_preview(content: str) -> bool:
    text = str(content or "")
    return text.endswith("\u2589") or text.endswith("\u2588")


def _strip_stream_cursor(content: str) -> str:
    text = str(content or "")
    while text.endswith("\u2589") or text.endswith("\u2588"):
        text = text[:-1]
    return text


def _delta_from_accumulated(previous: str, current: str) -> tuple[str, str]:
    if not current:
        return "", previous
    if not previous:
        return current, current
    if current == previous or current in previous or previous.endswith(current):
        return "", previous
    if current.startswith(previous):
        return current[len(previous) :], current
    overlap = _suffix_prefix_overlap(previous, current)
    if overlap >= 8:
        delta = current[overlap:]
        return delta, previous + delta
    common = _common_prefix_len(previous, current)
    if common >= 8:
        delta = current[common:]
        if delta:
            return delta, previous + delta
    if _looks_like_replayed_snapshot(previous, current):
        return "", previous
    return current, previous + current


def _delta_from_snapshot_after_chunks(previous: str, current: str) -> tuple[str, str]:
    if not current:
        return "", previous
    if not previous:
        return current, current
    if current == previous or current in previous or previous.startswith(current):
        return "", previous
    if current.startswith(previous):
        return current[len(previous) :], current
    # Once native MessageChunk deltas are flowing, a non-prefix snapshot is a
    # rewrite of text AX has already appended. Replaying it would duplicate.
    return "", previous


def _delta_from_unknown_stream_update(previous: str, current: str) -> tuple[str, str]:
    if not current:
        return "", previous
    if not previous:
        return current, current
    if current == previous:
        return "", previous
    if current.startswith(previous):
        return current[len(previous) :], current
    if _looks_like_replayed_snapshot(previous, current):
        return "", previous
    overlap = _suffix_prefix_overlap(previous, current)
    if overlap >= 8:
        delta = current[overlap:]
        return delta, previous + delta
    return current, previous + current


def _looks_like_replayed_snapshot(previous: str, current: str) -> bool:
    if len(current) < 16:
        return False
    if current in previous or previous.endswith(current):
        return True
    if len(current) > len(previous):
        return False
    common = _common_prefix_len(previous, current)
    shorter = min(len(previous), len(current))
    return shorter >= 16 and common / shorter >= 0.75


def _common_prefix_len(left: str, right: str) -> int:
    limit = min(len(left), len(right))
    index = 0
    while index < limit and left[index] == right[index]:
        index += 1
    return index


def _suffix_prefix_overlap(left: str, right: str) -> int:
    max_size = min(len(left), len(right))
    for size in range(max_size, 0, -1):
        if left.endswith(right[:size]):
            return size
    return 0


def _log_future_exception(future: Any) -> None:
    try:
        exc = future.exception()
    except asyncio.CancelledError:
        return
    except Exception as err:
        logger.debug("[ax] stream event failed: %s", err)
        return
    if exc:
        logger.debug("[ax] stream event failed: %s", exc)
