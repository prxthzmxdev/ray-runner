from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx

from app.data_store import set_setting
from app.solar_advisor import (
    build_alerts_text,
    build_history_text,
    build_quality_text,
    build_report_text,
    build_run_text,
    build_status_text,
    build_today_text,
    build_weather_text,
)


class TelegramBotService:
    def __init__(self) -> None:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self._token = token
        self._base_url = f"https://api.telegram.org/bot{token}" if token else ""
        self._allowed_chat_ids = _parse_allowed_chat_ids(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", ""))
        self._mode = os.getenv("TELEGRAM_MODE", "polling").strip().lower() or "polling"
        self._webhook_url = os.getenv("TELEGRAM_WEBHOOK_URL", "").strip()
        self._webhook_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._poll_timeout = int(os.getenv("TELEGRAM_POLL_TIMEOUT", "25"))
        self._request_timeout = float(os.getenv("TELEGRAM_REQUEST_TIMEOUT", "30"))

    @property
    def enabled(self) -> bool:
        return bool(self._token)

    @property
    def webhook_enabled(self) -> bool:
        return self.enabled and self._mode == "webhook"

    def validate_webhook_secret(self, received_secret: str | None) -> bool:
        if not self._webhook_secret:
            return True
        return received_secret == self._webhook_secret

    async def start(self) -> None:
        if not self.enabled or self._running:
            return

        await self._configure_bot()

        self._running = True
        if self.webhook_enabled:
            return

        self._task = asyncio.create_task(self._poll_loop(), name="telegram-poll-loop")

    async def stop(self) -> None:
        self._running = False
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def handle_webhook_update(self, update: dict[str, Any]) -> None:
        if not self.enabled:
            return
        async with httpx.AsyncClient(timeout=self._request_timeout) as client:
            await self._handle_update(client, update)

    async def _configure_bot(self) -> None:
        async with httpx.AsyncClient(timeout=self._request_timeout) as client:
            await self._set_commands(client)
            if self.webhook_enabled and self._webhook_url:
                await self._set_webhook(client)

    async def _set_commands(self, client: httpx.AsyncClient) -> None:
        commands = [
            {"command": "help", "description": "Show all commands"},
            {"command": "status", "description": "Current plant status and latest decision"},
            {"command": "today", "description": "Today energy, expected, performance"},
            {"command": "run", "description": "Force health-check fetch now"},
            {"command": "report", "description": "Force final report now"},
            {"command": "history", "description": "Last 7 days energy and decisions"},
            {"command": "weather", "description": "Today weather class and sunrise/sunset"},
            {"command": "quality", "description": "Data completeness and anomalies"},
            {"command": "alerts", "description": "Show alert settings"},
            {"command": "setalerts", "description": "Update alert thresholds/cooldown"},
        ]
        payload = {"commands": commands}
        try:
            response = await client.post(f"{self._base_url}/setMyCommands", json=payload)
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            return

    async def _set_webhook(self, client: httpx.AsyncClient) -> None:
        payload: dict[str, Any] = {
            "url": self._webhook_url,
            "allowed_updates": ["message", "edited_message"],
            "drop_pending_updates": False,
        }
        if self._webhook_secret:
            payload["secret_token"] = self._webhook_secret
        try:
            response = await client.post(f"{self._base_url}/setWebhook", json=payload)
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            return

    async def _poll_loop(self) -> None:
        offset: int | None = None
        async with httpx.AsyncClient(timeout=self._request_timeout) as client:
            while self._running:
                try:
                    params: dict[str, Any] = {"timeout": self._poll_timeout}
                    if offset is not None:
                        params["offset"] = offset

                    response = await client.get(f"{self._base_url}/getUpdates", params=params)
                    response.raise_for_status()
                    payload = response.json()

                    for update in payload.get("result", []):
                        if not isinstance(update, dict):
                            continue
                        update_id = update.get("update_id")
                        if isinstance(update_id, int):
                            offset = update_id + 1
                        await self._handle_update(client, update)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    await asyncio.sleep(3)

    async def _handle_update(self, client: httpx.AsyncClient, update: dict[str, Any]) -> None:
        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            return

        chat = message.get("chat", {})
        chat_id = chat.get("id")
        if chat_id is None:
            return

        if self._allowed_chat_ids and str(chat_id) not in self._allowed_chat_ids:
            return

        text = str(message.get("text", "")).strip()
        if not text:
            return

        command, arg = _parse_command(text)
        verbose = _is_verbose_arg(arg)
        if command == "status":
            reply = await build_status_text(verbose=verbose)
        elif command == "today":
            reply = await build_today_text(verbose=verbose)
        elif command == "run":
            reply = await build_run_text()
        elif command == "report":
            reply = await build_report_text(days=7, force_refresh=True)
        elif command == "history":
            days = _safe_days_arg(arg, default=7)
            reply = await build_history_text(days=days, verbose=verbose)
        elif command == "weather":
            reply = await build_weather_text(verbose=verbose)
        elif command == "quality":
            days = _safe_days_arg(arg, default=7)
            reply = await build_quality_text(days=days, verbose=verbose)
        elif command == "alerts":
            reply = await build_alerts_text()
        elif command == "setalerts":
            reply = await _handle_setalerts(arg)
        elif command in {"start", "help"}:
            reply = (
                "Solar Monitor Bot Commands\n"
                "- /status : quick live status + recommended next action\n"
                "- /today : detailed today report (energy, weather, quality)\n"
                "- /run : force immediate fresh fetch + decision\n"
                "- /report : force final combined report now\n"
                "- /history : last 7 days summary\n"
                "- /weather : weather class + rain/cloud + sunrise/sunset\n"
                "- /quality : data completeness + anomalies\n"
                "- /alerts : show active thresholds and cooldown\n"
                "- /setalerts ratio=0.65 streak=2 rain=3 cooldown_h=24\n"
                "- /history 14 : custom history (1..30)\n"
                "- add 'verbose' for full diagnostics (e.g. /status verbose)\n"
                "Tip: use /history 7 for quick trend review."
            )
        else:
            return

        await self._send_message(client, chat_id=chat_id, text=reply)

    async def _send_message(self, client: httpx.AsyncClient, chat_id: int | str, text: str) -> None:
        trimmed = text if len(text) <= 4000 else text[:3990] + "\n..."
        payload = {
            "chat_id": chat_id,
            "text": trimmed,
            "disable_web_page_preview": True,
        }
        response = await client.post(f"{self._base_url}/sendMessage", json=payload)
        response.raise_for_status()



def _parse_allowed_chat_ids(raw: str) -> set[str]:
    return {item.strip() for item in raw.split(",") if item.strip()}



def _parse_command(text: str) -> tuple[str, str | None]:
    token = text.split()[0]
    args = text.split(maxsplit=1)
    arg = args[1].strip() if len(args) > 1 else None

    normalized = token.lower()
    if normalized.startswith("/"):
        normalized = normalized[1:]
    if "@" in normalized:
        normalized = normalized.split("@", maxsplit=1)[0]

    return normalized, arg



def _safe_days_arg(arg: str | None, default: int = 7) -> int:
    if not arg:
        return default
    for token in arg.split():
        try:
            value = int(token)
            return max(1, min(value, 30))
        except ValueError:
            continue
    return default


def _is_verbose_arg(arg: str | None) -> bool:
    if not arg:
        return False
    value = arg.strip().lower()
    return value in {"verbose", "full", "details"} or value.endswith(" verbose")


async def _handle_setalerts(arg: str | None) -> str:
    if not arg:
        return (
            "Solar Monitor - SETALERTS\n\n"
            "[DECISION]\n"
            "State: WATCH\n"
            "Recommendation: Missing arguments.\n"
            "Confidence: INFO\n"
            "Why: No key=value pairs were provided.\n\n"
            "[NEXT ACTION]\n"
            "- Now: /setalerts ratio=0.62 streak=2 rain=4 cooldown_h=24\n"
            "- Later: Run /alerts to verify active values."
        )

    updates = _parse_setalerts_args(arg)
    if not updates:
        return (
            "Solar Monitor - SETALERTS\n\n"
            "[DECISION]\n"
            "State: ERROR\n"
            "Recommendation: No valid settings found.\n"
            "Confidence: INFO\n"
            "Why: Allowed keys are ratio, streak, rain, cooldown_h.\n\n"
            "[NEXT ACTION]\n"
            "- Now: /setalerts ratio=0.62 streak=2 rain=4 cooldown_h=24\n"
            "- Later: Run /alerts to verify active values."
        )

    for key, value in updates.items():
        await set_setting(key, value)

    lines = [
        "Solar Monitor - SETALERTS",
        "",
        "[DECISION]",
        "State: OK",
        "Recommendation: Alert settings updated.",
        "Confidence: INFO",
        "Why: New thresholds are persisted in the local DB.",
        "",
        "[KEY METRICS]",
    ]
    if "clean_ratio_threshold" in updates:
        lines.append(f"- ratio={updates['clean_ratio_threshold']}")
    if "clear_streak_days" in updates:
        lines.append(f"- streak={updates['clear_streak_days']}")
    if "rain_postpone_mm" in updates:
        lines.append(f"- rain={updates['rain_postpone_mm']}")
    if "alert_cooldown_hours" in updates:
        lines.append(f"- cooldown_h={updates['alert_cooldown_hours']}")
    lines.extend(
        [
            "",
            "[NEXT ACTION]",
            "- Now: Run /alerts to confirm all active thresholds.",
            "- Later: Use /status and /history to observe behavior change.",
        ]
    )
    return "\n".join(lines)


def _parse_setalerts_args(arg: str) -> dict[str, str]:
    result: dict[str, str] = {}
    tokens = [token.strip() for token in arg.split() if token.strip()]
    for token in tokens:
        if "=" not in token:
            continue
        key, value = token.split("=", maxsplit=1)
        k = key.strip().lower()
        v = value.strip()
        if not v:
            continue
        if k == "ratio":
            parsed = _bounded_float(v, 0.3, 1.2)
            if parsed is not None:
                result["clean_ratio_threshold"] = f"{parsed:.3f}"
        elif k == "streak":
            parsed = _bounded_int(v, 1, 7)
            if parsed is not None:
                result["clear_streak_days"] = str(parsed)
        elif k == "rain":
            parsed = _bounded_float(v, 0.0, 20.0)
            if parsed is not None:
                result["rain_postpone_mm"] = f"{parsed:.2f}"
        elif k in {"cooldown_h", "cooldown"}:
            parsed = _bounded_int(v, 0, 168)
            if parsed is not None:
                result["alert_cooldown_hours"] = str(parsed)
    return result


def _bounded_float(value: str, low: float, high: float) -> float | None:
    try:
        val = float(value)
    except ValueError:
        return None
    if val < low or val > high:
        return None
    return val


def _bounded_int(value: str, low: int, high: int) -> int | None:
    try:
        val = int(float(value))
    except ValueError:
        return None
    if val < low or val > high:
        return None
    return val
