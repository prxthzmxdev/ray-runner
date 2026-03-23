from datetime import date as date_type

from fastapi import FastAPI, Header, HTTPException

from app.data_store import ensure_db
from app.solar_advisor import get_snapshot
from app.shinemonitor import call_shinemonitor_api
from app.telegram_bot import TelegramBotService

app = FastAPI(title="Solar Bot API", version="0.1.0")
telegram_bot = TelegramBotService()


@app.on_event("startup")
async def startup_event() -> None:
    await ensure_db()
    await telegram_bot.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await telegram_bot.stop()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/shinemonitor/test")
async def shinemonitor_test(date: str | None = None) -> dict:
    if date:
        try:
            date_type.fromisoformat(date)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {exc}") from exc
    try:
        return await call_shinemonitor_api(date_override=date)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/status")
async def status() -> dict:
    try:
        snapshot = await get_snapshot(days=7, use_cache=True)
        return {
            "generated_at": snapshot["generated_at"],
            "generated_at_local": snapshot["generated_at_local"],
            "timezone": snapshot["timezone"],
            "today": snapshot["today"],
            "recommendation": snapshot["recommendation"],
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/today")
async def today() -> dict:
    try:
        snapshot = await get_snapshot(days=7, use_cache=True)
        return {
            "generated_at": snapshot["generated_at"],
            "generated_at_local": snapshot["generated_at_local"],
            "timezone": snapshot["timezone"],
            "today": snapshot["today"],
            "recommendation": snapshot["recommendation"],
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/history")
async def history(days: int = 7) -> dict:
    try:
        clamped_days = max(1, min(days, 30))
        snapshot = await get_snapshot(days=clamped_days, use_cache=True)
        return {
            "generated_at": snapshot["generated_at"],
            "generated_at_local": snapshot["generated_at_local"],
            "timezone": snapshot["timezone"],
            "history_days": clamped_days,
            "history": snapshot["history"][:clamped_days],
            "recommendation": snapshot["recommendation"],
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/telegram/webhook")
async def telegram_webhook(
    update: dict,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
) -> dict[str, bool]:
    if not telegram_bot.enabled:
        return {"ok": True}
    if not telegram_bot.webhook_enabled:
        raise HTTPException(status_code=400, detail="Telegram bot is not in webhook mode")
    if not telegram_bot.validate_webhook_secret(x_telegram_bot_api_secret_token):
        raise HTTPException(status_code=403, detail="Invalid webhook secret")
    try:
        await telegram_bot.handle_webhook_update(update)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True}
