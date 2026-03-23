# RayRunner

RayRunner is an intelligent solar assistant that helps you monitor energy generation, track system health, and get alerts when cleaning or maintenance may be required.

It combines ShineMonitor data, weather context, and recent trends to give practical recommendations that improve efficiency and savings.

## Features

- Real-time and daily solar performance tracking
- Cleaning/maintenance recommendation alerts
- Trend analysis from recent history
- Weather-aware performance checks
- Telegram bot support for quick updates and commands

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Update `.env` with your ShineMonitor credentials:

- `SHINEMONITOR_BASE_URL`
- `SHINEMONITOR_TOKEN`
- `SHINEMONITOR_SECRET`
- `SHINEMONITOR_PLANT_ID`

Optional but useful:

- `SOLAR_LATITUDE`, `SOLAR_LONGITUDE`, `SOLAR_TIMEZONE`
- `SOLAR_DB_PATH` (default: `solar_data.db`)

Telegram (optional):

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_ALLOWED_CHAT_IDS`
- `TELEGRAM_MODE` (`polling` or `webhook`)
- `TELEGRAM_WEBHOOK_URL` (for webhook mode)

## Run the App

Start API server:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Useful API routes:

- `GET /health` - service health
- `GET /status` - current status + recommendation
- `GET /today` - today's summary + recommendation
- `GET /history?days=7` - last N days (1 to 30)
- `GET /shinemonitor/test` - raw ShineMonitor test call

## Telegram Usage (Optional)

After setting Telegram env variables, run the app and use:

- `/status`
- `/today`
- `/history 7`
- `/run`
- `/report`
- `/weather`
- `/quality`
- `/alerts`
- `/setalerts ratio=0.65 streak=2 rain=3 cooldown_h=24`

### Webhook Setup

Use webhook mode if you want Telegram to push updates to your server.

Required env values:

- `TELEGRAM_MODE=webhook`
- `TELEGRAM_WEBHOOK_URL=https://your-domain/telegram/webhook`
- `TELEGRAM_WEBHOOK_SECRET=your_secret` (recommended)

How to create `TELEGRAM_WEBHOOK_SECRET`:

- This secret is not provided by Telegram. You create it yourself.
- Example:

```bash
openssl rand -hex 32
```

Webhook endpoint:

- `POST /telegram/webhook`
- Validates `X-Telegram-Bot-Api-Secret-Token` when `TELEGRAM_WEBHOOK_SECRET` is set

## Use Cases

- Get notified when panel cleaning is likely needed
- Monitor underperformance early and avoid energy loss
- Track output trends over the last week/month
- Receive simple daily guidance to keep system efficiency high
- Use Telegram for quick remote checks without logging into dashboards

## Quick Test

Run the direct ShineMonitor API test script:

```bash
python scripts/test_shinemonitor.py
```
