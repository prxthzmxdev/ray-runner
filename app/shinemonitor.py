from __future__ import annotations

import hashlib
import os
import time
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict

import httpx
from dotenv import load_dotenv

load_dotenv()


def generate_signed_url(
    base_url: str,
    token: str,
    secret: str,
    action_name: str,
    params: Dict[str, str],
) -> str:
    """Generate signed API URL for ShineMonitor-compatible APIs."""
    salt = str(int(time.time() * 1000))
    query_params = {"action": action_name, **params}
    action_str = "&" + urllib.parse.urlencode(query_params)
    raw_string = f"{salt}{secret}{token}{action_str}"
    sign = hashlib.sha1(raw_string.encode("utf-8")).hexdigest()

    return f"{base_url}?sign={sign}&salt={salt}&token={token}{action_str}"


async def call_shinemonitor_api(date_override: str | None = None) -> Dict[str, Any]:
    """Call the ShineMonitor API endpoint configured in environment variables."""
    base_url = os.getenv("SHINEMONITOR_BASE_URL")
    token = os.getenv("SHINEMONITOR_TOKEN")
    secret = os.getenv("SHINEMONITOR_SECRET")
    action_name = os.getenv("SHINEMONITOR_ACTION", "queryPlantActiveOuputPowerOneDay")
    plant_id = os.getenv("SHINEMONITOR_PLANT_ID")
    env_override_date = os.getenv("SHINEMONITOR_DATE", "").strip()
    dynamic_date = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
    query_date = date_override or env_override_date or dynamic_date
    i18n = os.getenv("SHINEMONITOR_I18N", "en_US")
    lang = os.getenv("SHINEMONITOR_LANG", "en_US")
    timeout = float(os.getenv("SHINEMONITOR_TIMEOUT", "20"))

    if not base_url:
        raise ValueError("SHINEMONITOR_BASE_URL is not set in environment")
    if not token:
        raise ValueError("SHINEMONITOR_TOKEN is not set in environment")
    if not secret:
        raise ValueError("SHINEMONITOR_SECRET is not set in environment")
    if not plant_id:
        raise ValueError("SHINEMONITOR_PLANT_ID is not set in environment")

    params = {
        "plantid": plant_id,
        "date": query_date,
        "i18n": i18n,
        "lang": lang,
    }
    signed_url = generate_signed_url(
        base_url=base_url,
        token=token,
        secret=secret,
        action_name=action_name,
        params=params,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(signed_url)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            payload: Any = response.json()
        else:
            payload = {"raw_text": response.text}

        return {
            "status_code": response.status_code,
            "url": str(response.url),
            "payload": payload,
        }
