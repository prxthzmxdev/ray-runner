from __future__ import annotations

import asyncio
import hashlib
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict

import httpx
from dotenv import load_dotenv

load_dotenv()

PLANT_ACTION = "queryPlantActiveOuputPowerOneDay"
_CRED_SAFETY_MARGIN_S = 60.0

# Match browser requests to web.shinemonitor.com (Referer / UA / client hints).
_SHINEMONITOR_BROWSER_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.shinemonitor.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": (
        '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"'
    ),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "DNT": "1",
}

_cred_lock = asyncio.Lock()
_cred_cache: dict[str, Any] | None = None


def _normalize_public_base_url(url: str) -> str:
    """Match browser/curl: https://web.shinemonitor.com/public/"""
    u = url.strip()
    if u.startswith("http://web.shinemonitor.com"):
        u = "https://" + u[len("http://") :]
    if not u.endswith("/"):
        u = u + "/"
    return u


def _salt_ms() -> int:
    """Same as legacy script: int(round(time.time() * 1000))."""
    return int(round(time.time() * 1000))


def _build_auth_url(
    base_url: str,
    usr: str,
    plain_password: str,
    company_key: str,
    auth_action: str = "auth",
) -> str:
    """
    Sign (legacy): pwdaction = str(salt) + sha1(pwd).hexdigest() + action_fragment
    where action_fragment = '&action=auth&usr='+usr+'&company-key='+key

    Request URL matches browser/curl:
    .../public/?sign=...&salt=...&action=auth&usr=...&company-key=...
    """
    salt = _salt_ms()
    pow_sha1 = hashlib.sha1(plain_password.encode("utf-8")).hexdigest()
    action_fragment = (
        "&action="
        + str(auth_action)
        + "&usr="
        + str(usr)
        + "&company-key="
        + str(company_key)
    )
    pwdaction = str(salt) + str(pow_sha1) + action_fragment
    sign = hashlib.sha1(pwdaction.encode("utf-8")).hexdigest()
    base = _normalize_public_base_url(base_url)
    # Same parameter order as curl (sign, salt, action, usr, company-key).
    return (
        f"{base}?sign={sign}&salt={salt}&action={auth_action}"
        f"&usr={usr}&company-key={company_key}"
    )


def _build_plant_signed_url(
    base_url: str,
    secret: str,
    token: str,
    plant_id: str,
    query_date: str,
    i18n: str,
    lang: str,
) -> str:
    """
    Legacy-compatible signed plant URL (buildRequestUrl pattern):
    reqaction = str(salt) + secret + token + action_params
    sign = sha1(reqaction)
    url = base + '?sign=' + sign + '&salt=' + salt + '&token=' + token + action_params
    """
    salt_val = _salt_ms()
    salt = str(salt_val)
    action_params = (
        "&action="
        + PLANT_ACTION
        + "&plantid="
        + str(plant_id)
        + "&date="
        + str(query_date)
        + "&i18n="
        + str(i18n)
        + "&lang="
        + str(lang)
    )
    reqaction = salt + secret + token + action_params
    sign = hashlib.sha1(reqaction.encode("utf-8")).hexdigest()
    return base_url + "?sign=" + sign + "&salt=" + salt + "&token=" + token + action_params


async def _ensure_shinemonitor_credentials(
    client: httpx.AsyncClient,
    base_url: str,
    username: str,
    password: str,
    company_key: str,
    auth_action: str,
) -> tuple[str, str]:
    global _cred_cache

    now = time.time()
    async with _cred_lock:
        if (
            _cred_cache
            and now + _CRED_SAFETY_MARGIN_S < _cred_cache["expires_at"]
        ):
            return _cred_cache["token"], _cred_cache["secret"]

        auth_url = _build_auth_url(
            base_url, username, password, company_key, auth_action=auth_action
        )
        response = await client.get(
            auth_url,
            headers=_SHINEMONITOR_BROWSER_HEADERS,
        )
        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            raise ValueError(f"ShineMonitor auth expected JSON, got: {content_type!r}")

        payload: Any = response.json()
        if payload.get("err") != 0:
            desc = payload.get("desc", "unknown")
            raise ValueError(f"ShineMonitor auth failed: err={payload.get('err')!r} desc={desc!r}")

        dat = payload.get("dat") or {}
        token = dat.get("token")
        secret = dat.get("secret")
        expire = dat.get("expire")
        if not token or not secret or expire is None:
            raise ValueError("ShineMonitor auth response missing token, secret, or expire")

        expires_at = now + float(expire)
        _cred_cache = {
            "token": str(token),
            "secret": str(secret),
            "expires_at": expires_at,
        }
        return _cred_cache["token"], _cred_cache["secret"]


async def call_shinemonitor_api(date_override: str | None = None) -> Dict[str, Any]:
    """Call the ShineMonitor plant API using credentials from action=auth."""
    base_url = os.getenv("SHINEMONITOR_BASE_URL")
    username = (os.getenv("SHINEMONITOR_USERNAME") or "").strip()
    password = os.getenv("SHINEMONITOR_PASSWORD") or ""
    company_key = (os.getenv("SHINEMONITOR_COMPANY_KEY") or "").strip()
    auth_action = (os.getenv("SHINEMONITOR_AUTH_ACTION") or "auth").strip() or "auth"
    if auth_action not in ("auth", "authEmail"):
        raise ValueError(
            "SHINEMONITOR_AUTH_ACTION must be 'auth' or 'authEmail'"
        )
    plant_id = os.getenv("SHINEMONITOR_PLANT_ID")
    env_override_date = os.getenv("SHINEMONITOR_DATE", "").strip()
    dynamic_date = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
    query_date = date_override or env_override_date or dynamic_date
    i18n = os.getenv("SHINEMONITOR_I18N", "en_US")
    lang = os.getenv("SHINEMONITOR_LANG", "en_US")
    timeout = float(os.getenv("SHINEMONITOR_TIMEOUT", "20"))

    if not base_url:
        raise ValueError("SHINEMONITOR_BASE_URL is not set in environment")
    if not username:
        raise ValueError("SHINEMONITOR_USERNAME is not set in environment")
    if not password:
        raise ValueError("SHINEMONITOR_PASSWORD is not set in environment")
    if not company_key:
        raise ValueError("SHINEMONITOR_COMPANY_KEY is not set in environment")
    if not plant_id:
        raise ValueError("SHINEMONITOR_PLANT_ID is not set in environment")

    base_url = _normalize_public_base_url(base_url)

    async with httpx.AsyncClient(timeout=timeout) as client:
        token, secret = await _ensure_shinemonitor_credentials(
            client,
            base_url,
            username,
            password,
            company_key,
            auth_action,
        )
        signed_url = _build_plant_signed_url(
            base_url=base_url,
            secret=secret,
            token=token,
            plant_id=str(plant_id),
            query_date=query_date,
            i18n=i18n,
            lang=lang,
        )
        response = await client.get(
            signed_url,
            headers=_SHINEMONITOR_BROWSER_HEADERS,
        )
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
