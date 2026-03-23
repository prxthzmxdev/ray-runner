from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS solar_daily_data (
    day TEXT PRIMARY KEY,
    response_json TEXT,
    status_code INTEGER,
    source_url TEXT,
    fetched_at TEXT NOT NULL,
    error TEXT
);

CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _db_path() -> str:
    return os.getenv("SOLAR_DB_PATH", "solar_data.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _now_utc_iso() -> str:
    return datetime.now(tz=ZoneInfo("UTC")).isoformat()


def ensure_db_sync() -> None:
    with _connect() as conn:
        conn.executescript(DB_SCHEMA)
        conn.commit()


async def ensure_db() -> None:
    await asyncio.to_thread(ensure_db_sync)


def get_day_record_sync(day: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT day, response_json, status_code, source_url, fetched_at, error
            FROM solar_daily_data
            WHERE day = ?
            """,
            (day,),
        ).fetchone()
        if not row:
            return None
        return {
            "day": row["day"],
            "response_json": row["response_json"],
            "status_code": row["status_code"],
            "source_url": row["source_url"],
            "fetched_at": row["fetched_at"],
            "error": row["error"],
        }


async def get_day_record(day: str) -> dict[str, Any] | None:
    return await asyncio.to_thread(get_day_record_sync, day)


def upsert_day_success_sync(day: str, response: dict[str, Any]) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO solar_daily_data (day, response_json, status_code, source_url, fetched_at, error)
            VALUES (?, ?, ?, ?, ?, NULL)
            ON CONFLICT(day) DO UPDATE SET
                response_json=excluded.response_json,
                status_code=excluded.status_code,
                source_url=excluded.source_url,
                fetched_at=excluded.fetched_at,
                error=NULL
            """,
            (
                day,
                json.dumps(response, ensure_ascii=True),
                response.get("status_code"),
                response.get("url"),
                _now_utc_iso(),
            ),
        )
        conn.commit()


async def upsert_day_success(day: str, response: dict[str, Any]) -> None:
    await asyncio.to_thread(upsert_day_success_sync, day, response)


def upsert_day_error_sync(day: str, error: str) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO solar_daily_data (day, response_json, status_code, source_url, fetched_at, error)
            VALUES (?, NULL, NULL, NULL, ?, ?)
            ON CONFLICT(day) DO UPDATE SET
                fetched_at=excluded.fetched_at,
                error=excluded.error
            """,
            (day, _now_utc_iso(), error[:2000]),
        )
        conn.commit()


async def upsert_day_error(day: str, error: str) -> None:
    await asyncio.to_thread(upsert_day_error_sync, day, error)


def get_setting_sync(key: str) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT value
            FROM bot_settings
            WHERE key = ?
            """,
            (key,),
        ).fetchone()
        if not row:
            return None
        return str(row["value"])


async def get_setting(key: str) -> str | None:
    return await asyncio.to_thread(get_setting_sync, key)


def get_settings_sync(keys: list[str]) -> dict[str, str]:
    if not keys:
        return {}
    placeholders = ",".join(["?"] * len(keys))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT key, value
            FROM bot_settings
            WHERE key IN ({placeholders})
            """,
            tuple(keys),
        ).fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}


async def get_settings(keys: list[str]) -> dict[str, str]:
    return await asyncio.to_thread(get_settings_sync, keys)


def set_setting_sync(key: str, value: str) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO bot_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (key, value, _now_utc_iso()),
        )
        conn.commit()


async def set_setting(key: str, value: str) -> None:
    await asyncio.to_thread(set_setting_sync, key, value)
