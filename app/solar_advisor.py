from __future__ import annotations

import asyncio
import json
import os
import statistics
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.data_store import get_day_record, get_settings, upsert_day_error, upsert_day_success
from app.shinemonitor import call_shinemonitor_api

DEFAULT_TZ = "Asia/Kolkata"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
MIN_NON_ZERO_KW = 0.05
ALERT_SETTING_KEYS = [
    "clean_ratio_threshold",
    "clear_streak_days",
    "rain_postpone_mm",
    "alert_cooldown_hours",
]
ALERT_DEFAULTS = {
    "clean_ratio_threshold": 0.65,
    "clear_streak_days": 2,
    "rain_postpone_mm": 3.0,
    "alert_cooldown_hours": 24,
}

_snapshot_cache: dict[int, tuple[datetime, dict[str, Any]]] = {}
_snapshot_cache_lock = asyncio.Lock()


@dataclass
class WeatherDay:
    sunrise: datetime | None = None
    sunset: datetime | None = None
    precipitation_mm: float | None = None
    cloud_cover_pct: float | None = None
    weather_code: int | None = None
    daylight_hours: float | None = None
    sunshine_hours: float | None = None


@dataclass
class DayEvaluation:
    day: date
    samples_total: int
    daylight_samples: int
    non_zero_daylight_samples: int
    energy_kwh: float
    projected_energy_kwh: float | None
    peak_kw: float
    current_kw: float | None
    latest_timestamp: datetime | None
    daylight_hours: float
    completion_ratio: float
    sunrise: datetime
    sunset: datetime
    precipitation_mm: float | None
    cloud_cover_pct: float | None
    weather_code: int | None
    quality: str
    data_source: str
    fetched_at: str | None
    api_url: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.day.isoformat(),
            "samples_total": self.samples_total,
            "samples_daylight": self.daylight_samples,
            "samples_daylight_non_zero": self.non_zero_daylight_samples,
            "energy_estimate_kwh": round(self.energy_kwh, 3),
            "projected_energy_kwh": (
                round(self.projected_energy_kwh, 3)
                if self.projected_energy_kwh is not None
                else None
            ),
            "peak_output_kw": round(self.peak_kw, 3),
            "current_output_kw": round(self.current_kw, 3) if self.current_kw is not None else None,
            "latest_timestamp": self.latest_timestamp.isoformat() if self.latest_timestamp else None,
            "daylight_hours": round(self.daylight_hours, 3),
            "day_completion_ratio": round(self.completion_ratio, 3),
            "sunrise": self.sunrise.isoformat(),
            "sunset": self.sunset.isoformat(),
            "weather": {
                "precipitation_mm": self.precipitation_mm,
                "cloud_cover_pct": self.cloud_cover_pct,
                "weather_code": self.weather_code,
            },
            "quality": self.quality,
            "data_source": self.data_source,
            "fetched_at": self.fetched_at,
            "api_url": self.api_url,
            "error": self.error,
        }


async def get_snapshot(
    days: int = 7,
    use_cache: bool = True,
    force_today_refresh: bool = False,
) -> dict[str, Any]:
    days = max(1, min(days, 30))
    cache_age_seconds = int(os.getenv("SNAPSHOT_CACHE_SECONDS", "90"))
    now_utc = datetime.now(tz=ZoneInfo("UTC"))

    if use_cache and not force_today_refresh:
        cached = _snapshot_cache.get(days)
        if cached and (now_utc - cached[0]).total_seconds() <= cache_age_seconds:
            return cached[1]

    async with _snapshot_cache_lock:
        if use_cache and not force_today_refresh:
            cached = _snapshot_cache.get(days)
            if cached and (now_utc - cached[0]).total_seconds() <= cache_age_seconds:
                return cached[1]

        snapshot = await _build_snapshot(days, force_today_refresh=force_today_refresh)
        if not force_today_refresh:
            _snapshot_cache[days] = (datetime.now(tz=ZoneInfo("UTC")), snapshot)
        return snapshot


async def get_alert_settings() -> dict[str, float]:
    raw = await get_settings(ALERT_SETTING_KEYS)
    return {
        "clean_ratio_threshold": _as_float(raw.get("clean_ratio_threshold"), ALERT_DEFAULTS["clean_ratio_threshold"]),
        "clear_streak_days": float(_as_int(raw.get("clear_streak_days"), int(ALERT_DEFAULTS["clear_streak_days"]))),
        "rain_postpone_mm": _as_float(raw.get("rain_postpone_mm"), ALERT_DEFAULTS["rain_postpone_mm"]),
        "alert_cooldown_hours": float(_as_int(raw.get("alert_cooldown_hours"), int(ALERT_DEFAULTS["alert_cooldown_hours"]))),
    }


async def build_status_text(verbose: bool = False) -> str:
    snapshot = await get_snapshot(days=7, use_cache=False, force_today_refresh=True)
    today = snapshot["today"]
    rec = snapshot["recommendation"]
    weather = today.get("weather", {})
    reason = (rec.get("reasons") or ["n/a"])[0]
    decision = rec.get("decision", "")
    baseline = rec.get("baseline_kwh")
    perf_ratio = _perf_ratio(today, baseline)
    detailed = _render_template(
        header="Solar Monitor - STATUS",
        generated_at=snapshot["generated_at_local"],
        state=_severity_label(decision),
        recommendation=rec.get("summary", "n/a"),
        confidence=str(rec.get("confidence", "n/a")).upper(),
        why=reason,
        key_metrics=[
            f"Current: {_fmt(today.get('current_output_kw'), 'kW')}",
            f"Peak: {_fmt(today.get('peak_output_kw'), 'kW')}",
            f"Energy: {_fmt(today.get('energy_estimate_kwh'), 'kWh')} (Projected {_fmt(today.get('projected_energy_kwh'), 'kWh')})",
            f"Progress: {_pct(today.get('day_completion_ratio'))}",
            f"Perf Ratio vs Baseline: {_fmt_ratio(perf_ratio)}",
        ],
        context=[
            f"Weather: {_weather_class(weather.get('precipitation_mm'), weather.get('cloud_cover_pct'))} | rain {_fmt(weather.get('precipitation_mm'), 'mm')} | cloud {_fmt(weather.get('cloud_cover_pct'), '%')}",
            f"Data: {today.get('data_source', 'n/a')} | fetched {_short_ts(today.get('fetched_at'))} | quality {today.get('quality', 'n/a')}",
        ],
        next_now=_next_action(decision),
        next_later="Run /history 7 for trend confirmation.",
    )
    if verbose:
        return detailed

    confidence_inline = _confidence_inline(rec, today)
    return "\n".join(
        [
            "⚡ Solar Monitor - Live Status",
            "",
            f"{_emoji_for_state(_severity_label(decision))} {_summary_line(rec)} {confidence_inline}",
            "",
            "🔋 Output",
            f"• Current: {_fmt(today.get('current_output_kw'), 'kW')}",
            f"• Peak Today: {_fmt(today.get('peak_output_kw'), 'kW')}",
            f"• Energy: {_fmt(today.get('energy_estimate_kwh'), 'kWh')} ({_fmt_ratio(perf_ratio)} vs baseline)",
            "",
            "🌤 Context",
            f"• Weather: {_weather_class(weather.get('precipitation_mm'), weather.get('cloud_cover_pct'))} ({_fmt(weather.get('cloud_cover_pct'), '%')} clouds)",
            f"• Data Quality: {_quality_label(today.get('quality'))}",
            "",
            "➡️ Next",
            f"• {_next_action(decision)}",
            "• Use /status verbose for full diagnostics",
        ]
    )


async def build_today_text(verbose: bool = False) -> str:
    snapshot = await get_snapshot(days=7, use_cache=False, force_today_refresh=True)
    today = snapshot["today"]
    rec = snapshot["recommendation"]
    sunrise = _iso_to_local_display(today.get("sunrise"))
    sunset = _iso_to_local_display(today.get("sunset"))
    weather = today.get("weather", {})
    reason = (rec.get("reasons") or ["n/a"])[0]
    decision = rec.get("decision", "")
    baseline = rec.get("baseline_kwh")
    perf_ratio = _perf_ratio(today, baseline)
    detailed = _render_template(
        header=f"Solar Monitor - TODAY ({today['date']})",
        generated_at=snapshot["generated_at_local"],
        state=_severity_label(decision),
        recommendation=rec.get("summary", "n/a"),
        confidence=str(rec.get("confidence", "n/a")).upper(),
        why=reason,
        key_metrics=[
            f"Current/Peak: {_fmt(today.get('current_output_kw'), 'kW')} / {_fmt(today.get('peak_output_kw'), 'kW')}",
            f"Energy: {_fmt(today.get('energy_estimate_kwh'), 'kWh')} (Projected {_fmt(today.get('projected_energy_kwh'), 'kWh')})",
            f"Progress: {_pct(today.get('day_completion_ratio'))}",
            f"Performance Ratio: {_fmt_ratio(perf_ratio)}",
            f"Samples: total={today['samples_total']} daylight={today['samples_daylight']} nonzero={today.get('samples_daylight_non_zero', 0)}",
        ],
        context=[
            f"Sunrise/Sunset: {sunrise}/{sunset}",
            f"Weather: {_weather_class(weather.get('precipitation_mm'), weather.get('cloud_cover_pct'))} | rain {_fmt(weather.get('precipitation_mm'), 'mm')} | cloud {_fmt(weather.get('cloud_cover_pct'), '%')}",
            f"Data: {today.get('data_source', 'n/a')} | fetched {_short_ts(today.get('fetched_at'))} | quality {today.get('quality', 'n/a')}",
        ],
        next_now=_next_action(decision),
        next_later="Use /report for full consolidated view.",
        error_text=today.get("error"),
    )
    if verbose:
        return detailed

    confidence_inline = _confidence_inline(rec, today)
    return "\n".join(
        [
            f"☀️ Solar Monitor - Today ({today['date']})",
            "",
            f"{_emoji_for_state(_severity_label(decision))} {_summary_line(rec)} {confidence_inline}",
            "",
            "📊 Performance",
            f"• Energy: {_fmt(today.get('energy_estimate_kwh'), 'kWh')}",
            f"• Projected: {_fmt(today.get('projected_energy_kwh'), 'kWh')}",
            f"• Peak: {_fmt(today.get('peak_output_kw'), 'kW')}",
            f"• Progress: {_pct(today.get('day_completion_ratio'))}",
            "",
            "🌤 Context",
            f"• Weather: {_weather_class(weather.get('precipitation_mm'), weather.get('cloud_cover_pct'))}",
            f"• Cloud/Rain: {_fmt(weather.get('cloud_cover_pct'), '%')} / {_fmt(weather.get('precipitation_mm'), 'mm')}",
            "",
            "➡️ Next",
            f"• {_next_action(decision)}",
            "• Use /today verbose for full diagnostics",
        ]
    )


async def build_history_text(days: int = 7, verbose: bool = False) -> str:
    days = max(1, min(days, 30))
    snapshot = await get_snapshot(days=days, use_cache=True)
    rec = snapshot["recommendation"]
    history_days = snapshot["history"][:days]
    valid_days = [d for d in history_days if not d.get("error")]
    missing_days = [d for d in history_days if d.get("error")]
    avg_energy = None
    if valid_days:
        avg_energy = sum(float(d.get("energy_estimate_kwh") or 0.0) for d in valid_days) / len(valid_days)
    rows = []
    for day in history_days:
        if day.get("error"):
            rows.append(f"{day['date']}: no data ({day.get('data_source', 'n/a')})")
            continue

        rows.append(
            (
                f"{day['date']}: energy={_fmt(day.get('energy_estimate_kwh'), 'kWh')}, "
                f"peak={_fmt(day.get('peak_output_kw'), 'kW')}, "
                f"rain={_fmt(day.get('weather', {}).get('precipitation_mm'), 'mm')}, "
                f"cloud={_fmt(day.get('weather', {}).get('cloud_cover_pct'), '%')}, "
                f"src={day.get('data_source', 'n/a')}"
            )
        )

    baseline = rec.get("baseline_kwh")
    trend_line = _history_trend_line(history_days, baseline)
    reason = (rec.get("reasons") or ["n/a"])[0]
    missing_dates = ", ".join(item["date"] for item in missing_days[:5]) if missing_days else "none"
    detailed = _render_template(
        header=f"Solar Monitor - HISTORY ({days}d)",
        generated_at=snapshot["generated_at_local"],
        state=_severity_label(rec.get("decision", "")),
        recommendation=rec.get("summary", "n/a"),
        confidence=str(rec.get("confidence", "n/a")).upper(),
        why=reason,
        key_metrics=[
            f"Coverage: {len(valid_days)}/{days}",
            f"Average Energy: {_fmt(avg_energy, 'kWh')}",
            f"Missing Days: {len(missing_days)} ({missing_dates})",
        ],
        trend=[trend_line, *rows[: min(len(rows), 10)]],
        next_now=_next_action(rec.get("decision", "")),
        next_later="Use /quality to inspect data gaps and anomalies.",
    )
    if verbose:
        return detailed

    yesterday = history_days[1] if len(history_days) > 1 else None
    causal = _history_causal_insight(today=history_days[0] if history_days else None, yesterday=yesterday)
    deltas = _history_delta_lines(today=history_days[0] if history_days else None, yesterday=yesterday, baseline=baseline)
    table_block = _history_short_table(history_days[: min(days, 7)])
    current_dev = _safe_float(deltas.get("baseline_dev_pct"))
    in_range = current_dev is not None and abs(current_dev) <= 8.0
    confidence_inline = _confidence_inline(rec, history_days[0] if history_days else {})
    return "\n".join(
        [
            f"☀️ Solar Monitor - {days} Day Summary",
            "",
            f"{_emoji_for_state(_severity_label(rec.get('decision', '')))} {_summary_line(rec)} {confidence_inline}",
            "",
            "📊 Performance",
            f"• Avg Energy: {_fmt(avg_energy, 'kWh')}",
            f"• vs Yesterday: {deltas['vs_yesterday']}",
            f"• vs Baseline: {deltas['vs_baseline']}",
            f"• Coverage: {len(valid_days)}/{days}",
            "",
            "🧠 Insight",
            "No performance anomaly detected." if in_range else "Performance deviation detected; watch closely.",
            f"{causal}",
            "",
            "🛡 Expected Range",
            "• Normal variation: ±5-8%",
            f"• Current: {'within range' if in_range else 'outside range'}",
            "",
            "📋 Last 7 Days (Energy | Cloud)",
            table_block,
            "",
            "➡️ Next",
            "• No action required" if _severity_label(rec.get("decision", "")) == "OK" else f"• {_next_action(rec.get('decision', ''))}",
            "• Recheck if drop exceeds 10% without weather change",
            "• Use /history verbose for day-by-day rows",
        ]
    )


async def build_weather_text(verbose: bool = False) -> str:
    snapshot = await get_snapshot(days=7, use_cache=True)
    today = snapshot["today"]
    weather = today.get("weather", {})
    sunrise = _iso_to_local_display(today.get("sunrise"))
    sunset = _iso_to_local_display(today.get("sunset"))
    rain = weather.get("precipitation_mm")
    cloud = weather.get("cloud_cover_pct")
    weather_class = _weather_class(rain, cloud)
    detailed = _render_template(
        header=f"Solar Monitor - WEATHER ({today['date']})",
        generated_at=snapshot["generated_at_local"],
        state="OK",
        recommendation=f"Weather class: {weather_class}",
        confidence="INFO",
        why="Weather context is used to avoid false cleaning alerts on cloudy/rainy days.",
        key_metrics=[
            f"Rain: {_fmt(rain, 'mm')}",
            f"Cloud Cover: {_fmt(cloud, '%')}",
            f"Sunrise/Sunset: {sunrise}/{sunset}",
            f"Weather Code: {weather.get('weather_code', 'n/a')}",
        ],
        next_now="No immediate action needed from weather alone.",
        next_later="Correlate with /today and /history for cleaning decisions.",
    )
    if verbose:
        return detailed
    return "\n".join(
        [
            f"🌤 Weather ({today['date']})",
            f"• Class: {weather_class}",
            f"• Rain/Cloud: {_fmt(rain, 'mm')} / {_fmt(cloud, '%')}",
            f"• Sun: {sunrise}-{sunset}",
            "• Use /weather verbose for full weather diagnostics",
        ]
    )


async def build_quality_text(days: int = 7, verbose: bool = False) -> str:
    snapshot = await get_snapshot(days=days, use_cache=True)
    history = snapshot["history"][:days]
    missing = [d for d in history if d.get("error")]
    sparse = [d for d in history if d.get("quality") == "sparse"]
    early = [d for d in history if d.get("quality") == "early"]
    anomalies = [
        d
        for d in history
        if not d.get("error")
        and d.get("samples_daylight", 0) >= 12
        and d.get("samples_daylight_non_zero", 0) == 0
        and _as_float(d.get("weather", {}).get("cloud_cover_pct"), 0.0) < 45.0
        and _as_float(d.get("weather", {}).get("precipitation_mm"), 0.0) < 1.0
    ]
    quality_score = max(0, 100 - (len(missing) * 20) - (len(sparse) * 10) - (len(anomalies) * 20))

    missing_dates = ", ".join(item["date"] for item in missing[:5]) if missing else "none"
    sparse_dates = ", ".join(item["date"] for item in sparse[:5]) if sparse else "none"
    anomaly_dates = ", ".join(item["date"] for item in anomalies[:5]) if anomalies else "none"
    detailed = _render_template(
        header=f"Solar Monitor - QUALITY ({days}d)",
        generated_at=snapshot["generated_at_local"],
        state="WATCH" if quality_score < 80 else "OK",
        recommendation=f"Data quality score: {quality_score}/100",
        confidence="INFO",
        why="Score penalizes missing days, sparse samples, and clear-day zero-output anomalies.",
        key_metrics=[
            f"Missing Days: {len(missing)} ({missing_dates})",
            f"Sparse Days: {len(sparse)} ({sparse_dates})",
            f"Early/Partial Days: {len(early)}",
            f"Anomalies: {len(anomalies)} ({anomaly_dates})",
        ],
        next_now="Fix missing/anomalous data feed first if score is low.",
        next_later="Run /run and re-check /quality.",
    )
    if verbose:
        return detailed
    return "\n".join(
        [
            f"🧪 Quality ({days}d)",
            f"• Score: {quality_score}/100",
            f"• Missing: {len(missing)} | Sparse: {len(sparse)} | Anomalies: {len(anomalies)}",
            "• Use /quality verbose for anomaly dates and diagnostics",
        ]
    )


async def build_report_text(days: int = 7, force_refresh: bool = False) -> str:
    status = await build_status_text(verbose=False) if not force_refresh else await _build_status_uncached()
    history = await build_history_text(days=days, verbose=False) if not force_refresh else await _build_history_uncached(days=days)
    quality = await build_quality_text(days=days, verbose=False)
    return f"{status}\n\n{history}\n\n{quality}"


async def build_run_text() -> str:
    snapshot = await get_snapshot(days=7, use_cache=False)
    today = snapshot["today"]
    rec = snapshot["recommendation"]
    return _render_template(
        header="Solar Monitor - RUN",
        generated_at=snapshot["generated_at_local"],
        state=_severity_label(rec.get("decision", "")),
        recommendation=rec.get("summary", "n/a"),
        confidence=str(rec.get("confidence", "n/a")).upper(),
        why="Manual refresh requested.",
        key_metrics=[
            f"Current Output: {_fmt(today.get('current_output_kw'), 'kW')}",
            f"Energy Today: {_fmt(today.get('energy_estimate_kwh'), 'kWh')}",
            f"Data Source: {today.get('data_source', 'n/a')}",
        ],
        next_now=_next_action(rec.get("decision", "")),
        next_later="Use /report for full diagnostic.",
    )


async def build_alerts_text() -> str:
    settings = await get_alert_settings()
    return _render_template(
        header="Solar Monitor - ALERTS",
        generated_at=_now_local_display(),
        state="OK",
        recommendation="Active alert thresholds loaded.",
        confidence="INFO",
        why="These thresholds drive cleaning recommendation and alert cadence.",
        key_metrics=[
            f"ratio threshold: {settings['clean_ratio_threshold']:.2f} (alert when performance ratio is lower)",
            f"clear streak days: {int(settings['clear_streak_days'])}",
            f"rain postpone: {settings['rain_postpone_mm']:.1f} mm",
            f"cooldown: {int(settings['alert_cooldown_hours'])} h",
        ],
        next_now="Use /setalerts to update thresholds.",
        next_later="Re-run /status after changing alert settings.",
    )


async def _build_snapshot(days: int, force_today_refresh: bool = False) -> dict[str, Any]:
    tz_name = os.getenv("SOLAR_TIMEZONE", DEFAULT_TZ)
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz=tz)
    today = now_local.date()
    day_list = [today - timedelta(days=offset) for offset in range(days)]

    weather_map = await _fetch_weather_range(min(day_list), max(day_list), tz_name=tz_name)
    evals = await asyncio.gather(
        *[
            _build_day_evaluation(
                d,
                weather_map.get(d.isoformat()),
                force_refresh_today=force_today_refresh,
            )
            for d in day_list
        ]
    )
    evals_sorted = sorted(evals, key=lambda item: item.day, reverse=True)
    today_eval = next((item for item in evals_sorted if item.day == today), evals_sorted[0])
    upcoming_rain = await _fetch_upcoming_rain_mm(tz_name)
    alert_settings = await get_alert_settings()

    recommendation = _build_recommendation(
        today_eval=today_eval,
        history=evals_sorted,
        now_local=now_local,
        upcoming_rain_mm=upcoming_rain,
        alert_settings=alert_settings,
    )

    return {
        "generated_at": datetime.now(tz=ZoneInfo("UTC")).isoformat(),
        "generated_at_local": now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "timezone": tz_name,
        "alert_settings": alert_settings,
        "today": today_eval.to_dict(),
        "history": [item.to_dict() for item in evals_sorted],
        "recommendation": recommendation,
    }


async def _build_day_evaluation(
    day: date,
    weather: WeatherDay | None,
    force_refresh_today: bool = False,
) -> DayEvaluation:
    tz_name = os.getenv("SOLAR_TIMEZONE", DEFAULT_TZ)
    tz = ZoneInfo(tz_name)
    default_sunrise = datetime.combine(day, time(6, 0), tzinfo=tz)
    default_sunset = datetime.combine(day, time(18, 0), tzinfo=tz)
    sunrise = weather.sunrise if weather and weather.sunrise else default_sunrise
    sunset = weather.sunset if weather and weather.sunset else default_sunset

    try:
        response, data_source, fetched_at = await _get_or_fetch_day_response(
            day,
            force_refresh_today=force_refresh_today,
        )
        points = _parse_power_points(response)
        return _summarize_day(
            day,
            points,
            sunrise,
            sunset,
            weather,
            api_url=response.get("url"),
            data_source=data_source,
            fetched_at=fetched_at,
        )
    except Exception as exc:  # noqa: BLE001
        return DayEvaluation(
            day=day,
            samples_total=0,
            daylight_samples=0,
            non_zero_daylight_samples=0,
            energy_kwh=0.0,
            projected_energy_kwh=None,
            peak_kw=0.0,
            current_kw=None,
            latest_timestamp=None,
            daylight_hours=max((sunset - sunrise).total_seconds() / 3600.0, 0.1),
            completion_ratio=0.0,
            sunrise=sunrise,
            sunset=sunset,
            precipitation_mm=weather.precipitation_mm if weather else None,
            cloud_cover_pct=weather.cloud_cover_pct if weather else None,
            weather_code=weather.weather_code if weather else None,
            quality="missing",
            data_source="none",
            fetched_at=None,
            api_url=None,
            error=str(exc),
        )


def _parse_power_points(response: dict[str, Any]) -> list[tuple[datetime, float]]:
    payload = response.get("payload", {})
    if not isinstance(payload, dict):
        raise ValueError("Unexpected payload from ShineMonitor")

    err = payload.get("err")
    if err not in (None, 0, "0"):
        desc = payload.get("desc", "unknown")
        raise ValueError(f"ShineMonitor API returned err={err}, desc={desc}")

    items = payload.get("dat", {}).get("outputPower", [])
    if not isinstance(items, list):
        raise ValueError("Invalid outputPower format")

    tz = ZoneInfo(os.getenv("SOLAR_TIMEZONE", DEFAULT_TZ))
    dedup: dict[datetime, float] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        ts_raw = item.get("ts")
        val_raw = item.get("val")
        if not ts_raw:
            continue
        try:
            ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
            value = max(float(val_raw), 0.0)
        except (TypeError, ValueError):
            continue
        dedup[ts] = value

    return sorted(dedup.items(), key=lambda entry: entry[0])


def _summarize_day(
    day: date,
    points: list[tuple[datetime, float]],
    sunrise: datetime,
    sunset: datetime,
    weather: WeatherDay | None,
    api_url: str | None,
    data_source: str,
    fetched_at: str | None,
) -> DayEvaluation:
    tz = ZoneInfo(os.getenv("SOLAR_TIMEZONE", DEFAULT_TZ))
    now_local = datetime.now(tz=tz)

    daylight_hours = max((sunset - sunrise).total_seconds() / 3600.0, 0.1)
    elapsed_hours = _elapsed_daylight_hours(now_local, sunrise, sunset) if day == now_local.date() else daylight_hours
    completion_ratio = min(max(elapsed_hours / daylight_hours, 0.0), 1.0)

    daylight_points = [(ts, val) for ts, val in points if sunrise <= ts <= sunset]
    non_zero_daylight = sum(1 for _, val in daylight_points if val >= MIN_NON_ZERO_KW)
    energy_kwh = _integrate_energy_kwh(daylight_points)
    peak_kw = max((val for _, val in daylight_points), default=0.0)

    latest_timestamp: datetime | None = points[-1][0] if points else None
    current_kw: float | None = points[-1][1] if points else None

    projected: float | None = None
    if day == now_local.date() and completion_ratio >= 0.25:
        projected = energy_kwh / completion_ratio
    if day != now_local.date():
        projected = energy_kwh

    quality = "ok"
    if not points:
        quality = "missing"
    elif day == now_local.date() and completion_ratio < 0.2:
        quality = "early"
    elif len(daylight_points) < 12 and completion_ratio > 0.6:
        quality = "sparse"

    return DayEvaluation(
        day=day,
        samples_total=len(points),
        daylight_samples=len(daylight_points),
        non_zero_daylight_samples=non_zero_daylight,
        energy_kwh=energy_kwh,
        projected_energy_kwh=projected,
        peak_kw=peak_kw,
        current_kw=current_kw,
        latest_timestamp=latest_timestamp,
        daylight_hours=daylight_hours,
        completion_ratio=completion_ratio,
        sunrise=sunrise,
        sunset=sunset,
        precipitation_mm=weather.precipitation_mm if weather else None,
        cloud_cover_pct=weather.cloud_cover_pct if weather else None,
        weather_code=weather.weather_code if weather else None,
        quality=quality,
        data_source=data_source,
        fetched_at=fetched_at,
        api_url=api_url,
        error=None,
    )


async def _get_or_fetch_day_response(
    day: date,
    force_refresh_today: bool = False,
) -> tuple[dict[str, Any], str, str | None]:
    day_str = day.isoformat()
    record = await get_day_record(day_str)
    tz = ZoneInfo(os.getenv("SOLAR_TIMEZONE", DEFAULT_TZ))
    is_today = day == datetime.now(tz=tz).date()

    if is_today and force_refresh_today:
        try:
            fresh_response = await call_shinemonitor_api(date_override=day_str)
            await upsert_day_success(day_str, fresh_response)
            fresh_record = await get_day_record(day_str)
            return fresh_response, "api", fresh_record.get("fetched_at") if fresh_record else None
        except Exception as exc:  # noqa: BLE001
            await upsert_day_error(day_str, str(exc))
            if record and record.get("response_json"):
                try:
                    parsed = json.loads(record["response_json"])
                    return parsed, "db_stale", record.get("fetched_at")
                except json.JSONDecodeError:
                    pass
            raise

    if record and record.get("response_json") and not _should_refresh_record(day=day, fetched_at=record.get("fetched_at")):
        try:
            parsed = json.loads(record["response_json"])
            return parsed, "db", record.get("fetched_at")
        except json.JSONDecodeError:
            pass

    try:
        fresh_response = await call_shinemonitor_api(date_override=day_str)
        await upsert_day_success(day_str, fresh_response)
        fresh_record = await get_day_record(day_str)
        return fresh_response, "api", fresh_record.get("fetched_at") if fresh_record else None
    except Exception as exc:  # noqa: BLE001
        await upsert_day_error(day_str, str(exc))
        if record and record.get("response_json"):
            try:
                parsed = json.loads(record["response_json"])
                return parsed, "db_stale", record.get("fetched_at")
            except json.JSONDecodeError:
                pass
        raise


def _should_refresh_record(day: date, fetched_at: str | None) -> bool:
    tz = ZoneInfo(os.getenv("SOLAR_TIMEZONE", DEFAULT_TZ))
    today = datetime.now(tz=tz).date()
    if day != today:
        return False

    if not fetched_at:
        return True

    try:
        fetched_dt = datetime.fromisoformat(fetched_at)
    except ValueError:
        return True
    if fetched_dt.tzinfo is None:
        fetched_dt = fetched_dt.replace(tzinfo=ZoneInfo("UTC"))

    refresh_minutes = int(os.getenv("TODAY_REFRESH_MINUTES", "15"))
    age_seconds = (datetime.now(tz=ZoneInfo("UTC")) - fetched_dt.astimezone(ZoneInfo("UTC"))).total_seconds()
    return age_seconds >= max(refresh_minutes, 1) * 60


def _integrate_energy_kwh(points: list[tuple[datetime, float]]) -> float:
    if len(points) < 2:
        return 0.0

    energy = 0.0
    for (t1, v1), (t2, v2) in zip(points, points[1:]):
        delta_h = (t2 - t1).total_seconds() / 3600.0
        if delta_h <= 0 or delta_h > 0.5:
            continue
        energy += max((v1 + v2) / 2.0, 0.0) * delta_h
    return energy


def _elapsed_daylight_hours(now_local: datetime, sunrise: datetime, sunset: datetime) -> float:
    if now_local <= sunrise:
        return 0.0
    if now_local >= sunset:
        return max((sunset - sunrise).total_seconds() / 3600.0, 0.0)
    return max((now_local - sunrise).total_seconds() / 3600.0, 0.0)


async def _fetch_weather_range(start_day: date, end_day: date, tz_name: str) -> dict[str, WeatherDay]:
    lat_raw = os.getenv("SOLAR_LATITUDE", "").strip()
    lon_raw = os.getenv("SOLAR_LONGITUDE", "").strip()
    if not lat_raw or not lon_raw:
        return {}

    try:
        lat = float(lat_raw)
        lon = float(lon_raw)
    except ValueError:
        return {}

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "daily": "sunrise,sunset,precipitation_sum,weather_code,daylight_duration,sunshine_duration",
        "hourly": "cloud_cover",
        "timezone": tz_name,
    }

    timeout = float(os.getenv("WEATHER_API_TIMEOUT", "15"))
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(WEATHER_URL, params=params)
            response.raise_for_status()
            payload = response.json()
    except Exception:  # noqa: BLE001
        return {}

    daily = payload.get("daily", {})
    daily_dates = daily.get("time", [])
    sunrise_values = daily.get("sunrise", [])
    sunset_values = daily.get("sunset", [])
    rain_values = daily.get("precipitation_sum", [])
    weather_values = daily.get("weather_code", [])
    daylight_values = daily.get("daylight_duration", [])
    sunshine_values = daily.get("sunshine_duration", [])

    cloud_map = _build_hourly_cloud_map(payload)

    tz = ZoneInfo(tz_name)
    result: dict[str, WeatherDay] = {}
    for idx, day_value in enumerate(daily_dates):
        sunrise = _parse_iso_dt(_safe_index(sunrise_values, idx), tz)
        sunset = _parse_iso_dt(_safe_index(sunset_values, idx), tz)
        rain = _safe_float(_safe_index(rain_values, idx))
        code = _safe_int(_safe_index(weather_values, idx))
        daylight_h = _seconds_to_hours(_safe_float(_safe_index(daylight_values, idx)))
        sunshine_h = _seconds_to_hours(_safe_float(_safe_index(sunshine_values, idx)))

        result[day_value] = WeatherDay(
            sunrise=sunrise,
            sunset=sunset,
            precipitation_mm=rain,
            cloud_cover_pct=cloud_map.get(day_value),
            weather_code=code,
            daylight_hours=daylight_h,
            sunshine_hours=sunshine_h,
        )

    return result


async def _fetch_upcoming_rain_mm(tz_name: str) -> float | None:
    lat_raw = os.getenv("SOLAR_LATITUDE", "").strip()
    lon_raw = os.getenv("SOLAR_LONGITUDE", "").strip()
    if not lat_raw or not lon_raw:
        return None

    try:
        lat = float(lat_raw)
        lon = float(lon_raw)
    except ValueError:
        return None

    tz = ZoneInfo(tz_name)
    today = datetime.now(tz=tz).date()
    end_day = today + timedelta(days=2)

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": today.isoformat(),
        "end_date": end_day.isoformat(),
        "daily": "precipitation_sum",
        "timezone": tz_name,
    }

    timeout = float(os.getenv("WEATHER_API_TIMEOUT", "15"))
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(WEATHER_URL, params=params)
            response.raise_for_status()
            payload = response.json()
    except Exception:  # noqa: BLE001
        return None

    rain_values = payload.get("daily", {}).get("precipitation_sum", [])
    cleaned = [value for value in (_safe_float(item) for item in rain_values) if value is not None]
    if not cleaned:
        return None
    return round(sum(cleaned), 3)


def _build_hourly_cloud_map(payload: dict[str, Any]) -> dict[str, float]:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    values = hourly.get("cloud_cover", [])

    per_day: dict[str, list[float]] = {}
    for ts, val in zip(times, values):
        day = str(ts)[:10]
        num = _safe_float(val)
        if num is None:
            continue
        per_day.setdefault(day, []).append(num)

    return {
        day: round(sum(day_values) / len(day_values), 2)
        for day, day_values in per_day.items()
        if day_values
    }


def _build_recommendation(
    today_eval: DayEvaluation,
    history: list[DayEvaluation],
    now_local: datetime,
    upcoming_rain_mm: float | None,
    alert_settings: dict[str, float],
) -> dict[str, Any]:
    reasons: list[str] = []

    historical_complete = [
        item
        for item in history
        if item.day < today_eval.day and item.error is None and item.quality in {"ok", "sparse"}
    ]

    clear_days = [item for item in historical_complete if _is_clear_day(item)]
    baseline_pool = [item.energy_kwh for item in clear_days if item.energy_kwh > 0]
    if len(baseline_pool) < 2:
        baseline_pool = [item.energy_kwh for item in historical_complete if item.energy_kwh > 0]

    baseline_kwh = statistics.median(baseline_pool) if baseline_pool else None

    compare_energy = (
        today_eval.projected_energy_kwh
        if today_eval.projected_energy_kwh is not None
        else today_eval.energy_kwh
    )

    performance_ratio: float | None = None
    if baseline_kwh and baseline_kwh > 0:
        performance_ratio = compare_energy / baseline_kwh
        reasons.append(
            f"Performance ratio vs recent baseline: {performance_ratio:.2f} (baseline {baseline_kwh:.2f} kWh)."
        )
    else:
        reasons.append("Not enough historical baseline to compare performance confidently.")

    if today_eval.error:
        return {
            "decision": "insufficient_data",
            "confidence": "low",
            "summary": "Cannot evaluate cleaning today because generation data is unavailable.",
            "reasons": [today_eval.error],
            "baseline_kwh": baseline_kwh,
            "upcoming_rain_mm": upcoming_rain_mm,
        }

    if today_eval.quality == "early":
        return {
            "decision": "wait_more_daylight",
            "confidence": "low",
            "summary": "Too early in the day to make a reliable cleaning decision.",
            "reasons": [
                "Daylight progress is low; wait until more daytime generation is available.",
                *reasons,
            ],
            "baseline_kwh": baseline_kwh,
            "upcoming_rain_mm": upcoming_rain_mm,
        }

    if today_eval.daylight_samples >= 12 and today_eval.non_zero_daylight_samples == 0:
        if _is_clear_day(today_eval):
            return {
                "decision": "check_system",
                "confidence": "high",
                "summary": "Daylight output is near zero on a clear day; likely system fault before cleaning.",
                "reasons": [
                    "Clear weather but no meaningful daylight generation detected.",
                    "Check inverter, breaker, and connectivity before scheduling cleaning.",
                ],
                "baseline_kwh": baseline_kwh,
                "upcoming_rain_mm": upcoming_rain_mm,
            }
        return {
            "decision": "weather_limited",
            "confidence": "medium",
            "summary": "Very low generation, but weather likely explains it today.",
            "reasons": [
                "Daylight generation was near zero, but weather conditions are not clear.",
                *reasons,
            ],
            "baseline_kwh": baseline_kwh,
            "upcoming_rain_mm": upcoming_rain_mm,
        }

    if _is_bad_weather(today_eval):
        return {
            "decision": "no_cleaning_weather",
            "confidence": "medium",
            "summary": "No cleaning suggestion today because weather conditions can naturally reduce output.",
            "reasons": [
                "Cloud/rain conditions are significant, so output drop may be weather-driven.",
                *reasons,
            ],
            "baseline_kwh": baseline_kwh,
            "upcoming_rain_mm": upcoming_rain_mm,
        }

    ratio_threshold = alert_settings["clean_ratio_threshold"]
    streak_needed = int(alert_settings["clear_streak_days"])
    rain_postpone_mm = alert_settings["rain_postpone_mm"]
    low_performance = performance_ratio is not None and performance_ratio < ratio_threshold
    clear_today = _is_clear_day(today_eval)
    low_streak = _count_low_clear_streak(history=history, baseline_kwh=baseline_kwh)

    if low_performance and clear_today and low_streak >= streak_needed:
        if upcoming_rain_mm is not None and upcoming_rain_mm >= rain_postpone_mm:
            return {
                "decision": "postpone_cleaning_rain_expected",
                "confidence": "medium",
                "summary": "Output is low on clear days, but rain is expected soon, so recheck after rain.",
                "reasons": [
                    f"Clear-day underperformance streak: {low_streak} days.",
                    f"Expected precipitation next ~48h: {upcoming_rain_mm:.1f} mm.",
                    *reasons,
                ],
                "baseline_kwh": baseline_kwh,
                "upcoming_rain_mm": upcoming_rain_mm,
            }

        return {
            "decision": "cleaning_recommended",
            "confidence": "high",
            "summary": "Cleaning is recommended: output is consistently low on clear-weather days.",
            "reasons": [
                f"Clear-day underperformance streak: {low_streak} days.",
                *reasons,
            ],
            "baseline_kwh": baseline_kwh,
            "upcoming_rain_mm": upcoming_rain_mm,
        }

    if low_performance and clear_today:
        return {
            "decision": "monitor_close",
            "confidence": "medium",
            "summary": "Possible soiling signal, but wait for one more clear day confirmation.",
            "reasons": [
                "Underperformance detected on a clear day but not enough streak yet.",
                *reasons,
            ],
            "baseline_kwh": baseline_kwh,
            "upcoming_rain_mm": upcoming_rain_mm,
        }

    daytime_state = "after_sunset" if now_local >= today_eval.sunset else "day_in_progress"
    return {
        "decision": "no_cleaning_needed",
        "confidence": "medium",
        "summary": "No cleaning required based on current output trend and weather context.",
        "reasons": [
            f"State: {daytime_state}.",
            *reasons,
        ],
        "baseline_kwh": baseline_kwh,
        "upcoming_rain_mm": upcoming_rain_mm,
    }


async def _build_status_uncached() -> str:
    snapshot = await get_snapshot(days=7, use_cache=False)
    today = snapshot["today"]
    rec = snapshot["recommendation"]
    return "\n".join(
        [
            "Solar Monitor - Status (fresh)",
            f"Generated at: {snapshot['generated_at_local']}",
            f"Current Output: {_fmt(today.get('current_output_kw'), 'kW')}",
            f"Energy Today (est): {_fmt(today.get('energy_estimate_kwh'), 'kWh')}",
            f"Decision: {rec.get('summary', 'n/a')}",
        ]
    )


async def _build_history_uncached(days: int) -> str:
    snapshot = await get_snapshot(days=days, use_cache=False)
    rec = snapshot["recommendation"]
    return "\n".join(
        [
            f"Solar Monitor - History ({days}d, fresh)",
            f"Decision: {rec.get('summary', 'n/a')}",
            f"Coverage: {len([d for d in snapshot['history'][:days] if not d.get('error')])}/{days}",
        ]
    )


def _count_low_clear_streak(history: list[DayEvaluation], baseline_kwh: float | None) -> int:
    if not baseline_kwh or baseline_kwh <= 0:
        return 0

    streak = 0
    for item in history:
        if item.error or not _is_clear_day(item):
            break

        compare_value = item.projected_energy_kwh if item.projected_energy_kwh is not None else item.energy_kwh
        ratio = compare_value / baseline_kwh if baseline_kwh > 0 else 1.0
        if ratio < 0.7:
            streak += 1
            continue
        break

    return streak


def _is_clear_day(day_eval: DayEvaluation) -> bool:
    if day_eval.precipitation_mm is not None and day_eval.precipitation_mm >= 1.0:
        return False
    if day_eval.cloud_cover_pct is not None and day_eval.cloud_cover_pct >= 45.0:
        return False
    return True


def _is_bad_weather(day_eval: DayEvaluation) -> bool:
    if day_eval.precipitation_mm is not None and day_eval.precipitation_mm >= 2.0:
        return True
    if day_eval.cloud_cover_pct is not None and day_eval.cloud_cover_pct >= 65.0:
        return True
    return False


def _parse_iso_dt(value: Any, tz: ZoneInfo) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def _safe_index(values: list[Any], idx: int) -> Any:
    if idx < 0 or idx >= len(values):
        return None
    return values[idx]


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _seconds_to_hours(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value / 3600.0, 3)


def _fmt(value: Any, unit: str) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, (int, float)):
        return f"{value:.2f} {unit}".strip()
    return f"{value} {unit}".strip()


def _iso_to_local_display(value: str | None) -> str:
    if not value:
        return "n/a"
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.strftime("%H:%M")
    except ValueError:
        return value


def _short_ts(value: str | None) -> str:
    if not value:
        return "n/a"
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value


def _next_action(decision: str) -> str:
    action_map = {
        "cleaning_recommended": "Schedule panel cleaning in next 24-48h.",
        "postpone_cleaning_rain_expected": "Wait for forecast rain, then re-check.",
        "monitor_close": "Monitor next clear day before cleaning.",
        "no_cleaning_needed": "No action needed; continue monitoring.",
        "no_cleaning_weather": "Wait for clearer weather and reassess.",
        "check_system": "Inspect inverter/breaker/connectivity first.",
        "weather_limited": "Treat as weather-affected day.",
        "wait_more_daylight": "Re-run after more daytime generation.",
        "insufficient_data": "Check API/data feed health.",
    }
    return action_map.get(decision, "Monitor and recheck soon.")


def _as_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _weather_class(rain_mm: float | None, cloud_pct: float | None) -> str:
    rain = _as_float(rain_mm, 0.0)
    cloud = _as_float(cloud_pct, 0.0)
    if rain >= 2.0:
        return "rainy"
    if cloud >= 70:
        return "very_cloudy"
    if cloud >= 45:
        return "cloudy"
    return "clear"


def _severity_label(decision: str) -> str:
    mapping = {
        "cleaning_recommended": "ACTION",
        "check_system": "ERROR",
        "insufficient_data": "ERROR",
        "monitor_close": "WATCH",
        "postpone_cleaning_rain_expected": "WATCH",
        "wait_more_daylight": "WATCH",
        "weather_limited": "WATCH",
        "no_cleaning_weather": "OK",
        "no_cleaning_needed": "OK",
    }
    return mapping.get(decision, "WATCH")


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _fmt_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}x"


def _perf_ratio(today: dict[str, Any], baseline: Any) -> float | None:
    baseline_num = _safe_float(baseline)
    if baseline_num is None or baseline_num <= 0:
        return None
    compare = _safe_float(today.get("projected_energy_kwh"))
    if compare is None:
        compare = _safe_float(today.get("energy_estimate_kwh"))
    if compare is None:
        return None
    return compare / baseline_num


def _history_trend_line(history_days: list[dict[str, Any]], baseline: Any) -> str:
    if not history_days:
        return "Trend: n/a"
    today = history_days[0]
    yesterday = history_days[1] if len(history_days) > 1 else None
    today_energy = _safe_float(today.get("energy_estimate_kwh"))
    y_energy = _safe_float(yesterday.get("energy_estimate_kwh")) if yesterday else None
    baseline_num = _safe_float(baseline)

    vs_y = "n/a"
    if today_energy is not None and y_energy is not None and y_energy > 0:
        delta = ((today_energy - y_energy) / y_energy) * 100
        arrow = "↑" if delta > 2 else "↓" if delta < -2 else "→"
        vs_y = f"{arrow} {delta:+.1f}%"

    vs_b = "n/a"
    if today_energy is not None and baseline_num is not None and baseline_num > 0:
        delta_b = ((today_energy - baseline_num) / baseline_num) * 100
        arrow_b = "↑" if delta_b > 2 else "↓" if delta_b < -2 else "→"
        vs_b = f"{arrow_b} {delta_b:+.1f}%"

    return f"Trend: vs yesterday {vs_y}, vs baseline {vs_b}"


def _now_local_display() -> str:
    tz_name = os.getenv("SOLAR_TIMEZONE", DEFAULT_TZ)
    tz = ZoneInfo(tz_name)
    return datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S %Z")


def _render_template(
    header: str,
    generated_at: str,
    state: str,
    recommendation: str,
    confidence: str,
    why: str,
    key_metrics: list[str],
    next_now: str,
    next_later: str,
    context: list[str] | None = None,
    trend: list[str] | None = None,
    error_text: str | None = None,
) -> str:
    lines = [
        header,
        "",
        "[DECISION]",
        f"State: {state}",
        f"Recommendation: {recommendation}",
        f"Confidence: {confidence}",
        f"Why: {why}",
        f"Generated: {generated_at}",
        "",
        "[KEY METRICS]",
    ]
    for item in key_metrics:
        lines.append(f"- {item}")

    if context:
        lines.extend(["", "[CONTEXT]"])
        for item in context:
            lines.append(f"- {item}")

    if trend:
        lines.extend(["", "[TREND]"])
        for item in trend:
            lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "[NEXT ACTION]",
            f"- Now: {next_now}",
            f"- Later: {next_later}",
        ]
    )

    if error_text:
        lines.extend(["", "[ERROR]", f"- {error_text}"])

    return "\n".join(lines)


def _emoji_for_state(state: str) -> str:
    mapping = {"OK": "✅", "WATCH": "⚠️", "ACTION": "🧽", "ERROR": "❌"}
    return mapping.get(state, "ℹ️")


def _summary_line(recommendation: dict[str, Any]) -> str:
    decision = str(recommendation.get("decision", ""))
    mapping = {
        "no_cleaning_needed": "System OK - No cleaning needed",
        "no_cleaning_weather": "System OK - No cleaning needed",
        "monitor_close": "Monitor - Possible early soiling signal",
        "postpone_cleaning_rain_expected": "Wait - Rain may clean naturally",
        "cleaning_recommended": "Action - Cleaning recommended",
        "check_system": "Issue - Check inverter/system first",
        "insufficient_data": "Issue - Data unavailable",
    }
    return mapping.get(decision, recommendation.get("summary", "Status updated"))


def _confidence_note(recommendation: dict[str, Any], today: dict[str, Any]) -> str:
    conf = str(recommendation.get("confidence", "n/a")).lower()
    progress = _safe_float(today.get("day_completion_ratio"))
    if conf == "high":
        return "Confidence note: strong signal across trend and context."
    if progress is not None and progress < 0.95:
        return "Confidence note: medium because the day is still in progress."
    return "Confidence note: medium due to normal weather/output variability."


def _confidence_inline(recommendation: dict[str, Any], today: dict[str, Any]) -> str:
    conf = str(recommendation.get("confidence", "n/a")).capitalize()
    conf_note = _confidence_note(recommendation, today).replace("Confidence note: ", "")
    return f"(Confidence: {conf} - {conf_note})"


def _quality_label(quality: Any) -> str:
    q = str(quality or "unknown").lower()
    mapping = {"ok": "Good", "sparse": "Limited", "early": "Partial", "missing": "Poor"}
    return mapping.get(q, q.capitalize())


def _history_causal_insight(today: dict[str, Any] | None, yesterday: dict[str, Any] | None) -> str:
    if not today:
        return "Insufficient data for causal insight."
    if not yesterday:
        return "Need one more day to build a comparison insight."

    t_energy = _safe_float(today.get("energy_estimate_kwh"))
    y_energy = _safe_float(yesterday.get("energy_estimate_kwh"))
    t_cloud = _safe_float(today.get("weather", {}).get("cloud_cover_pct"))
    y_cloud = _safe_float(yesterday.get("weather", {}).get("cloud_cover_pct"))

    if t_energy is None or y_energy is None or y_energy <= 0:
        return "Not enough energy history to infer cause."

    delta_e = ((t_energy - y_energy) / y_energy) * 100
    if t_cloud is not None and y_cloud is not None and t_cloud > y_cloud + 8:
        return (
            f"Energy dip {delta_e:+.1f}% aligns with higher cloud cover "
            f"({t_cloud:.0f}% vs {y_cloud:.0f}%). Likely weather-driven."
        )
    if abs(delta_e) < 6:
        return "Output variation is within normal day-to-day range."
    return "Output moved materially; review /quality for non-weather anomalies."


def _history_short_table(days: list[dict[str, Any]]) -> str:
    rows: list[str] = []
    for item in days:
        day = str(item.get("date", "n/a"))[:10]
        day_label = _date_short(day)
        if item.get("error"):
            rows.append(f"{day_label}: n/a | n/a")
            continue
        energy = _fmt_short_num(item.get("energy_estimate_kwh"), 1)
        cloud = _fmt_short_num(item.get("weather", {}).get("cloud_cover_pct"), 0)
        rows.append(f"{day_label}: {energy} kWh | {cloud}%")
    return "\n".join("• " + row for row in rows)


def _num(value: Any, decimals: int, width: int) -> str:
    num = _safe_float(value)
    if num is None:
        return "n/a".rjust(width)
    return f"{num:.{decimals}f}".rjust(width)


def _fmt_short_num(value: Any, decimals: int) -> str:
    num = _safe_float(value)
    if num is None:
        return "n/a"
    return f"{num:.{decimals}f}"


def _date_short(day: str) -> str:
    try:
        parsed = datetime.fromisoformat(day)
        return parsed.strftime("%d %b")
    except ValueError:
        return day


def _history_delta_lines(today: dict[str, Any] | None, yesterday: dict[str, Any] | None, baseline: Any) -> dict[str, str]:
    result = {"vs_yesterday": "n/a", "vs_baseline": "n/a", "baseline_dev_pct": None}
    if not today:
        return result

    t_energy = _safe_float(today.get("energy_estimate_kwh"))
    y_energy = _safe_float(yesterday.get("energy_estimate_kwh")) if yesterday else None
    b_energy = _safe_float(baseline)

    if t_energy is not None and y_energy is not None and y_energy > 0:
        delta = ((t_energy - y_energy) / y_energy) * 100
        result["vs_yesterday"] = _delta_str(delta)
    if t_energy is not None and b_energy is not None and b_energy > 0:
        delta_b = ((t_energy - b_energy) / b_energy) * 100
        result["vs_baseline"] = _delta_str(delta_b)
        result["baseline_dev_pct"] = delta_b
    return result


def _delta_str(delta: float) -> str:
    arrow = "↓" if delta < -0.1 else "↑" if delta > 0.1 else "→"
    return f"{arrow} {abs(delta):.1f}%"
