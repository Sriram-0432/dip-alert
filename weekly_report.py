# -*- coding: utf-8 -*-
"""
Weekly Market Report — every Monday 09:00 IST
===============================================
Sends a Telegram digest covering:
  • Last week's NAV performance for all 3 funds
  • Nifty 50 weekly return
  • India VIX (current + weekly range)
  • Current MDD from 52-week high for each fund
  • Simple signal summary: improving / holding / worsening

Uses the same TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID secrets as dip_alert.py.
Run via GitHub Actions cron: "30 3 * * 1"  (03:30 UTC = 09:00 IST, Mondays)
"""

import os
import sys
import io
import sqlite3
import logging
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd
import yfinance as yf

# ── UTF-8 FIX ─────────────────────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FUNDS = {
    "122639": {"name": "PPFAS Flexi Cap Direct",       "emoji": "🟦"},
    "120716": {"name": "UTI Nifty 50 Index Direct",     "emoji": "🟧"},
    "127042": {"name": "Motilal Oswal Midcap Direct",   "emoji": "🟩"},
}

NIFTY50   = "^NSEI"
INDIA_VIX = "^INDIAVIX"
WATERMARK_DB = "watermarks.db"

# ── IST ───────────────────────────────────────────────────────────────────────
def ist_now():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def week_range_label():
    """Returns 'DD Mon – DD Mon YYYY' for the just-completed Mon–Fri week."""
    today    = ist_now()
    last_fri = today - timedelta(days=today.weekday() + 3)   # last Friday
    last_mon = last_fri - timedelta(days=4)                   # last Monday
    if last_mon.year == last_fri.year:
        return f"{last_mon.strftime('%d %b')} – {last_fri.strftime('%d %b %Y')}"
    return f"{last_mon.strftime('%d %b %Y')} – {last_fri.strftime('%d %b %Y')}"

# ── HELPERS ───────────────────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Weekly report sent")
        return True
    except Exception as exc:
        log.error(f"Telegram failed: {exc}")
        return False


def fetch_nav_history(scheme_code: str) -> pd.Series:
    """Returns date-indexed float Series from mfapi."""
    url  = f"https://api.mfapi.in/mf/{scheme_code}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    records = resp.json().get("data", [])
    parsed  = {}
    for entry in records:
        try:
            d   = datetime.strptime(entry["date"], "%d-%m-%Y").date()
            nav = float(entry["nav"])
            parsed[d] = nav
        except (KeyError, ValueError):
            continue
    return pd.Series(parsed).sort_index()


def get_stored_peak(scheme_code: str):
    """Pull watermark from SQLite if it exists."""
    if not os.path.exists(WATERMARK_DB):
        return None, None
    con = sqlite3.connect(WATERMARK_DB)
    row = con.execute(
        "SELECT peak_nav, peak_date FROM watermarks WHERE scheme_code = ?",
        (scheme_code,),
    ).fetchone()
    con.close()
    return (row[0], row[1]) if row else (None, None)


def weekly_return(series: pd.Series, n_days: int = 7) -> float | None:
    """% change between the NAV from ~n_days ago and the latest."""
    if len(series) < 2:
        return None
    cutoff  = series.index[-1] - timedelta(days=n_days)
    week    = series[series.index >= cutoff]
    if len(week) < 2:
        return None
    return ((week.iloc[-1] - week.iloc[0]) / week.iloc[0]) * 100


def arrow(val: float | None) -> str:
    if val is None:
        return "–"
    return "🟢" if val >= 0 else "🔴"


def fmt_pct(val: float | None) -> str:
    if val is None:
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"


def mdd_signal(mdd_pct: float) -> str:
    if mdd_pct >= 20: return "🚨 Aggressive Buy zone"
    if mdd_pct >= 12: return "💥 Strong Buy zone"
    if mdd_pct >= 8:  return "✅ Buy zone"
    if mdd_pct >= 5:  return "📉 Accumulate zone"
    return "⏳ Wait — near peak"


# ── MAIN ──────────────────────────────────────────────────────────────────────
def build_report() -> str:
    sep  = "━" * 30
    week = week_range_label()
    lines = [
        f"📊 *WEEKLY MARKET REPORT*",
        f"🗓 Week: {week}",
        sep,
    ]

    # ── Nifty 50 ──────────────────────────────────────────────────────────────
    try:
        nifty_df  = yf.download(NIFTY50, period="10d", progress=False)["Close"].squeeze()
        nifty_wk  = weekly_return(nifty_df)
        nifty_cur = float(nifty_df.iloc[-1])
        nifty_52h = float(yf.download(NIFTY50, period="1y", progress=False)["Close"].squeeze().max())
        nifty_mdd = ((nifty_52h - nifty_cur) / nifty_52h) * 100
        lines += [
            f"*🏦 Nifty 50 Index*",
            f"Weekly:   {arrow(nifty_wk)} `{fmt_pct(nifty_wk)}`",
            f"Current:  `{nifty_cur:,.2f}`",
            f"52-Wk DD: `{nifty_mdd:.2f}%` {mdd_signal(nifty_mdd)}",
        ]
    except Exception as exc:
        log.warning(f"Nifty fetch failed: {exc}")
        lines.append("🏦 Nifty 50: data unavailable")

    # ── India VIX ─────────────────────────────────────────────────────────────
    try:
        vix_df  = yf.download(INDIA_VIX, period="10d", progress=False)["Close"].squeeze()
        vix_cur = float(vix_df.iloc[-1])
        vix_wk  = weekly_return(vix_df)
        vix_hi  = float(vix_df.iloc[-7:].max()) if len(vix_df) >= 7 else vix_cur
        vix_lo  = float(vix_df.iloc[-7:].min()) if len(vix_df) >= 7 else vix_cur
        vix_tag = "⚠️ Elevated" if vix_cur >= 18 else ("😰 Very High" if vix_cur >= 25 else "😌 Calm")
        lines += [
            "",
            f"*📈 India VIX*",
            f"Current:  `{vix_cur:.2f}` {vix_tag}",
            f"Wk Range: `{vix_lo:.2f}` – `{vix_hi:.2f}`  ({arrow(vix_wk)} `{fmt_pct(vix_wk)}`)",
        ]
    except Exception as exc:
        log.warning(f"VIX fetch failed: {exc}")
        lines.append("📈 India VIX: data unavailable")

    lines.append(sep)
    lines.append("*📂 YOUR FUNDS — LAST WEEK*")

    # ── Per-fund block ────────────────────────────────────────────────────────
    for code, cfg in FUNDS.items():
        try:
            history  = fetch_nav_history(code)
            curr_nav = float(history.iloc[-1])
            wk_ret   = weekly_return(history)

            # 52-week MDD from SQLite watermark (fallback: compute from history)
            peak_nav, peak_date = get_stored_peak(code)
            if peak_nav is None:
                window   = history.iloc[-252:]
                peak_nav = float(window.max())
                peak_date = str(window.idxmax())

            mdd_pct  = ((peak_nav - curr_nav) / peak_nav) * 100

            # Week-start NAV for context
            cutoff    = history.index[-1] - timedelta(days=7)
            wk_start  = float(history[history.index >= cutoff].iloc[0]) if len(history[history.index >= cutoff]) else curr_nav

            lines += [
                "",
                f"*{cfg['emoji']} {cfg['name']}*",
                f"NAV:       ₹`{curr_nav:.4f}`",
                f"Wk Start:  ₹`{wk_start:.4f}`",
                f"Wk Return: {arrow(wk_ret)} `{fmt_pct(wk_ret)}`",
                f"52-Wk DD:  `{mdd_pct:.2f}%` {mdd_signal(mdd_pct)}",
            ]
        except Exception as exc:
            log.warning(f"Fund {code} failed: {exc}")
            lines.append(f"{cfg['emoji']} {cfg['name']}: data unavailable")

    lines += [
        sep,
        "_Next dip alert runs daily at 08:00 IST_",
        f"_Report generated: {ist_now().strftime('%d %b %Y %H:%M IST')}_",
    ]

    return "\n".join(lines)


def main():
    log.info("=== Weekly Report — Started ===")
    report = build_report()
    log.info("Report built, sending to Telegram...")
    send_telegram(report)
    log.info("=== Weekly Report — Done ===")


if __name__ == "__main__":
    main()
