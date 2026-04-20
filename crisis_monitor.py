# -*- coding: utf-8 -*-
"""
Global Crisis Monitor — powered by Claude + web search
========================================================
Runs twice daily (08:00 IST and 20:00 IST) via GitHub Actions.

How it works:
  1. Calls the Anthropic API with the web_search tool enabled
  2. Asks Claude to scan live news for global events that could
     significantly impact Indian markets (wars, crashes, sanctions,
     rate surprises, pandemics, geopolitical shocks, etc.)
  3. Claude rates the risk: HIGH / MEDIUM / LOW / NONE
  4. HIGH  → immediate Telegram alert with full analysis
     MEDIUM → Telegram alert tagged as "Watch" (monitor, not panic)
     LOW/NONE → logged only, no Telegram noise

Requires one extra GitHub secret:
  ANTHROPIC_API_KEY   →  from console.anthropic.com

All other secrets (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) are shared
with dip_alert.py — no new Telegram setup needed.
"""

import os
import sys
import io
import json
import logging
import hashlib
from datetime import datetime, timezone, timedelta

import requests

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
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")

CRISIS_LOG = "crisis_log.csv"

# Only alert once per unique event within this many hours
DEDUP_HOURS = 12

# ── IST ───────────────────────────────────────────────────────────────────────
def ist_now():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def today_str():
    return ist_now().strftime("%Y-%m-%d %H:%M")

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
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
        log.info("Telegram alert sent")
        return True
    except Exception as exc:
        log.error(f"Telegram failed: {exc}")
        return False

# ── DEDUP ─────────────────────────────────────────────────────────────────────
def event_hash(summary: str) -> str:
    """Stable hash from the first 120 chars of the summary."""
    return hashlib.md5(summary[:120].encode()).hexdigest()

def already_alerted(h: str) -> bool:
    if not os.path.exists(CRISIS_LOG):
        return False
    try:
        import pandas as pd
        df = pd.read_csv(CRISIS_LOG)
        if "hash" not in df.columns or "timestamp" not in df.columns:
            return False
        match = df[df["hash"] == h]
        if match.empty:
            return False
        # Check if most recent occurrence is within DEDUP_HOURS
        latest = pd.to_datetime(match["timestamp"]).max()
        return (ist_now().replace(tzinfo=None) - latest.to_pydatetime()) \
               < timedelta(hours=DEDUP_HOURS)
    except Exception:
        return False

def log_crisis(row: dict):
    import pandas as pd
    pd.DataFrame([row]).to_csv(
        CRISIS_LOG, mode="a",
        header=not os.path.exists(CRISIS_LOG),
        index=False,
    )

# ── CLAUDE API CALL ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a financial risk analyst specialising in Indian equity markets.
Your job is to scan current global news and identify any events in the last 24–48 hours
that could materially impact Indian stock markets (Nifty 50, Sensex) or Indian mutual funds.

Events to watch for:
- Wars, military escalations, geopolitical conflicts (especially involving major economies)
- Global market crashes or circuit breakers triggered (US, China, Europe, Japan)
- Emergency central bank actions (surprise rate cuts/hikes by Fed, ECB, RBI, etc.)
- Major sanctions, trade wars, oil price shocks, commodity crises
- Pandemics, health emergencies declared by WHO
- Banking collapses or systemic financial crises (Lehman-type events)
- Political shocks in G20 nations (coups, sudden elections, defaults)
- Natural disasters affecting global supply chains

You MUST respond with ONLY a valid JSON object — no preamble, no markdown fences.
Schema:
{
  "risk_level": "HIGH" | "MEDIUM" | "LOW" | "NONE",
  "headline": "One-line summary of the main risk event (max 100 chars)",
  "events": [
    {
      "title": "Event name",
      "impact": "How this affects Indian markets specifically",
      "urgency": "immediate" | "this_week" | "monitor"
    }
  ],
  "india_impact": "2–3 sentence assessment of likely impact on Indian equity/MF",
  "suggested_action": "What an Indian retail MF investor should do/watch",
  "sources_checked": ["list of news sources or regions you scanned"]
}

If nothing significant: set risk_level to "NONE", headline to "No major risk events detected",
and leave events as an empty array.
"""

USER_PROMPT = """Search for major global news from the last 48 hours that could 
significantly affect Indian stock markets. Focus on wars, geopolitical crises, 
major market crashes globally, central bank emergency actions, pandemics, or 
any systemic financial risks. Be thorough — check US, Europe, Middle East, China, 
and South/Southeast Asia news. Return your analysis as JSON only."""

def call_claude_with_search() -> dict | None:
    """
    Calls claude-sonnet-4-20250514 with web_search enabled.
    Returns parsed JSON dict or None on failure.
    """
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set — cannot run crisis monitor")
        return None

    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1500,
        "system": SYSTEM_PROMPT,
        "tools": [
            {
                "type": "web_search_20250305",
                "name": "web_search"
            }
        ],
        "messages": [
            {"role": "user", "content": USER_PROMPT}
        ],
    }

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type":         "application/json",
                "x-api-key":            ANTHROPIC_API_KEY,
                "anthropic-version":    "2023-06-01",
                "anthropic-beta":       "web-search-2025-03-05",
            },
            json=payload,
            timeout=90,   # web search calls can take time
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error(f"Anthropic API call failed: {exc}")
        return None

    # Extract the final text block (after tool_use/tool_result rounds)
    text_blocks = [
        block["text"]
        for block in data.get("content", [])
        if block.get("type") == "text"
    ]
    if not text_blocks:
        log.error("No text block in Claude response")
        log.debug(f"Full response: {json.dumps(data, indent=2)}")
        return None

    raw = text_blocks[-1].strip()
    # Strip markdown fences if model wraps in ```json
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.error(f"Failed to parse Claude JSON response: {exc}")
        log.error(f"Raw response:\n{raw}")
        return None

# ── ALERT FORMATTING ──────────────────────────────────────────────────────────
def format_alert(result: dict, risk_level: str) -> str:
    sep = "━" * 30

    if risk_level == "HIGH":
        header = f"🚨 *GLOBAL CRISIS ALERT — HIGH RISK*"
    else:
        header = f"👁 *GLOBAL MARKET WATCH — MEDIUM RISK*"

    events_text = ""
    for ev in result.get("events", []):
        urgency_emoji = {"immediate": "🔴", "this_week": "🟡", "monitor": "🔵"}.get(
            ev.get("urgency", "monitor"), "🔵"
        )
        events_text += (
            f"\n{urgency_emoji} *{ev.get('title', '')}*\n"
            f"   _{ev.get('impact', '')}_\n"
        )

    msg = (
        f"{header}\n"
        f"{sep}\n"
        f"📌 *{result.get('headline', '')}*\n"
        f"{events_text}\n"
        f"*🇮🇳 India Impact:*\n{result.get('india_impact', '')}\n\n"
        f"*💡 Suggested Action:*\n{result.get('suggested_action', '')}\n"
        f"{sep}\n"
        f"_Crisis Monitor • {today_str()} IST_"
    )
    return msg

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=== Crisis Monitor — Started ===")

    result = call_claude_with_search()
    if result is None:
        send_telegram(
            "🛠 *DEV — Crisis Monitor Failed*\nClaude API call returned no result. "
            "Check ANTHROPIC_API_KEY secret.",
            )
        sys.exit(1)

    risk_level = result.get("risk_level", "NONE").upper()
    headline   = result.get("headline", "")
    log.info(f"Risk level: {risk_level} | {headline}")

    # Always log to CSV
    h = event_hash(headline)
    log_crisis({
        "timestamp":  today_str(),
        "risk_level": risk_level,
        "headline":   headline,
        "events":     str(result.get("events", [])),
        "hash":       h,
    })

    if risk_level in ("HIGH", "MEDIUM"):
        if already_alerted(h):
            log.info(f"Already alerted for this event within {DEDUP_HOURS}h — skipping")
        else:
            alert_msg = format_alert(result, risk_level)
            send_telegram(alert_msg)
    else:
        log.info("No significant risk detected — no alert sent")

    log.info("=== Crisis Monitor — Done ===")


if __name__ == "__main__":
    main()
