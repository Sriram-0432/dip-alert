# -*- coding: utf-8 -*-
"""
Global Crisis Monitor — zero API key, completely free
=======================================================
Runs twice daily (08:00 IST and 20:00 IST) via GitHub Actions.

Two detection layers — no paid API needed:

  LAYER 1 — Market Data (yfinance)
    Checks overnight moves in major global indices and commodities:
    S&P 500, Nasdaq, Hang Seng, Nikkei, crude oil, gold, USD/INR.
    Triggers alert if any single asset drops beyond threshold in one session.

  LAYER 2 — News RSS Headlines (free feeds)
    Fetches latest headlines from Reuters Business, BBC Business,
    Economic Times Markets, and Mint. Scans for crisis keywords
    (war, sanctions, crash, circuit breaker, pandemic, default, etc.)
    Matches are scored and rated HIGH / MEDIUM / LOW.

Requires NO new secrets — uses the dedicated crisis channel:
  TELEGRAM_BOT_TOKEN_CRISIS
  TELEGRAM_CHAT_ID_CRISIS
"""

import os
import sys
import io
import re
import csv
import logging
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

import requests
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

# ── TELEGRAM (dedicated crisis channel) ──────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN_CRISIS", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_CRISIS", "")

# ── STATE ─────────────────────────────────────────────────────────────────────
CRISIS_LOG  = "crisis_log.csv"
DEDUP_HOURS = 12    # suppress identical alert within this window

# ─────────────────────────────────────────────────────────────────────────────
# LAYER 1 — MARKET THRESHOLDS
# Each tuple: (yfinance_ticker, display_name, drop_HIGH%, drop_MEDIUM%)
# Negative = drop; positive = spike (e.g. VIX spike, USD/INR spike)
# ─────────────────────────────────────────────────────────────────────────────
MARKET_WATCHLIST = [
    # Global equity indices
    ("^GSPC",    "S&P 500",          -3.0,  -2.0),
    ("^IXIC",    "Nasdaq",           -3.5,  -2.5),
    ("^N225",    "Nikkei 225",       -3.0,  -2.0),
    ("^HSI",     "Hang Seng",        -4.0,  -2.5),
    ("^FTSE",    "FTSE 100",         -3.0,  -2.0),
    # Indian market
    ("^NSEI",    "Nifty 50",         -3.0,  -2.0),
    ("^INDIAVIX","India VIX",        +30.0, +20.0),  # spike = fear
    # Commodities & FX
    ("CL=F",     "Crude Oil (WTI)",  -5.0,  -3.5),
    ("GC=F",     "Gold",             +3.0,  +2.0),   # gold spike = panic
    ("USDINR=X", "USD/INR",          +1.5,  +1.0),   # INR weakening
]

# ─────────────────────────────────────────────────────────────────────────────
# LAYER 2 — NEWS RSS FEEDS + KEYWORD SCORING
# ─────────────────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    ("Reuters Business",    "https://feeds.reuters.com/reuters/businessNews"),
    ("BBC Business",        "http://feeds.bbci.co.uk/news/business/rss.xml"),
    ("Economic Times Mkts", "https://economictimes.indiatimes.com/markets/rss.cms"),
    ("Mint Markets",        "https://www.livemint.com/rss/markets"),
]

# (keyword_pattern, score, category)
# Score 3 = HIGH, 2 = MEDIUM, 1 = LOW
CRISIS_KEYWORDS = [
    # Wars / geopolitics
    (r"\bwar\b",                        3, "Geopolitical"),
    (r"\bmilitary.{0,15}strike",        3, "Geopolitical"),
    (r"\bnuclear\b",                    3, "Geopolitical"),
    (r"\bsanctions?\b",                 2, "Geopolitical"),
    (r"\bblockade\b",                   2, "Geopolitical"),
    (r"\bgeopolit",                     1, "Geopolitical"),

    # Market crashes
    (r"\bcircuit.{0,10}breaker",        3, "Market Crash"),
    (r"\bmarket.{0,10}crash",           3, "Market Crash"),
    (r"\bblack.{0,5}(monday|tuesday|wednesday|thursday|friday)", 3, "Market Crash"),
    (r"\bstock.{0,10}(plunge|collapse|meltdown)", 3, "Market Crash"),
    (r"\bsell.{0,5}off\b",             2, "Market Crash"),
    (r"\bbear.{0,8}market",            2, "Market Crash"),

    # Financial / banking crises
    (r"\bbank.{0,10}(collapse|fail|run|default)", 3, "Banking Crisis"),
    (r"\bsovereign.{0,10}default",     3, "Banking Crisis"),
    (r"\bdebt.{0,10}crisis",           3, "Banking Crisis"),
    (r"\bliquidity.{0,10}crisis",      2, "Banking Crisis"),
    (r"\bbailout\b",                   2, "Banking Crisis"),
    (r"\bcredit.{0,10}crunch",         2, "Banking Crisis"),

    # Central bank emergency
    (r"\bemergency.{0,15}rate",        3, "Central Bank"),
    (r"\bunscheduled.{0,15}(meeting|cut|hike)", 3, "Central Bank"),
    (r"\bfed.{0,10}(emergency|panic|crisis)", 2, "Central Bank"),
    (r"\brbi.{0,10}(emergency|action|intervene)", 2, "Central Bank"),

    # Commodities / supply chain
    (r"\boil.{0,10}(shock|embargo|crisis|spike)", 2, "Commodity"),
    (r"\bsupply.{0,10}chain.{0,10}(crisis|collapse|disruption)", 2, "Commodity"),

    # Health / pandemic
    (r"\bpandemic\b",                  3, "Health"),
    (r"\bwho.{0,15}(emergency|declare|alert)", 3, "Health"),
    (r"\bepidemic\b",                  2, "Health"),
    (r"\blockdown.{0,15}(china|global|nationwide)", 2, "Health"),

    # India-specific macro
    (r"\binr.{0,10}(crash|collapse|record.{0,10}low)", 3, "India Macro"),
    (r"\brupee.{0,10}(crash|plunge|record.{0,10}low)", 3, "India Macro"),
    (r"\bsebi.{0,10}(ban|suspend|halt)",               2, "India Macro"),
    (r"\bindia.{0,10}(recession|downgr)",              2, "India Macro"),
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def ist_now():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def today_str():
    return ist_now().strftime("%Y-%m-%d %H:%M")

def send_telegram(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Crisis Telegram not configured — set TELEGRAM_BOT_TOKEN_CRISIS + TELEGRAM_CHAT_ID_CRISIS")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Crisis alert sent to Telegram")
        return True
    except Exception as exc:
        log.error(f"Telegram send failed: {exc}")
        return False

def event_hash(text: str) -> str:
    return hashlib.md5(text[:120].encode()).hexdigest()

def already_alerted(h: str) -> bool:
    if not os.path.exists(CRISIS_LOG):
        return False
    try:
        with open(CRISIS_LOG, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("hash") == h:
                    ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M")
                    if (ist_now().replace(tzinfo=None) - ts) < timedelta(hours=DEDUP_HOURS):
                        return True
    except Exception:
        pass
    return False

def log_crisis(timestamp, risk, headline, source, h):
    file_exists = os.path.exists(CRISIS_LOG)
    with open(CRISIS_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp","risk_level","headline","source","hash"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "timestamp":  timestamp,
            "risk_level": risk,
            "headline":   headline,
            "source":     source,
            "hash":       h,
        })


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 1 — MARKET DATA CHECK
# ─────────────────────────────────────────────────────────────────────────────

def check_markets() -> list[dict]:
    """
    Downloads last 5 days of data for each ticker.
    Returns list of triggered assets with their move %.
    """
    triggered = []
    for ticker, name, high_thresh, med_thresh in MARKET_WATCHLIST:
        try:
            df = yf.download(ticker, period="5d", interval="1d", progress=False)
            if df is None or len(df) < 2:
                continue

            close = df["Close"].squeeze().dropna()
            if len(close) < 2:
                continue

            prev  = float(close.iloc[-2])
            curr  = float(close.iloc[-1])
            if prev == 0:
                continue

            pct_chg = ((curr - prev) / prev) * 100

            # Determine direction of concern
            if high_thresh < 0:
                is_high = pct_chg <= high_thresh
                is_med  = high_thresh < pct_chg <= med_thresh
            else:
                is_high = pct_chg >= high_thresh
                is_med  = med_thresh <= pct_chg < high_thresh

            if is_high or is_med:
                risk = "HIGH" if is_high else "MEDIUM"
                triggered.append({
                    "ticker":  ticker,
                    "name":    name,
                    "pct_chg": round(pct_chg, 2),
                    "curr":    round(curr, 2),
                    "risk":    risk,
                })
                log.info(f"  Market trigger: {name} {pct_chg:+.2f}% → {risk}")
            else:
                log.info(f"  {name}: {pct_chg:+.2f}% — within normal range")

        except Exception as exc:
            log.warning(f"  {name} ({ticker}) fetch failed: {exc}")

    return triggered


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 2 — RSS NEWS SCAN
# ─────────────────────────────────────────────────────────────────────────────

def fetch_rss_headlines(url: str, max_items: int = 20) -> list[str]:
    """Parse RSS feed and return list of title + description strings."""
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root  = ET.fromstring(resp.content)
        items = root.findall(".//item")[:max_items]
        texts = []
        for item in items:
            parts = []
            for tag in ("title", "description"):
                el = item.find(tag)
                if el is not None and el.text:
                    parts.append(el.text.strip())
            if parts:
                texts.append(" | ".join(parts))
        return texts
    except Exception as exc:
        log.warning(f"RSS fetch failed for {url}: {exc}")
        return []


def score_headlines(headlines: list[str]) -> list[dict]:
    """
    Returns list of matched headlines with score and category.
    Score 3+ = HIGH, 2 = MEDIUM, 1 = LOW.
    """
    matched = []
    seen    = set()

    for text in headlines:
        text_lower = text.lower()
        total_score = 0
        categories  = set()

        for pattern, score, category in CRISIS_KEYWORDS:
            if re.search(pattern, text_lower):
                total_score += score
                categories.add(category)

        if total_score >= 2:
            key = text[:80]
            if key not in seen:
                seen.add(key)
                risk = "HIGH" if total_score >= 3 else "MEDIUM"
                matched.append({
                    "headline":   text[:200],
                    "score":      total_score,
                    "categories": list(categories),
                    "risk":       risk,
                })

    # Sort most alarming first
    matched.sort(key=lambda x: x["score"], reverse=True)
    return matched


def check_news() -> list[dict]:
    """Scan all RSS feeds, return matched items with source tag."""
    all_matches = []
    for source_name, url in RSS_FEEDS:
        log.info(f"  Scanning: {source_name}")
        headlines = fetch_rss_headlines(url)
        matches   = score_headlines(headlines)
        for m in matches:
            m["source"] = source_name
        all_matches.extend(matches)
        log.info(f"    {len(headlines)} headlines → {len(matches)} matches")
    return all_matches


# ─────────────────────────────────────────────────────────────────────────────
# ALERT FORMATTING
# ─────────────────────────────────────────────────────────────────────────────

def format_market_alert(triggered: list[dict]) -> str:
    sep   = "━" * 30
    level = "HIGH" if any(t["risk"] == "HIGH" for t in triggered) else "MEDIUM"
    header = "🚨 *GLOBAL CRISIS ALERT — MARKET SHOCK*" if level == "HIGH" \
             else "👁 *MARKET WATCH — SIGNIFICANT MOVES*"

    lines = [header, sep]
    for t in triggered:
        arrow  = "🔻" if t["pct_chg"] < 0 else "🔺"
        risk_tag = "‼️" if t["risk"] == "HIGH" else "⚠️"
        lines.append(f"{risk_tag} *{t['name']}*: {arrow} `{t['pct_chg']:+.2f}%`  _(now {t['curr']:,})_")

    lines += [
        "",
        "📌 *What this means for Indian MFs:*",
        "Global sell-offs typically drag Indian markets within 1–2 sessions.",
        "Monitor Nifty open. If further dip — dip alert will trigger.",
        sep,
        f"_Crisis Monitor • {today_str()} IST_",
    ]
    return "\n".join(lines)


def format_news_alert(matches: list[dict]) -> str:
    sep   = "━" * 30
    level = "HIGH" if any(m["risk"] == "HIGH" for m in matches) else "MEDIUM"
    header = "🚨 *GLOBAL CRISIS ALERT — NEWS*" if level == "HIGH" \
             else "👁 *GLOBAL MARKET WATCH — NEWS*"

    lines = [header, sep]
    # Show top 4 matches max to keep message readable
    for m in matches[:4]:
        cats = ", ".join(m["categories"])
        icon = "🔴" if m["risk"] == "HIGH" else "🟡"
        lines.append(f"{icon} *[{cats}]* _{m['source']}_")
        lines.append(f"   {m['headline'][:180]}")
        lines.append("")

    lines += [
        "📌 *Suggested action:*",
        "Stay alert. Do NOT make panic decisions.",
        "Wait for dip alert system to confirm a real NAV drop.",
        sep,
        f"_Crisis Monitor • {today_str()} IST_",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("═" * 50)
    log.info("  Crisis Monitor — Started")
    log.info("═" * 50)

    # ── Layer 1: Market data ───────────────────────────────────────────────
    log.info("Layer 1: Checking global market moves...")
    market_hits = check_markets()
    log.info(f"  Triggered assets: {len(market_hits)}")

    # ── Layer 2: RSS news ─────────────────────────────────────────────────
    log.info("Layer 2: Scanning news RSS feeds...")
    news_hits = check_news()
    log.info(f"  Crisis news matches: {len(news_hits)}")

    # ── Market alerts ─────────────────────────────────────────────────────
    if market_hits:
        summary  = " | ".join(f"{t['name']} {t['pct_chg']:+.2f}%" for t in market_hits)
        h        = event_hash("market:" + summary)
        risk     = "HIGH" if any(t["risk"] == "HIGH" for t in market_hits) else "MEDIUM"

        log_crisis(today_str(), risk, summary, "market_data", h)

        if not already_alerted(h):
            msg = format_market_alert(market_hits)
            send_telegram(msg)
        else:
            log.info("  Market alert already sent within 12h — skipping")
    else:
        log.info("  No abnormal market moves detected")

    # ── News alerts ───────────────────────────────────────────────────────
    if news_hits:
        # Group by risk level — fire one message per level max
        high_hits = [m for m in news_hits if m["risk"] == "HIGH"]
        med_hits  = [m for m in news_hits if m["risk"] == "MEDIUM"]

        for group, label in [(high_hits, "HIGH"), (med_hits, "MEDIUM")]:
            if not group:
                continue
            key = event_hash("news:" + label + group[0]["headline"])
            log_crisis(today_str(), label, group[0]["headline"], group[0]["source"], key)

            if not already_alerted(key):
                msg = format_news_alert(group)
                send_telegram(msg)
            else:
                log.info(f"  {label} news alert already sent within 12h — skipping")
    else:
        log.info("  No crisis keywords found in news feeds")

    log.info("═" * 50)
    log.info("  Crisis Monitor — Done")
    log.info("═" * 50)


if __name__ == "__main__":
    main()
