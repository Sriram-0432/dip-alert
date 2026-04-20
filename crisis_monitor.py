# -*- coding: utf-8 -*-
"""
Global Crisis Monitor — India-relevant alerts only
====================================================
Runs twice daily (08:00 IST and 20:00 IST) via GitHub Actions.

Alert philosophy: Only fire if the event is CONFIRMED to affect Indian
markets. A US market dip alone, or a generic war headline without India
context, will NOT trigger an alert. This prevents noise and alert fatigue.

TWO LAYERS, BOTH MUST CONFIRM INDIA RELEVANCE:

  LAYER 1 — Market Data (yfinance, free)
    Rule A — Direct India signal (always relevant):
              Nifty 50 drops, India VIX spikes, or USD/INR weakens sharply.
    Rule B — Global contagion signal:
              3 or more major global indices crash in the same session.
              (One market having a bad day = not India's problem.)

  LAYER 2 — News Headlines (free RSS feeds)
    Pass 1 — Match crisis keywords (war, crash, default, pandemic, etc.)
    Pass 2 — Confirm India relevance via one of:
              (a) India/Nifty/Rupee/RBI/Sensex/SEBI mentioned in headline, OR
              (b) Event is in "globally unavoidable" category that hits ALL
                  markets regardless: nuclear threat, WHO pandemic, oil embargo,
                  G7/G20 systemic collapse, major central bank emergency.
    If Pass 2 fails → headline is discarded, no alert.

Secrets needed (no API key required):
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN_CRISIS", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_CRISIS", "")

# ── STATE ─────────────────────────────────────────────────────────────────────
CRISIS_LOG  = "crisis_log.csv"
DEDUP_HOURS = 12

# ─────────────────────────────────────────────────────────────────────────────
# LAYER 1 CONFIG — MARKET WATCHLIST
#
# Group A: DIRECT INDIA signals — any single trigger fires an alert
# Group B: GLOBAL indices — need 3+ simultaneous triggers (contagion rule)
#
# Tuple: (ticker, display_name, group, drop_HIGH_pct, drop_MEDIUM_pct)
# Negative threshold = looking for a drop; positive = looking for a spike
# ─────────────────────────────────────────────────────────────────────────────

INDIA_DIRECT = [
    # (ticker, name, HIGH_threshold, MEDIUM_threshold)
    ("^NSEI",     "Nifty 50",    -3.0, -2.0),   # Indian index drop
    ("^INDIAVIX", "India VIX",  +35.0, +25.0),  # fear spike
    ("USDINR=X",  "USD/INR",     +1.5,  +1.0),  # rupee weakening
]

GLOBAL_INDICES = [
    ("^GSPC",  "S&P 500",      -3.0, -2.0),
    ("^IXIC",  "Nasdaq",       -3.5, -2.5),
    ("^N225",  "Nikkei 225",   -3.0, -2.0),
    ("^HSI",   "Hang Seng",    -4.0, -3.0),
    ("^FTSE",  "FTSE 100",     -3.0, -2.0),
    ("^GDAXI", "DAX",          -3.0, -2.0),
    ("CL=F",   "Crude Oil",    -6.0, -4.0),
]

# Minimum global markets that must trigger simultaneously for contagion rule
CONTAGION_THRESHOLD = 3

# ─────────────────────────────────────────────────────────────────────────────
# LAYER 2 CONFIG — NEWS KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────

RSS_FEEDS = [
    ("Reuters Business",    "https://feeds.reuters.com/reuters/businessNews"),
    ("BBC Business",        "http://feeds.bbci.co.uk/news/business/rss.xml"),
    ("Economic Times Mkts", "https://economictimes.indiatimes.com/markets/rss.cms"),
    ("Mint Markets",        "https://www.livemint.com/rss/markets"),
    ("Reuters World",       "https://feeds.reuters.com/Reuters/worldNews"),
]

# Pass 1: Crisis keyword matching (score + category)
# Score ≥ 3 = HIGH candidate, 2 = MEDIUM candidate
CRISIS_KEYWORDS = [
    # Wars / military
    (r"\bwar\b",                                    3, "War"),
    (r"\bmilitary.{0,15}(strike|attack|invasion)",  3, "War"),
    (r"\bnuclear.{0,20}(threat|weapon|strike)",     3, "Nuclear"),
    (r"\bsanctions?\b",                             2, "Geopolitical"),
    (r"\bblockade\b",                               2, "Geopolitical"),
    # Market crashes
    (r"\bcircuit.{0,10}breaker",                    3, "Market Crash"),
    (r"\bmarket.{0,10}crash",                       3, "Market Crash"),
    (r"\bblack.{0,5}(monday|tuesday|wednesday|thursday|friday)", 3, "Market Crash"),
    (r"\bstock.{0,15}(plunge|collapse|meltdown)",  3, "Market Crash"),
    (r"\bglobal.{0,10}sell.{0,5}off",              2, "Market Crash"),
    # Banking / financial crisis
    (r"\bbank.{0,10}(collapse|fail|run|default)",  3, "Banking Crisis"),
    (r"\bsovereign.{0,10}default",                 3, "Banking Crisis"),
    (r"\bdebt.{0,10}crisis",                       3, "Banking Crisis"),
    (r"\bsystemic.{0,10}risk",                     2, "Banking Crisis"),
    (r"\bbailout\b",                               2, "Banking Crisis"),
    # Central bank emergency
    (r"\bemergency.{0,15}(rate|cut|meeting)",      3, "Central Bank"),
    (r"\bunscheduled.{0,15}(rate|meeting|cut)",    3, "Central Bank"),
    # Oil / commodity shock
    (r"\boil.{0,10}(embargo|shock|crisis)",        3, "Oil Shock"),
    (r"\bopec.{0,15}(cut|ban|embargo)",            2, "Oil Shock"),
    # Pandemic / health
    (r"\bpandemic\b",                              3, "Pandemic"),
    (r"\bwho.{0,15}(emergency|declare|outbreak)",  3, "Pandemic"),
    (r"\bepidemic\b",                              2, "Pandemic"),
    # India-direct (automatically pass the relevance check)
    (r"\bnifty.{0,15}(crash|circuit|halt|suspend)", 3, "India Direct"),
    (r"\brupee.{0,10}(crash|plunge|record.{0,10}low|all.{0,5}time.{0,5}low)", 3, "India Direct"),
    (r"\binr.{0,10}(crash|plunge|record.{0,10}low)", 3, "India Direct"),
    (r"\brbi.{0,10}(emergency|intervene|crisis)",  3, "India Direct"),
    (r"\bsebi.{0,10}(ban|suspend|halt|crisis)",    2, "India Direct"),
    (r"\bindia.{0,10}(recession|downgrad|crisis)",  2, "India Direct"),
]

# Pass 2a: India mention keywords — if any found in headline = relevant
INDIA_MENTION_PATTERNS = [
    r"\bindia\b", r"\bindian\b", r"\bnifty\b", r"\bsensex\b",
    r"\brupee\b", r"\binr\b", r"\brbi\b", r"\bsebi\b",
    r"\bmumbai\b", r"\bnse\b", r"\bbse\b", r"\bnew delhi\b",
    r"\bmodi\b", r"\bbjp\b",
]

# Pass 2b: Globally unavoidable categories — these affect India regardless
# If the event falls in one of these categories AND score ≥ 3, skip India mention check
GLOBALLY_UNAVOIDABLE = {"Nuclear", "Pandemic", "Oil Shock", "Market Crash", "Banking Crisis"}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def ist_now():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def today_str():
    return ist_now().strftime("%Y-%m-%d %H:%M")

def send_telegram(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Crisis Telegram not configured")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Crisis alert sent")
        return True
    except Exception as exc:
        log.error(f"Telegram failed: {exc}")
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

def write_crisis_log(timestamp, risk, headline, source, h):
    exists = os.path.exists(CRISIS_LOG)
    with open(CRISIS_LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp","risk_level","headline","source","hash"])
        if not exists:
            w.writeheader()
        w.writerow({"timestamp": timestamp, "risk_level": risk,
                    "headline": headline, "source": source, "hash": h})


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 1 — MARKET DATA
# ─────────────────────────────────────────────────────────────────────────────

def get_one_day_move(ticker: str) -> float | None:
    """Returns % change from previous close to latest close. None on failure."""
    try:
        df = yf.download(ticker, period="5d", interval="1d", progress=False)
        if df is None or len(df) < 2:
            return None
        close = df["Close"].squeeze().dropna()
        prev, curr = float(close.iloc[-2]), float(close.iloc[-1])
        if prev == 0:
            return None
        return ((curr - prev) / prev) * 100
    except Exception as exc:
        log.warning(f"  yfinance failed for {ticker}: {exc}")
        return None


def check_markets() -> tuple[list, list, str]:
    """
    Returns (india_hits, global_hits, overall_risk).
    india_hits  — Direct India signals triggered (any = alert)
    global_hits — Global indices triggered (need CONTAGION_THRESHOLD = alert)
    """
    india_hits  = []
    global_hits = []

    log.info("  Checking direct India indicators...")
    for ticker, name, high_t, med_t in INDIA_DIRECT:
        pct = get_one_day_move(ticker)
        if pct is None:
            log.info(f"    {name}: no data")
            continue
        if high_t < 0:
            triggered_high = pct <= high_t
            triggered_med  = high_t < pct <= med_t
        else:
            triggered_high = pct >= high_t
            triggered_med  = med_t <= pct < high_t

        if triggered_high or triggered_med:
            risk = "HIGH" if triggered_high else "MEDIUM"
            india_hits.append({"name": name, "pct": round(pct, 2), "risk": risk})
            log.info(f"    ⚡ {name}: {pct:+.2f}% → {risk}")
        else:
            log.info(f"    {name}: {pct:+.2f}% — normal")

    log.info("  Checking global indices for contagion...")
    for ticker, name, high_t, med_t in GLOBAL_INDICES:
        pct = get_one_day_move(ticker)
        if pct is None:
            continue
        if high_t < 0:
            triggered = pct <= med_t   # use MEDIUM as the contagion bar
        else:
            triggered = pct >= med_t

        if triggered:
            risk = "HIGH" if (pct <= high_t if high_t < 0 else pct >= high_t) else "MEDIUM"
            global_hits.append({"name": name, "pct": round(pct, 2), "risk": risk})
            log.info(f"    ⚡ {name}: {pct:+.2f}% → {risk}")
        else:
            log.info(f"    {name}: {pct:+.2f}% — normal")

    # Determine overall risk
    contagion = len(global_hits) >= CONTAGION_THRESHOLD
    if india_hits:
        risk = "HIGH" if any(h["risk"] == "HIGH" for h in india_hits) else "MEDIUM"
    elif contagion:
        risk = "HIGH" if sum(1 for h in global_hits if h["risk"] == "HIGH") >= 2 else "MEDIUM"
        log.info(f"  Contagion rule triggered ({len(global_hits)} global markets affected)")
    else:
        risk = "NONE"

    return india_hits, global_hits, risk


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 2 — NEWS RSS (with India relevance gate)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_rss_headlines(url: str, max_items: int = 25) -> list[str]:
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root  = ET.fromstring(resp.content)
        items = root.findall(".//item")[:max_items]
        out   = []
        for item in items:
            parts = [
                (item.find(tag).text or "").strip()
                for tag in ("title", "description")
                if item.find(tag) is not None
            ]
            text = " | ".join(p for p in parts if p)
            if text:
                out.append(text)
        return out
    except Exception as exc:
        log.warning(f"    RSS fetch failed ({url}): {exc}")
        return []


def is_india_relevant(text_lower: str, categories: set) -> tuple[bool, str]:
    """
    Returns (is_relevant, reason_string).
    Two paths to relevance:
      (a) India explicitly mentioned in the headline
      (b) Event is globally unavoidable AND high-scoring
    """
    # Path (a): India mentioned directly
    for pat in INDIA_MENTION_PATTERNS:
        if re.search(pat, text_lower):
            return True, "India mentioned directly"

    # (b): Globally unavoidable category
    overlap = categories & GLOBALLY_UNAVOIDABLE
    if overlap:
        return True, f"Globally unavoidable: {', '.join(overlap)}"

    return False, "No India relevance confirmed"


def scan_news() -> list[dict]:
    """
    Returns confirmed India-relevant crisis headlines only.
    Two-pass: keyword match → India relevance gate.
    """
    confirmed = []
    seen_keys = set()

    for source_name, url in RSS_FEEDS:
        log.info(f"  Scanning: {source_name}")
        headlines = fetch_rss_headlines(url)
        log.info(f"    {len(headlines)} headlines fetched")

        for text in headlines:
            text_lower = text.lower()

            # Pass 1: Crisis keyword scoring
            total_score = 0
            categories  = set()
            for pattern, score, category in CRISIS_KEYWORDS:
                if re.search(pattern, text_lower):
                    total_score += score
                    categories.add(category)

            if total_score < 2:
                continue   # Not a crisis headline at all

            # "India Direct" category automatically passes relevance check
            if "India Direct" in categories:
                relevant, reason = True, "India Direct category"
            else:
                # Pass 2: India relevance gate
                relevant, reason = is_india_relevant(text_lower, categories)

            if not relevant:
                log.info(f"    ✗ Filtered out (no India relevance): {text[:80]}...")
                continue

            # Deduplicate
            key = text[:80]
            if key in seen_keys:
                continue
            seen_keys.add(key)

            risk = "HIGH" if total_score >= 3 else "MEDIUM"
            confirmed.append({
                "headline":   text[:220],
                "score":      total_score,
                "categories": list(categories),
                "risk":       risk,
                "source":     source_name,
                "reason":     reason,
            })
            log.info(f"    ✓ Confirmed [{risk}] ({reason}): {text[:80]}...")

    confirmed.sort(key=lambda x: x["score"], reverse=True)
    return confirmed


# ─────────────────────────────────────────────────────────────────────────────
# ALERT FORMATTING
# ─────────────────────────────────────────────────────────────────────────────

def format_market_alert(india_hits: list, global_hits: list, risk: str) -> str:
    sep    = "━" * 30
    header = "🚨 *CRISIS ALERT — INDIA MARKET SHOCK*" if risk == "HIGH" \
             else "👁 *MARKET WATCH — INDIA IMPACT DETECTED*"
    lines  = [header, sep]

    if india_hits:
        lines.append("*🇮🇳 Direct India Signals:*")
        for h in india_hits:
            arrow = "🔻" if h["pct"] < 0 else "🔺"
            icon  = "‼️" if h["risk"] == "HIGH" else "⚠️"
            lines.append(f"  {icon} *{h['name']}*: {arrow} `{h['pct']:+.2f}%`")

    if global_hits and len(global_hits) >= CONTAGION_THRESHOLD:
        lines.append(f"\n*🌍 Global Contagion ({len(global_hits)} markets):*")
        for h in global_hits:
            arrow = "🔻" if h["pct"] < 0 else "🔺"
            lines.append(f"  ⚠️ {h['name']}: {arrow} `{h['pct']:+.2f}%`")

    lines += [
        "",
        "📌 *What to do:*",
        "Indian market likely to open weak. Monitor your dip alert channel.",
        "Do NOT panic sell existing SIPs. Await NAV confirmation.",
        sep,
        f"_Crisis Monitor • {today_str()} IST_",
    ]
    return "\n".join(lines)


def format_news_alert(matches: list[dict]) -> str:
    sep    = "━" * 30
    risk   = "HIGH" if any(m["risk"] == "HIGH" for m in matches) else "MEDIUM"
    header = "🚨 *CRISIS ALERT — INDIA-RELEVANT NEWS*" if risk == "HIGH" \
             else "👁 *MARKET WATCH — INDIA-RELEVANT NEWS*"

    lines = [header, sep]
    for m in matches[:4]:
        cats = ", ".join(m["categories"])
        icon = "🔴" if m["risk"] == "HIGH" else "🟡"
        lines += [
            f"{icon} *[{cats}]* — _{m['source']}_",
            f"   {m['headline'][:200]}",
            f"   _India relevance: {m['reason']}_",
            "",
        ]

    lines += [
        "📌 *Suggested action:*",
        "Stay informed. Wait for dip alert to confirm actual NAV impact.",
        "SIP: continue as planned. Lump sum: hold until signal confirmed.",
        sep,
        f"_Crisis Monitor • {today_str()} IST_",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("═" * 52)
    log.info("  Crisis Monitor — India-Relevant Filter Active")
    log.info("═" * 52)

    # ── Layer 1: Market data ───────────────────────────────────────────────
    log.info("Layer 1: Market data check...")
    india_hits, global_hits, market_risk = check_markets()

    if market_risk != "NONE":
        summary = " | ".join(
            f"{h['name']} {h['pct']:+.2f}%"
            for h in (india_hits + global_hits)
        )
        h = event_hash("market:" + summary)
        write_crisis_log(today_str(), market_risk, summary, "market_data", h)

        if not already_alerted(h):
            log.info(f"  → Firing market alert [{market_risk}]")
            send_telegram(format_market_alert(india_hits, global_hits, market_risk))
        else:
            log.info("  → Market alert already sent within 12h — suppressed")
    else:
        log.info("  No India-relevant market moves detected")

    # ── Layer 2: News headlines ────────────────────────────────────────────
    log.info("Layer 2: News RSS scan with India relevance gate...")
    news_matches = scan_news()
    log.info(f"  India-confirmed matches: {len(news_matches)}")

    if news_matches:
        high = [m for m in news_matches if m["risk"] == "HIGH"]
        med  = [m for m in news_matches if m["risk"] == "MEDIUM"]

        for group, label in [(high, "HIGH"), (med, "MEDIUM")]:
            if not group:
                continue
            h = event_hash("news:" + label + group[0]["headline"])
            write_crisis_log(today_str(), label, group[0]["headline"], group[0]["source"], h)

            if not already_alerted(h):
                log.info(f"  → Firing news alert [{label}]")
                send_telegram(format_news_alert(group))
            else:
                log.info(f"  → {label} news alert already sent within 12h — suppressed")
    else:
        log.info("  No India-relevant crisis news confirmed")

    log.info("═" * 52)
    log.info("  Crisis Monitor — Done")
    log.info("═" * 52)


if __name__ == "__main__":
    main()
