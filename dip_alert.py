# -*- coding: utf-8 -*-
"""
Mutual Fund Dip Alert Pipeline v3.2  — Institutional Grade
============================================================
Funds     : PPFCF Direct (122639) + UTI Nifty 50 Direct (120716) + MO Midcap Direct (127042)
Alerts    : Single Telegram channel for all funds + dev errors
Scheduler : GitHub Actions (cron: 02:30 UTC = 08:00 IST, weekdays only)
Signals   : 52-week MDD tiers — L1 ≥5%, L2 ≥8%, L3 ≥12%, L4 ≥20%
State     : SQLite (watermarks.db) committed to repo each run
Retries   : tenacity exponential backoff on all HTTP calls

v3.2 additions:
  RSI-14       — Confirms fund is technically oversold (RSI < 35)
  Rel Strength — Classifies dip as Systemic vs Fund-Specific vs Fund-Strong
  Pyramiding   — Exact deploy % advice per tier (10 / 25 / 50 / 75%)
"""

import os
import sys
import io
import logging
import hashlib
import sqlite3
from datetime import datetime, timezone, timedelta
from json import JSONDecodeError

import requests
import pandas as pd
import yfinance as yf
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    RetryError,
)
from requests.exceptions import RequestException

# ── UTF-8 STDOUT FIX ──────────────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ── SINGLE TELEGRAM CHANNEL ───────────────────────────────────────────────────
# One bot, one chat — all fund alerts AND dev/failure messages go here.
# GitHub Secrets needed:
#   TELEGRAM_BOT_TOKEN   →  from @BotFather
#   TELEGRAM_CHAT_ID     →  your personal chat ID (from @userinfobot)
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── FUND CONFIG ───────────────────────────────────────────────────────────────
# Scheme codes verified against AMFI:
#   122639 = Parag Parikh Flexi Cap Fund — Direct Plan — Growth
#   120716 = UTI Nifty 50 Index Fund — Direct Plan — Growth
FUNDS = {
    "122639": {
        "name":      "PPFAS Flexi Cap Direct",
        "emoji":     "🟦",
        "threshold": 5.0,   # MDD % to trigger minimum alert
    },
    "120716": {
        "name":      "UTI Nifty 50 Index Direct",
        "emoji":     "🟧",
        "threshold": 3.5,   # Index fund — lower volatility, lower bar
    },
    "127042": {
        "name":      "Motilal Oswal Midcap Direct",
        "emoji":     "🟩",
        "threshold": 5.0,   # Midcap fund — standard 5% trigger
    },
}

# ── SIGNAL TIERS ──────────────────────────────────────────────────────────────
# MDD thresholds mapped to actionable investment signals.
# Evaluated most-severe-first; first match wins.
#
#   MDD ≥ 20%  →  AGGRESSIVE BUY  — deep crash, deploy maximum capital
#   MDD ≥ 12%  →  STRONG BUY      — significant correction, strong entry
#   MDD ≥  8%  →  BUY             — healthy dip, start building position
#   MDD ≥  5%  →  ACCUMULATE      — mild pullback, add small tranches
#   MDD <  5%  →  WAIT            — no actionable dip yet (no alert sent)
#
# (mdd_threshold, tier_level, signal_label, action_line)
ALERT_TIERS = [
    (0.20, 4, "🚨 AGGRESSIVE BUY",  "Deploy maximum capital. Deep crash — rare entry."),
    (0.12, 3, "💥 STRONG BUY",      "Strong entry point. Significant correction."),
    (0.08, 2, "✅ BUY",             "Start building position. Healthy dip confirmed."),
    (0.05, 1, "📉 ACCUMULATE",      "Add a small tranche. Mild pullback in progress."),
]

# ── EXTERNAL DATA SOURCES ─────────────────────────────────────────────────────
INDIA_VIX    = "^INDIAVIX"
AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
LOOKBACK_DAYS  = 252   # ~52 trading weeks = 1 full year of NAV data
VIX_HIGH       = 18.0  # above = "high volatility" flag in alert message
COOLDOWN_HOURS = 24    # suppress same-tier re-alert within this window
RSI_PERIOD     = 14    # standard RSI window
RSI_OVERSOLD   = 35    # below this = technically oversold (MF NAVs lag, so 35 not 30)
NIFTY_TICKER   = "^NSEI"

# ── PYRAMIDING CONFIG ─────────────────────────────────────────────────────────
# How much of your available dry powder to deploy at each tier.
# Tier→ (deploy_pct, cash_reserve_note)
PYRAMID = {
    1: (10,  "Keep 90% dry — dip may deepen."),
    2: (25,  "Meaningful entry. Keep 75% in reserve."),
    3: (50,  "Strong conviction entry. Hold 50% for further dips."),
    4: (75,  "Rare crash entry. Deploy 75%. Keep 25% for any further leg down."),
}

# ── STATE FILES (all git-committed by workflow after each run) ────────────────
WATERMARK_DB  = "watermarks.db"
SIGNAL_LOG    = "signal_log.csv"
PROCESSED_LOG = "nav_processed.txt"


# ─────────────────────────────────────────────────────────────────────────────
# IST HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def ist_now() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def today_str() -> str:
    return ist_now().strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP — TENACITY RETRY WRAPPERS
# ─────────────────────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.5, min=2, max=60),
    retry=retry_if_exception_type((RequestException, JSONDecodeError)),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def _get_json(url: str, timeout: int = 30) -> dict:
    """GET → JSON with exponential backoff. Retries on network errors + bad JSON."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    try:
        return resp.json()
    except JSONDecodeError as exc:
        log.error(f"JSONDecodeError at {url}: {exc}")
        raise


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.5, min=2, max=60),
    retry=retry_if_exception_type(RequestException),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def _get_text(url: str, timeout: int = 30) -> str:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM — SINGLE CHANNEL
# ─────────────────────────────────────────────────────────────────────────────

def send_alert(message: str, is_dev: bool = False) -> bool:
    """
    Send any message to the single configured Telegram channel.
    is_dev=True prepends a wrench header so pipeline errors are visually distinct
    from fund alerts even though they share the same chat.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured — set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID")
        return False
    text = f"🛠 *DEV — Pipeline Error*\n{message}" if is_dev else message
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Telegram message sent")
        return True
    except Exception as exc:
        log.error(f"Telegram send failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SQLITE — WATERMARK + COOLDOWN STORE
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    """Create tables if absent. Idempotent — safe to call on every run."""
    con = sqlite3.connect(WATERMARK_DB)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS watermarks (
            scheme_code TEXT PRIMARY KEY,
            peak_nav    REAL NOT NULL,
            peak_date   TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alert_cooldown (
            scheme_code  TEXT    NOT NULL,
            tier         INTEGER NOT NULL,
            last_alerted TEXT    NOT NULL,
            PRIMARY KEY (scheme_code, tier)
        );
    """)
    con.commit()
    con.close()
    log.info("SQLite DB initialised (watermarks.db)")


def get_stored_peak(scheme_code: str) -> tuple:
    con = sqlite3.connect(WATERMARK_DB)
    row = con.execute(
        "SELECT peak_nav, peak_date FROM watermarks WHERE scheme_code = ?",
        (scheme_code,),
    ).fetchone()
    con.close()
    return (row[0], row[1]) if row else (None, None)


def upsert_peak(scheme_code: str, nav: float, nav_date: str):
    """Ratchets upward only — never overwrites a historical high with a lower value."""
    con = sqlite3.connect(WATERMARK_DB)
    con.execute("""
        INSERT INTO watermarks (scheme_code, peak_nav, peak_date, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(scheme_code) DO UPDATE SET
            peak_nav   = excluded.peak_nav,
            peak_date  = excluded.peak_date,
            updated_at = excluded.updated_at
        WHERE excluded.peak_nav > watermarks.peak_nav
    """, (scheme_code, nav, nav_date, today_str()))
    con.commit()
    con.close()


def is_on_cooldown(scheme_code: str, tier: int) -> bool:
    con = sqlite3.connect(WATERMARK_DB)
    row = con.execute(
        "SELECT last_alerted FROM alert_cooldown WHERE scheme_code = ? AND tier = ?",
        (scheme_code, tier),
    ).fetchone()
    con.close()
    if not row:
        return False
    last = datetime.fromisoformat(row[0])
    return (ist_now().replace(tzinfo=None) - last) < timedelta(hours=COOLDOWN_HOURS)


def set_cooldown(scheme_code: str, tier: int):
    con = sqlite3.connect(WATERMARK_DB)
    con.execute("""
        INSERT INTO alert_cooldown (scheme_code, tier, last_alerted)
        VALUES (?, ?, ?)
        ON CONFLICT(scheme_code, tier) DO UPDATE SET last_alerted = excluded.last_alerted
    """, (scheme_code, tier, ist_now().replace(tzinfo=None).isoformat()))
    con.commit()
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def fetch_amfi_navs() -> dict:
    """
    Download AMFI's daily NAVAll.txt → {scheme_code: float}.
    Most up-to-date source; updated by ~8 PM IST each trading day.
    Returns {} on failure so callers fall back to mfapi history.
    """
    try:
        text = _get_text(AMFI_NAV_URL)
        navs: dict[str, float] = {}
        for line in text.splitlines():
            parts = line.split(";")
            if len(parts) >= 5:
                code = parts[0].strip()
                try:
                    navs[code] = float(parts[4].strip())
                except (ValueError, TypeError):
                    pass
        log.info(f"AMFI NAV file loaded: {len(navs)} schemes")
        return navs
    except Exception as exc:
        log.warning(f"AMFI NAV fetch failed: {exc}")
        send_alert(f"AMFI NAV fetch failed — falling back to mfapi.\n`{exc}`", is_dev=True)
        return {}


def fetch_nav_history(scheme_code: str) -> pd.Series:
    """
    Historical NAV series from mfapi.in.
    Returns date-indexed pd.Series of float NAVs, sorted ascending.
    Explicit guards for JSONDecodeError (API), KeyError (schema change),
    ValueError/TypeError (bad 'nav' string like '-' or '').
    """
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    try:
        payload = _get_json(url)
    except RetryError as exc:
        raise RuntimeError(f"mfapi exhausted all retries for {scheme_code}: {exc}") from exc

    try:
        records = payload["data"]
    except KeyError as exc:
        raise RuntimeError(
            f"mfapi 'data' key missing for {scheme_code}. "
            f"Keys present: {list(payload.keys())}"
        ) from exc

    parsed: dict = {}
    skipped = 0
    for entry in records:
        try:
            d   = datetime.strptime(entry["date"], "%d-%m-%Y").date()
            nav = float(entry["nav"])   # strict cast — raises on "-", "", None
            parsed[d] = nav
        except (KeyError, ValueError, TypeError):
            skipped += 1
            continue

    if skipped:
        log.warning(f"{scheme_code}: skipped {skipped} malformed NAV rows")
    if not parsed:
        raise RuntimeError(f"No valid NAV rows could be parsed for {scheme_code}")

    series = pd.Series(parsed).sort_index()
    log.info(
        f"NAV history: {scheme_code} | {len(series)} rows | "
        f"{series.index[0]} → {series.index[-1]}"
    )
    return series


def fetch_vix() -> float:
    """
    India VIX from yfinance. Hard-raises RuntimeError on failure.
    Callers must catch this and abort — we never assume a default VIX value.
    """
    try:
        df = yf.download(INDIA_VIX, period="5d", progress=False)
        if df.empty:
            raise ValueError("yfinance returned empty dataframe for VIX")
        return float(df["Close"].squeeze().iloc[-1])
    except Exception as exc:
        raise RuntimeError(f"India VIX fetch failed: {exc}") from exc


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL MATH
# ─────────────────────────────────────────────────────────────────────────────

def refresh_watermark(scheme_code: str, history: pd.Series) -> tuple:
    """
    1. Find the peak in the most recent LOOKBACK_DAYS of live history.
    2. Upsert into SQLite (only if higher than stored value).
    3. Read back from DB — may return a higher value from a prior run.
    This ensures the 52-week high is never lost to a short API window.
    """
    window    = history.iloc[-LOOKBACK_DAYS:]
    peak_idx  = str(window.idxmax())
    peak_nav  = float(window.max())
    upsert_peak(scheme_code, peak_nav, peak_idx)
    return get_stored_peak(scheme_code)   # authoritative value from DB


def compute_mdd(current: float, peak: float) -> float:
    if peak <= 0.0:
        log.warning("Peak NAV ≤ 0 — returning 0.0 (ZeroDivisionError guard)")
        return 0.0
    return (peak - current) / peak


# ─────────────────────────────────────────────────────────────────────────────
# v3.2 FEATURE 1 — RSI-14 (Oversold Confirmation)
# ─────────────────────────────────────────────────────────────────────────────

def compute_rsi(series: pd.Series, period: int = RSI_PERIOD) -> float | None:
    """
    Wilder's RSI on a NAV price series.
    Returns float RSI (0–100) or None if insufficient data.
    Note: MF NAVs are end-of-day smoothed — RSI will lag live market RSI
    by ~1 session. Used here as a confirmation filter, not a primary signal.
    """
    if len(series) < period + 1:
        return None
    delta  = series.diff().dropna()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)
    # Wilder smoothing (exponential with alpha = 1/period)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    last_loss = float(avg_loss.iloc[-1])
    if last_loss == 0:
        return 100.0
    rs  = float(avg_gain.iloc[-1]) / last_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 1)


def rsi_label(rsi: float | None) -> str:
    if rsi is None:
        return "N/A"
    if rsi < 20:
        return f"`{rsi}` 🔥 Extremely oversold"
    if rsi < RSI_OVERSOLD:
        return f"`{rsi}` ✅ Oversold — confirms dip"
    if rsi < 50:
        return f"`{rsi}` ⚠️ Weak — not yet oversold"
    return f"`{rsi}` 🔵 Neutral / strong — dip unconfirmed"


# ─────────────────────────────────────────────────────────────────────────────
# v3.2 FEATURE 2 — RELATIVE STRENGTH (Fund vs Nifty)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_nifty_mdd() -> float | None:
    """
    Fetches Nifty 50 MDD from its own 52-week high via yfinance.
    Returns float (0.0–1.0) or None on failure.
    """
    try:
        df = yf.download(NIFTY_TICKER, period="1y", progress=False)
        if df is None or df.empty:
            return None
        close    = df["Close"].squeeze().dropna()
        peak     = float(close.max())
        current  = float(close.iloc[-1])
        return (peak - current) / peak if peak > 0 else None
    except Exception as exc:
        log.warning(f"Nifty MDD fetch failed: {exc}")
        return None


def classify_relative_strength(fund_mdd: float, nifty_mdd: float | None) -> tuple[str, str]:
    """
    Compares fund drawdown against Nifty drawdown.
    Returns (classification, explanation) tuple.

    Systemic   — both fund and Nifty are down by similar magnitude
                 → broad market selling, deploy across all holdings
    Fund-Spec  — fund is down significantly more than Nifty
                 → something specific to this fund/sector, investigate first
    Fund-Strong— fund is barely down while Nifty is falling
                 → manager doing their job, not a buying opportunity
    """
    if nifty_mdd is None:
        return "Unknown", "Nifty data unavailable — cannot classify."

    diff = fund_mdd - nifty_mdd   # positive = fund fell more than Nifty

    if diff > 0.04:               # fund down >4% more than Nifty
        return (
            "🔍 Fund-Specific Dip",
            f"Fund fell *{diff:.1%} more* than Nifty ({nifty_mdd:.1%} Nifty DD). "
            "Deploy cautiously — investigate fund-level cause first."
        )
    elif abs(diff) <= 0.04:       # both moving roughly together
        return (
            "🌊 Systemic Dip",
            f"Fund and Nifty falling together (Nifty DD: {nifty_mdd:.1%}). "
            "Broad market sell-off — strong case for deployment."
        )
    else:                         # fund down less than Nifty
        return (
            "💪 Fund Holding Strong",
            f"Fund down less than Nifty (Nifty DD: {nifty_mdd:.1%}). "
            "Manager defending well — not a distressed entry point."
        )


# ─────────────────────────────────────────────────────────────────────────────
# v3.2 FEATURE 3 — PYRAMIDING ADVICE
# ─────────────────────────────────────────────────────────────────────────────

def pyramiding_advice(tier: int, dip_type: str) -> str:
    """
    Returns deploy % and rationale based on tier + dip classification.
    Adjusts downward for Fund-Specific dips (higher uncertainty).
    """
    base_pct, note = PYRAMID.get(tier, (10, ""))

    # Reduce conviction for fund-specific dips — unknown cause warrants caution
    if "Fund-Specific" in dip_type and base_pct > 10:
        adjusted = base_pct // 2
        return (
            f"Deploy *~{adjusted}%* of dry powder "
            f"_(halved from {base_pct}% — fund-specific dip, verify cause first)_\n"
            f"   {note}"
        )
    return f"Deploy *~{base_pct}%* of dry powder\n   {note}"


def classify_tier(mdd: float) -> tuple:
    """Returns (tier_int, signal_label, action_line) or (None, None, None)."""
    for threshold, level, label, action in ALERT_TIERS:
        if mdd >= threshold:
            return level, label, action
    return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# DEDUP + DAILY GATE
# ─────────────────────────────────────────────────────────────────────────────

def signal_hash(fund: str, tier, nav: float) -> str:
    raw = f"{today_str()}-{fund}-{tier}-{round(nav, 2)}"
    return hashlib.md5(raw.encode()).hexdigest()


def already_logged(h: str) -> bool:
    if not os.path.exists(SIGNAL_LOG):
        return False
    try:
        df = pd.read_csv(SIGNAL_LOG)
        return "hash" in df.columns and h in df["hash"].values
    except Exception:
        return False


def log_signal(row: dict):
    pd.DataFrame([row]).to_csv(
        SIGNAL_LOG, mode="a",
        header=not os.path.exists(SIGNAL_LOG),
        index=False,
    )


def mark_processed(fund: str):
    with open(PROCESSED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{today_str()}|{fund}\n")


def already_processed_today(fund: str) -> bool:
    if not os.path.exists(PROCESSED_LOG):
        return False
    target = f"{today_str()}|{fund}"
    with open(PROCESSED_LOG, encoding="utf-8") as f:
        return any(line.strip() == target for line in f)


# ─────────────────────────────────────────────────────────────────────────────
# CORE FUND PROCESSING
# ─────────────────────────────────────────────────────────────────────────────

def process_fund(code: str, cfg: dict, vix: float, amfi_navs: dict):
    name  = cfg["name"]
    emoji = cfg["emoji"]
    log.info(f"{'─'*52}")
    log.info(f"Processing: {emoji} {name}  [{code}]")

    if already_processed_today(name):
        log.info("  Already processed today — skipping")
        return

    history = fetch_nav_history(code)

    # Current NAV: AMFI direct (today's official) → mfapi latest (fallback)
    if code in amfi_navs:
        curr_nav = amfi_navs[code]
        log.info(f"  Current NAV (AMFI direct):    ₹{curr_nav:.4f}")
    else:
        curr_nav = float(history.iloc[-1])
        log.warning(f"  Current NAV (mfapi fallback): ₹{curr_nav:.4f}")

    peak_nav, peak_date = refresh_watermark(code, history)
    log.info(f"  52-Week High (watermark DB):  ₹{peak_nav:.4f} on {peak_date}")

    mdd                  = compute_mdd(curr_nav, peak_nav)
    tier, signal, action = classify_tier(mdd)
    vix_high             = vix >= VIX_HIGH

    # ── v3.2: RSI-14 ──────────────────────────────────────────────────────────
    rsi = compute_rsi(history)
    oversold = rsi is not None and rsi < RSI_OVERSOLD

    # ── v3.2: Relative Strength ───────────────────────────────────────────────
    nifty_mdd                = fetch_nifty_mdd()
    rel_strength, rel_detail = classify_relative_strength(mdd, nifty_mdd)

    log.info(
        f"  MDD: {mdd:.2%}  |  Signal: {signal or 'WAIT'}  |  "
        f"RSI-14: {rsi}  |  {rel_strength}  |  "
        f"VIX: {vix:.2f} {'⬆ HIGH' if vix_high else ''}"
    )

    # Audit log — written every run for every fund
    h = signal_hash(name, tier, curr_nav)
    if not already_logged(h):
        log_signal({
            "date":          today_str(),
            "fund":          name,
            "scheme":        code,
            "nav":           round(curr_nav, 4),
            "peak_nav":      round(peak_nav, 4),
            "peak_date":     peak_date,
            "mdd_pct":       round(mdd * 100, 2),
            "tier":          tier,
            "signal":        signal or "WAIT",
            "rsi_14":        rsi,
            "oversold":      oversold,
            "nifty_mdd_pct": round(nifty_mdd * 100, 2) if nifty_mdd else None,
            "rel_strength":  rel_strength,
            "vix":           round(vix, 2),
            "vix_high":      vix_high,
            "hash":          h,
        })

    if tier is not None:
        if is_on_cooldown(code, tier):
            log.info(f"  {signal} on {COOLDOWN_HOURS}h cooldown — suppressing alert")
        else:
            # ── v3.2: Pyramiding advice ────────────────────────────────────────
            pyramid_line = pyramiding_advice(tier, rel_strength)

            # VIX context
            vix_line = (
                f"India VIX: `{vix:.2f}` 📈 High volatility — stronger entry"
                if vix_high else
                f"India VIX: `{vix:.2f}` — moderate volatility"
            )

            # RSI confirmation line
            rsi_line = f"RSI-14:    {rsi_label(rsi)}"

            # Relative strength line
            nifty_dd_str = f"{nifty_mdd:.2%}" if nifty_mdd else "N/A"
            rel_line = f"Dip Type:  *{rel_strength}*"

            sep = "━" * 30
            msg = (
                f"{signal}\n"
                f"{sep}\n"
                f"Fund:  *{emoji} {name}*\n\n"
                f"NAV Now:     ₹`{curr_nav:.4f}`\n"
                f"52-Wk High:  ₹`{peak_nav:.4f}` _({peak_date})_\n"
                f"Drawdown:    *{mdd:.2%}*  |  Nifty DD: `{nifty_dd_str}`\n\n"
                f"{rel_line}\n"
                f"_{rel_detail}_\n\n"
                f"{rsi_line}\n"
                f"{vix_line}\n\n"
                f"📌 *Action:* {action}\n"
                f"💰 *Deploy:* {pyramid_line}\n"
                f"{sep}\n"
                f"🗓 {today_str()}"
            )
            if send_alert(msg):
                set_cooldown(code, tier)
    else:
        log.info(f"  Signal: WAIT  (MDD {mdd:.2%} — below accumulate floor)")

    mark_processed(name)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("═" * 52)
    log.info("  Dip Alert Pipeline v3.2 — Started")
    log.info("═" * 52)

    init_db()

    # VIX hard abort — never proceed with an assumed/default VIX
    try:
        vix = fetch_vix()
        log.info(f"India VIX: {vix:.2f}")
    except RuntimeError as exc:
        log.error(str(exc))
        send_alert(f"*Pipeline aborted* — VIX fetch failed.\n`{exc}`", is_dev=True)
        sys.exit(1)

    amfi_navs = fetch_amfi_navs()

    for code, cfg in FUNDS.items():
        try:
            process_fund(code, cfg, vix, amfi_navs)
        except Exception as exc:
            log.exception(f"Fund failed: {cfg['name']} | {exc}")
            send_alert(
                f"*Fund processing failed*\nFund: {cfg['name']} `[{code}]`\n`{exc}`",
                is_dev=True,
            )

    log.info("═" * 52)
    log.info("  Dip Alert Pipeline v3.2 — Completed")
    log.info("═" * 52)


if __name__ == "__main__":
    main()
