# -*- coding: utf-8 -*-

"""
Dip Alert System - Production Hardened
Bug fixes applied:
  1. Telegram alerts added (was completely silent)
  2. Hash now includes date (same signal on new day was skipped)
  3. VIX filter applied to Strong Buy / Buy signals (false positives fixed)
  4. Dedup fixed: df["hash"].values instead of df.get("hash", []).values
  5. VIX fallback=18 replaced with hard abort (False Buy on API fail prevented)
  6. drawdown() zero-guard added (ZeroDivisionError on bad data prevented)
  7. AMFI direct NAV fetch added (replaces stale mfapi as primary source)
  8. nav_processed gate added (prevents multiple runs per day)

Production fixes (v2):
  FIX-A. Telegram env vars corrected: reads TELEGRAM_BOT_TOKEN_MO/PP and
          TELEGRAM_CHAT_ID_MO/PP to match GitHub Actions secrets — previously
          read generic TELEGRAM_BOT_TOKEN which is never set → silent failure.
  FIX-B. SIGNAL_LOG renamed signals.csv → signal_log.csv to match what the
          workflow actually commits — previously signals.csv was never committed
          and was lost on every fresh checkout.
  FIX-C. PROCESSED_LOG renamed nav_processed.csv → nav_processed.txt to match
          the file the workflow commits — previously the gate file was lost on
          every fresh checkout, causing the daily-run gate to never work.
  FIX-D. nav_processed.txt format changed from CSV to a plain date+fund store
          consistent with the existing file format on disk.
"""

import os
import sys
import io
import logging
import hashlib
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd
import yfinance as yf

# -------------------- STDOUT FIX --------------------
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# -------------------- LOGGING -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# -------------------- RETRY SESSION -----------------
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session():
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    s = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = create_session()

# -------------------- CONFIG ------------------------
# FIX-A: per-fund Telegram tokens match GitHub Actions secrets
FUNDS = {
    "Motilal Oswal Midcap Fund": {
        "scheme_code": "127042",
        "amfi_code":   "127042",
        "telegram_token":   os.environ.get("TELEGRAM_BOT_TOKEN_MO", ""),
        "telegram_chat_id": os.environ.get("TELEGRAM_CHAT_ID_MO", ""),
    },
    "Parag Parikh Flexi Cap Fund": {
        "scheme_code": "122639",
        "amfi_code":   "122639",
        "telegram_token":   os.environ.get("TELEGRAM_BOT_TOKEN_PP", ""),
        "telegram_chat_id": os.environ.get("TELEGRAM_CHAT_ID_PP", ""),
    },
}

NIFTY50   = "^NSEI"
INDIA_VIX = "^INDIAVIX"

# FIX-B: filename matches what the workflow git-adds and commits
SIGNAL_LOG    = "signal_log.csv"

# FIX-C: filename matches what the workflow git-adds and commits
PROCESSED_LOG = "nav_processed.txt"

# -------------------- HELPERS -----------------------

def ist_now():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def today():
    return ist_now().strftime("%Y-%m-%d")

# -------------------- BUG1 FIX: TELEGRAM -----------

def send_telegram(message: str, token: str, chat_id: str) -> bool:
    """Send a Telegram message for a specific fund. Returns True on success."""
    if not token or not chat_id:
        log.warning("Telegram not configured for this fund (token/chat_id missing)")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }
    try:
        resp = SESSION.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Telegram alert sent")
        return True
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False

# -------------------- DATA --------------------------

def fetch_json(url):
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

# -------------------- BUG7 FIX: AMFI DIRECT NAV ----

AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

def fetch_amfi_navs() -> dict:
    """
    Download AMFI's allnavs.txt and return {scheme_code: nav} mapping.
    Returns empty dict on failure so caller can fall back to mfapi.
    """
    try:
        resp = SESSION.get(AMFI_NAV_URL, timeout=30)
        resp.raise_for_status()
        navs = {}
        for line in resp.text.splitlines():
            parts = line.split(";")
            if len(parts) >= 5:
                code = parts[0].strip()
                try:
                    nav = float(parts[4].strip())
                    navs[code] = nav
                except ValueError:
                    pass
        log.info(f"AMFI NAV file loaded: {len(navs)} schemes")
        return navs
    except Exception as e:
        log.warning(f"AMFI NAV fetch failed: {e}")
        return {}

def fetch_nav_history(scheme_code: str) -> pd.Series:
    """Historical NAV series from mfapi (used for peak/drawdown calculation)."""
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    data = fetch_json(url)["data"]
    series = pd.Series({
        datetime.strptime(d["date"], "%d-%m-%Y").date(): float(d["nav"])
        for d in data
    }).sort_index()
    return series

def fetch_yf(ticker):
    df = yf.download(ticker, period="1y", progress=False)
    if df.empty:
        raise ValueError(f"No data for {ticker}")
    return df["Close"].squeeze()

# -------------------- BUG5 FIX: VIX ABORT ----------

def fetch_vix() -> float:
    """
    Fetch India VIX. Raises RuntimeError on failure — callers must handle this
    and abort rather than proceeding with an assumed VIX value.
    """
    try:
        vix = float(fetch_yf(INDIA_VIX).iloc[-1])
        return vix
    except Exception as e:
        raise RuntimeError(f"VIX fetch failed, aborting to avoid false signals: {e}") from e

# -------------------- VALIDATION --------------------

def validate_series(s, name):
    if s.empty:
        raise ValueError(f"{name}: empty")
    if s.isna().any():
        raise ValueError(f"{name}: NaN present")
    if len(s) < 30:
        raise ValueError(f"{name}: insufficient data ({len(s)} rows)")

# -------------------- LOGIC -------------------------

def rolling_peak(s, n):
    return float(s.iloc[-n:].max())

# -------------------- BUG6 FIX: ZERO GUARD ----------

def drawdown(curr: float, peak: float) -> float:
    if peak == 0.0:
        log.warning("Peak NAV is zero — cannot compute drawdown, returning 0.0")
        return 0.0
    return (peak - curr) / peak

# -------------------- BUG3 FIX: VIX FILTER ----------

VIX_THRESHOLDS = {
    "Aggressive Buy": 22,
    "Strong Buy":     18,
    "Buy":            15,
}

def generate_signal(dd: float, vix: float) -> str:
    if dd > 0.20 and vix > VIX_THRESHOLDS["Aggressive Buy"]:
        return "Aggressive Buy"
    if dd > 0.15 and vix > VIX_THRESHOLDS["Strong Buy"]:
        return "Strong Buy"
    if dd > 0.10 and vix > VIX_THRESHOLDS["Buy"]:
        return "Buy"
    return "None"

# -------------------- BUG2 FIX: DATE IN HASH --------

def signal_hash(fund: str, signal: str, nav: float) -> str:
    raw = f"{today()}-{fund}-{signal}-{round(nav, 2)}"
    return hashlib.md5(raw.encode()).hexdigest()

# -------------------- BUG4 FIX: DEDUP ---------------

def already_logged(hash_val: str) -> bool:
    if not os.path.exists(SIGNAL_LOG):
        return False
    df = pd.read_csv(SIGNAL_LOG)
    if "hash" not in df.columns:
        return False
    return hash_val in df["hash"].values

def log_signal(row: dict):
    df = pd.DataFrame([row])
    header = not os.path.exists(SIGNAL_LOG)
    df.to_csv(SIGNAL_LOG, mode="a", header=header, index=False)

# -------------------- BUG8 FIX: DAILY GATE ----------
# FIX-C/D: nav_processed.txt stores "date|fund" lines — one per processed fund.
# Matches the file the workflow commits, so the gate survives across checkouts.

def mark_processed(fund: str):
    with open(PROCESSED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{today()}|{fund}\n")

def already_processed_today(fund: str) -> bool:
    if not os.path.exists(PROCESSED_LOG):
        return False
    with open(PROCESSED_LOG, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line == f"{today()}|{fund}":
                return True
    return False

# -------------------- PROCESS -----------------------

def process_fund(name: str, cfg: dict, vix: float, amfi_navs: dict):
    log.info(f"Processing {name}")

    if already_processed_today(name):
        log.info(f"{name}: already processed today, skipping")
        return

    nav_history = fetch_nav_history(cfg["scheme_code"])
    validate_series(nav_history, name)

    amfi_code = cfg.get("amfi_code", cfg["scheme_code"])
    if amfi_code in amfi_navs:
        curr = amfi_navs[amfi_code]
        log.info(f"{name}: using AMFI direct NAV={curr:.4f}")
    else:
        curr = float(nav_history.iloc[-1])
        log.warning(f"{name}: AMFI NAV not found, falling back to mfapi NAV={curr:.4f}")

    peak = rolling_peak(nav_history, 120)
    dd   = drawdown(curr, peak)

    signal = generate_signal(dd, vix)

    h = signal_hash(name, signal, curr)
    if already_logged(h):
        log.info(f"{name}: duplicate signal hash, skipping")
        mark_processed(name)
        return

    log.info(f"{name} | NAV={curr:.4f} | Peak={peak:.4f} | DD={dd:.2%} | VIX={vix:.2f} | Signal={signal}")

    log_signal({
        "date":   today(),
        "fund":   name,
        "nav":    curr,
        "peak":   round(peak, 4),
        "dd":     round(dd, 4),
        "vix":    round(vix, 2),
        "signal": signal,
        "hash":   h,
    })

    # FIX-A: pass per-fund token and chat_id
    if signal != "None":
        msg = (
            f"*Dip Alert* 🔔\n"
            f"Fund: {name}\n"
            f"Signal: *{signal}*\n"
            f"NAV: ₹{curr:.4f} (Peak: ₹{peak:.4f})\n"
            f"Drawdown: {dd:.2%}\n"
            f"India VIX: {vix:.2f}\n"
            f"Date: {today()}"
        )
        send_telegram(msg, cfg["telegram_token"], cfg["telegram_chat_id"])

    mark_processed(name)

# -------------------- MAIN --------------------------

def main():
    log.info("=== Dip Alert Started ===")

    try:
        vix = fetch_vix()
        log.info(f"VIX={vix:.2f}")
    except RuntimeError as e:
        log.error(str(e))
        log.error("Aborting run to prevent false signals.")
        return

    amfi_navs = fetch_amfi_navs()

    for name, cfg in FUNDS.items():
        try:
            process_fund(name, cfg, vix, amfi_navs)
        except Exception as e:
            log.exception(f"Fund failed: {name} | {e}")

    log.info("=== Completed ===")

# -------------------- ENTRY -------------------------

if __name__ == "__main__":
    main()
