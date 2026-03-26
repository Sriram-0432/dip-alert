# -*- coding: utf-8 -*-

"""
Dip Alert System - Production Hardened
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
FUNDS = {
    "Motilal Oswal Midcap Fund": {
        "scheme_code": "127042",
    },
    "Parag Parikh Flexi Cap Fund": {
        "scheme_code": "122639",
    },
}

NIFTY50 = "^NSEI"
INDIA_VIX = "^INDIAVIX"

SIGNAL_LOG = "signals.csv"

# -------------------- HELPERS -----------------------

def ist_now():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def today():
    return ist_now().strftime("%Y-%m-%d")

# -------------------- DATA --------------------------

def fetch_json(url):
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_nav_history(scheme_code):
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

def fetch_vix():
    try:
        return float(fetch_yf(INDIA_VIX).iloc[-1])
    except Exception:
        log.warning("VIX failed, fallback=18")
        return 18.0

# -------------------- VALIDATION --------------------

def validate_series(s, name):
    if s.empty:
        raise ValueError(f"{name}: empty")
    if s.isna().any():
        raise ValueError(f"{name}: NaN present")
    if len(s) < 30:
        raise ValueError(f"{name}: insufficient data")

# -------------------- LOGIC -------------------------

def rolling_peak(s, n):
    return float(s.iloc[-n:].max())

def drawdown(curr, peak):
    return (peak - curr) / peak

def generate_signal(dd, vix):
    if dd > 0.20 and vix > 22:
        return "Aggressive Buy"
    if dd > 0.15:
        return "Strong Buy"
    if dd > 0.10:
        return "Buy"
    return "None"

# -------------------- DEDUPE ------------------------

def signal_hash(fund, signal, nav):
    raw = f"{fund}-{signal}-{round(nav,2)}"
    return hashlib.md5(raw.encode()).hexdigest()

def already_logged(hash_val):
    if not os.path.exists(SIGNAL_LOG):
        return False
    df = pd.read_csv(SIGNAL_LOG)
    return hash_val in df.get("hash", []).values

def log_signal(row):
    df = pd.DataFrame([row])
    header = not os.path.exists(SIGNAL_LOG)
    df.to_csv(SIGNAL_LOG, mode="a", header=header, index=False)

# -------------------- PROCESS -----------------------

def process_fund(name, cfg, vix):
    log.info(f"Processing {name}")

    nav = fetch_nav_history(cfg["scheme_code"])
    validate_series(nav, name)

    curr = float(nav.iloc[-1])
    peak = rolling_peak(nav, 120)
    dd = drawdown(curr, peak)

    signal = generate_signal(dd, vix)

    h = signal_hash(name, signal, curr)
    if already_logged(h):
        log.info("Duplicate signal skipped")
        return

    log.info(f"{name} | NAV={curr:.2f} | DD={dd:.2%} | Signal={signal}")

    log_signal({
        "date": today(),
        "fund": name,
        "nav": curr,
        "dd": round(dd, 4),
        "signal": signal,
        "hash": h
    })

# -------------------- MAIN --------------------------

def main():
    log.info("=== Dip Alert Started ===")

    try:
        vix = fetch_vix()
        log.info(f"VIX={vix}")
    except Exception as e:
        log.error(f"VIX fatal: {e}")
        return

    for name, cfg in FUNDS.items():
        try:
            process_fund(name, cfg, vix)
        except Exception as e:
            log.exception(f"Fund failed: {name} | {e}")

    log.info("=== Completed ===")

# -------------------- ENTRY -------------------------

if __name__ == "__main__":
    main()