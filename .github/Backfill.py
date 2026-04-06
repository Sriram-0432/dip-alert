# -*- coding: utf-8 -*-

"""
Backfill Script — Dip Alert System
===================================
Reconstructs missed signals from the last logged date up to yesterday,
using only data that was available on each historical date (no lookahead).

Usage
-----
  # Auto-detect start from signals.csv last entry:
  python backfill.py

  # Explicit start date:
  python backfill.py --start 2024-10-01

  # Explicit range:
  python backfill.py --start 2024-10-01 --end 2024-12-31

  # Dry run (no writes, no Telegram):
  python backfill.py --dry-run

GitHub Actions
--------------
  Triggered automatically on first push or manually via workflow_dispatch.
  Reads START_DATE from env / secret if set; otherwise auto-detects.

Design rules
------------
  - Point-in-time safe: peak uses only history[:date], no future leakage
  - Skips weekends and dates with no NAV (market holidays / data gaps)
  - Skips dates already present in signals.csv (idempotent — safe to re-run)
  - Does NOT write to nav_processed.csv (that gate is for the live daily script)
  - Does NOT send Telegram alerts (historical noise is not useful)
  - Mirrors dip_alert.py constants exactly: VIX_THRESHOLDS, PEAK_WINDOW, signal logic
"""

import os
import sys
import io
import logging
import hashlib
import argparse
from datetime import datetime, timezone, timedelta, date

import requests
import pandas as pd
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
def create_session():
    retry = Retry(total=5, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504])
    s = requests.Session()
    a = HTTPAdapter(max_retries=retry)
    s.mount("https://", a)
    s.mount("http://", a)
    return s

SESSION = create_session()

# -------------------- CONFIG (mirrors dip_alert.py) --
FUNDS = {
    "Motilal Oswal Midcap Fund": {
        "scheme_code": "127042",
    },
    "Parag Parikh Flexi Cap Fund": {
        "scheme_code": "122639",
    },
}

INDIA_VIX   = "^INDIAVIX"
PEAK_WINDOW = 120          # rolling days for peak — must match dip_alert.py
SIGNAL_LOG  = "signals.csv"

VIX_THRESHOLDS = {
    "Aggressive Buy": 22,
    "Strong Buy":     18,
    "Buy":            15,
}

# -------------------- HELPERS -----------------------

def ist_today() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()

def date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")

# -------------------- DATA --------------------------

def fetch_nav_history(scheme_code: str) -> pd.Series:
    """Full NAV history from mfapi, sorted ascending by date."""
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()["data"]
    series = pd.Series({
        datetime.strptime(d["date"], "%d-%m-%Y").date(): float(d["nav"])
        for d in data
    }).sort_index()
    return series

def fetch_vix_history() -> pd.Series:
    """
    Full 2-year VIX history from yfinance.
    Returns a Series with plain date (not datetime) index.
    """
    df = yf.download(INDIA_VIX, period="2y", progress=False)
    if df.empty:
        raise RuntimeError("VIX history fetch returned empty DataFrame")
    s = df["Close"].squeeze()
    s.index = pd.to_datetime(s.index).date
    return s.sort_index()

# -------------------- LOGIC (mirrors dip_alert.py) ---

def rolling_peak(s: pd.Series, n: int) -> float:
    return float(s.iloc[-n:].max())

def drawdown(curr: float, peak: float) -> float:
    if peak == 0.0:
        return 0.0
    return (peak - curr) / peak

def generate_signal(dd: float, vix: float) -> str:
    if dd > 0.20 and vix > VIX_THRESHOLDS["Aggressive Buy"]:
        return "Aggressive Buy"
    if dd > 0.15 and vix > VIX_THRESHOLDS["Strong Buy"]:
        return "Strong Buy"
    if dd > 0.10 and vix > VIX_THRESHOLDS["Buy"]:
        return "Buy"
    return "None"

def signal_hash(d: date, fund: str, signal: str, nav: float) -> str:
    raw = f"{date_str(d)}-{fund}-{signal}-{round(nav, 2)}"
    return hashlib.md5(raw.encode()).hexdigest()

# -------------------- DEDUP -------------------------

def load_logged_hashes() -> set:
    """Return set of hashes already in signals.csv."""
    if not os.path.exists(SIGNAL_LOG):
        return set()
    df = pd.read_csv(SIGNAL_LOG)
    if "hash" not in df.columns:
        return set()
    return set(df["hash"].values)

def load_logged_dates_per_fund() -> dict:
    """Return {fund: set(date_str)} of already-logged dates."""
    if not os.path.exists(SIGNAL_LOG):
        return {}
    df = pd.read_csv(SIGNAL_LOG)
    if "date" not in df.columns or "fund" not in df.columns:
        return {}
    result = {}
    for fund, grp in df.groupby("fund"):
        result[fund] = set(grp["date"].astype(str).values)
    return result

def last_logged_date() -> date | None:
    """Return the most recent date in signals.csv, or None if empty/missing."""
    if not os.path.exists(SIGNAL_LOG):
        return None
    df = pd.read_csv(SIGNAL_LOG)
    if df.empty or "date" not in df.columns:
        return None
    return pd.to_datetime(df["date"]).max().date()

def log_signal(row: dict, dry_run: bool):
    if dry_run:
        log.info(f"  [DRY-RUN] would write: {row}")
        return
    df_row = pd.DataFrame([row])
    header = not os.path.exists(SIGNAL_LOG)
    df_row.to_csv(SIGNAL_LOG, mode="a", header=header, index=False)

# -------------------- DATE RANGE --------------------

def parse_start(arg_start: str | None) -> date:
    """
    Determine backfill start date by priority:
      1. --start CLI argument
      2. START_DATE environment variable (set in GitHub Actions)
      3. Day after last entry in signals.csv
      4. 1 year ago as absolute fallback
    """
    if arg_start:
        return datetime.strptime(arg_start, "%Y-%m-%d").date()

    env_start = os.environ.get("START_DATE", "").strip()
    if env_start:
        log.info(f"Using START_DATE from environment: {env_start}")
        return datetime.strptime(env_start, "%Y-%m-%d").date()

    last = last_logged_date()
    if last:
        start = last + timedelta(days=1)
        log.info(f"Auto-detected start: {date_str(start)} (day after last log: {date_str(last)})")
        return start

    fallback = ist_today() - timedelta(days=365)
    log.warning(f"No signals.csv found — defaulting to 1 year ago: {date_str(fallback)}")
    return fallback

def business_dates(start: date, end: date) -> list:
    """Return Mon–Fri dates between start and end inclusive."""
    dates = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            dates.append(cur)
        cur += timedelta(days=1)
    return dates

# -------------------- BACKFILL ----------------------

def backfill_fund(
    name: str,
    cfg: dict,
    vix_history: pd.Series,
    logged_dates: set,
    logged_hashes: set,
    date_range: list,
    dry_run: bool,
) -> int:
    """
    Walk date_range for one fund.
    Computes point-in-time signals and writes missing rows.
    Returns count of rows written (or that would be written in dry-run).
    """
    log.info(f"--- Backfilling: {name} ---")

    nav_full = fetch_nav_history(cfg["scheme_code"])
    if nav_full.empty or len(nav_full) < PEAK_WINDOW:
        log.error(f"{name}: insufficient NAV history ({len(nav_full)} rows), skipping")
        return 0

    written = 0

    for d in date_range:
        date_s = date_str(d)

        # Skip if already logged for this fund+date
        if date_s in logged_dates:
            log.debug(f"  {date_s}: already in signals.csv, skipping")
            continue

        # Point-in-time NAV slice — no future data
        nav_slice = nav_full[nav_full.index <= d]

        # No NAV on this date = market holiday or data gap
        if nav_slice.empty or nav_slice.index[-1] != d:
            log.debug(f"  {date_s}: no NAV (holiday/gap), skipping")
            continue

        # Need enough history for a meaningful peak
        if len(nav_slice) < PEAK_WINDOW:
            log.debug(f"  {date_s}: only {len(nav_slice)} NAV rows, need {PEAK_WINDOW}, skipping")
            continue

        curr = float(nav_slice.iloc[-1])
        peak = rolling_peak(nav_slice, PEAK_WINDOW)
        dd   = drawdown(curr, peak)

        # VIX — closest available date on or before d
        vix_slice = vix_history[vix_history.index <= d]
        if vix_slice.empty:
            log.warning(f"  {date_s}: no VIX data available, skipping")
            continue
        vix = float(vix_slice.iloc[-1])

        signal = generate_signal(dd, vix)
        h      = signal_hash(d, name, signal, curr)

        # Hash dedup — safety net for partial re-runs
        if h in logged_hashes:
            log.debug(f"  {date_s}: hash already logged, skipping")
            continue

        log.info(
            f"  {date_s} | NAV={curr:.4f} | Peak={peak:.4f} | "
            f"DD={dd:.2%} | VIX={vix:.2f} | Signal={signal}"
        )

        row = {
            "date":   date_s,
            "fund":   name,
            "nav":    curr,
            "peak":   round(peak, 4),
            "dd":     round(dd, 4),
            "vix":    round(vix, 2),
            "signal": signal,
            "hash":   h,
        }
        log_signal(row, dry_run)

        if not dry_run:
            logged_hashes.add(h)
            logged_dates.add(date_s)

        written += 1

    log.info(f"  {name}: {written} rows {'would be ' if dry_run else ''}written")
    return written

# -------------------- MAIN --------------------------

def main():
    parser = argparse.ArgumentParser(description="Backfill missing dip-alert signals")
    parser.add_argument("--start",   help="Start date YYYY-MM-DD (default: auto-detect)")
    parser.add_argument("--end",     help="End date YYYY-MM-DD (default: yesterday IST)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview rows without writing files or sending alerts")
    args = parser.parse_args()

    dry_run = args.dry_run
    if dry_run:
        log.info("=== DRY RUN — no files will be written ===")

    # ---- Resolve date range ----
    start = parse_start(args.start)
    end   = (
        datetime.strptime(args.end, "%Y-%m-%d").date()
        if args.end
        else ist_today() - timedelta(days=1)
    )

    if start > end:
        log.info(f"Nothing to backfill: start={date_str(start)} end={date_str(end)}")
        return

    log.info(f"=== Backfill range: {date_str(start)} → {date_str(end)} ===")

    dates = business_dates(start, end)
    log.info(f"Business days in range: {len(dates)}")
    if not dates:
        log.info("No business days — done.")
        return

    # ---- Load existing log state ----
    logged_hashes        = load_logged_hashes()
    logged_dates_by_fund = load_logged_dates_per_fund()

    # ---- Fetch VIX history once for all funds ----
    log.info("Fetching VIX history (2 years)...")
    try:
        vix_history = fetch_vix_history()
        log.info(
            f"VIX loaded: {len(vix_history)} days | "
            f"{date_str(vix_history.index[0])} → {date_str(vix_history.index[-1])}"
        )
    except Exception as e:
        log.error(f"VIX history unavailable: {e} — aborting")
        return

    # ---- Backfill each fund ----
    total_written = 0
    for name, cfg in FUNDS.items():
        try:
            written = backfill_fund(
                name, cfg, vix_history,
                logged_dates=logged_dates_by_fund.get(name, set()),
                logged_hashes=logged_hashes,
                date_range=dates,
                dry_run=dry_run,
            )
            total_written += written
        except Exception as e:
            log.exception(f"Backfill failed for {name}: {e}")

    # ---- Summary ----
    log.info("=" * 52)
    if dry_run:
        log.info(f"DRY RUN complete — {total_written} rows would have been written to {SIGNAL_LOG}")
    elif total_written == 0:
        log.info(f"signals.csv is already up to date — nothing was missing.")
    else:
        log.info(f"Backfill complete — {total_written} rows written to {SIGNAL_LOG}")

if __name__ == "__main__":
    main()
