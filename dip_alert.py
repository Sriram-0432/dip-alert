# -*- coding: utf-8 -*-
"""
Mutual Fund Dip Alert System
Monitors: Motilal Oswal Midcap Fund, Parag Parikh Flexi Cap Fund
Run daily after market close (e.g. 4:00 PM IST via cron or GitHub Actions)
"""

import sys
import io
import os
import logging
import smtplib
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
import pandas as pd
import yfinance as yf

# ─── CONFIGURATION ──────────────────────────────────────────────────────────

FUNDS = {
    "Motilal Oswal Midcap Fund": {
        "mfapi_id": "127042",   # Direct Plan Growth
        "short_name": "MO_Midcap",
    },
    "Parag Parikh Flexi Cap Fund": {
        "mfapi_id": "122639",   # Direct Plan Growth - verify this
        "short_name": "PP_FlexiCap",
    },
}

NIFTY_MIDCAP_TICKER = "^NSEI"      # Yahoo Finance: Nifty 50 proxy; swap for actual Midcap 150
NIFTY_MIDCAP_150    = "NIFTYMIDCAP150.NS"  # try this first
INDIA_VIX_TICKER    = "^INDIAVIX"

SIGNAL_RULES = [
    {"label": "Aggressive Buy", "drawdown": 0.20, "vix": 25, "allocation": "Large"},
    {"label": "Strong Buy",     "drawdown": 0.15, "vix": 20, "allocation": "Medium"},
    {"label": "Buy",            "drawdown": 0.10, "vix": 18, "allocation": "Small"},
    {"label": "Alert",          "drawdown": 0.05, "vix": 15, "allocation": None},
]

MOMENTUM_LOOKBACK_DAYS = 10   # trading days
MOMENTUM_DROP_THRESHOLD = 0.05  # 5%

PEAK_WINDOWS = {
    "3m": 60,   # trading days
    "6m": 120,
}

# ─── NOTIFICATIONS ───────────────────────────────────────────────────────────
# Set these via environment variables — never hardcode credentials.

EMAIL_ENABLED   = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SENDER    = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD  = os.getenv("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "")
SMTP_HOST       = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT       = int(os.getenv("SMTP_PORT", "587"))

TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "false").lower() == "true"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

LOG_FILE = os.getenv("LOG_FILE", "dip_alerts.log")

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(stream=sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─── DATA FETCHING ────────────────────────────────────────────────────────────

def fetch_nav_history(fund_name: str, fund_cfg: dict) -> pd.Series:
    """Fetch NAV history from mfapi.in — returns a date-indexed Series."""
    url = f"https://api.mfapi.in/mf/{fund_cfg['mfapi_id']}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    records = {
        datetime.strptime(d["date"], "%d-%m-%Y").date(): float(d["nav"])
        for d in data
    }
    series = pd.Series(records).sort_index()
    log.info(f"  {fund_name}: fetched {len(series)} NAV records, latest = {series.iloc[-1]:.4f}")
    return series


def fetch_yf(ticker: str, period: str = "6mo") -> pd.Series:
    """Fetch closing price series from Yahoo Finance."""
    df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
    if df.empty:
        raise ValueError(f"No data returned for ticker {ticker}")
    series = df["Close"].squeeze()
    series.index = pd.to_datetime(series.index).date
    log.info(f"  {ticker}: {len(series)} rows, latest = {series.iloc[-1]:.2f}")
    return series


def fetch_india_vix() -> float:
    """Return the latest India VIX value."""
    series = fetch_yf(INDIA_VIX_TICKER, period="5d")
    return round(float(series.iloc[-1]), 2)


def fetch_midcap150() -> pd.Series:
    """Return Nifty Midcap 150 closing price series."""
    try:
        return fetch_yf(NIFTY_MIDCAP_150, period="3mo")
    except Exception:
        log.warning("  Midcap 150 ticker failed, falling back to Nifty 50")
        return fetch_yf(NIFTY_MIDCAP_TICKER, period="3mo")

# ─── CALCULATIONS ─────────────────────────────────────────────────────────────

def rolling_peak(nav: pd.Series, window_days: int) -> float:
    """Highest NAV in the last `window_days` trading days."""
    return float(nav.iloc[-window_days:].max())


def drawdown(current: float, peak: float) -> float:
    return (peak - current) / peak if peak > 0 else 0.0


def index_momentum_is_negative(index_series: pd.Series) -> bool:
    """True if index fell more than 5% in last 10 trading days."""
    if len(index_series) < MOMENTUM_LOOKBACK_DAYS + 1:
        log.warning("  Not enough index data for momentum — defaulting to False")
        return False
    recent = index_series.iloc[-MOMENTUM_LOOKBACK_DAYS:]
    start, end = float(recent.iloc[0]), float(recent.iloc[-1])
    change = (end - start) / start
    log.info(f"  Index momentum ({MOMENTUM_LOOKBACK_DAYS}d): {change:.2%}")
    return change < -MOMENTUM_DROP_THRESHOLD

# ─── SIGNAL LOGIC ─────────────────────────────────────────────────────────────

def evaluate_signal(dd_3m: float, dd_6m: float, vix: float, momentum_negative: bool) -> dict | None:
    """Return the highest-priority signal that fires, or None."""
    if not momentum_negative:
        return None  # only fire during corrections

    # Use the higher of 3-month and 6-month drawdown (more conservative)
    effective_dd = max(dd_3m, dd_6m)

    for rule in SIGNAL_RULES:
        if effective_dd >= rule["drawdown"] and vix >= rule["vix"]:
            return {**rule, "drawdown_used": effective_dd, "dd_3m": dd_3m, "dd_6m": dd_6m}
    return None

# ─── NOTIFICATIONS ────────────────────────────────────────────────────────────

def build_message(fund_name: str, signal: dict, current_nav: float, vix: float) -> str:
    label = signal["label"]
    dd_pct = f"{signal['drawdown_used']:.1%}"
    alloc = signal.get("allocation") or "Watch"

    # Signal strength bar — green filled circles
    strength_map = {
        "Alert":          "🟢⚪⚪⚪ Alert",
        "Buy":            "🟢🟢⚪⚪ Buy",
        "Strong Buy":     "🟢🟢🟢⚪ Strong Buy",
        "Aggressive Buy": "🟢🟢🟢🟢 Aggressive Buy",
    }
    strength = strength_map.get(label, label)

    # VIX condition label
    if vix < 15:
        vix_label = "Calm"
    elif vix < 18:
        vix_label = "⚠️ Moderate stress"
    elif vix < 22:
        vix_label = "⚠️ Elevated stress"
    else:
        vix_label = "🚨 Panic zone"

    return (
        f"📊 {label} — {fund_name}\n"
        f"{'─'*35}\n"
        f"Signal      : {strength}\n"
        f"Current NAV : ₹{current_nav:.4f}\n"
        f"Drawdown    : {dd_pct} from recent peak\n"
        f"India VIX   : {vix:.2f} {vix_label}\n"
        f"Momentum    : ↓ Correction phase\n"
        f"Suggested   : {alloc} investment\n"
        f"{'─'*35}\n"
        f"Date        : {(datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%d %b %Y %H:%M IST')}"
    )


def send_email(subject: str, body: str) -> None:
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECIPIENT
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
    log.info("  Email sent.")


def send_telegram(text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    resp.raise_for_status()
    log.info("  Telegram message sent.")


def notify(fund_name: str, signal: dict, current_nav: float, vix: float) -> None:
    label = signal["label"]
    msg = build_message(fund_name, signal, current_nav, vix)
    subject = f"[Dip Alert] {label} — {fund_name}"
    log.info(f"\n{'='*50}\n{msg}\n{'='*50}")

    if EMAIL_ENABLED:
        try:
            send_email(subject, msg)
        except Exception as e:
            log.error(f"  Email failed: {e}")

    if TELEGRAM_ENABLED:
        try:
            send_telegram(msg)
        except Exception as e:
            log.error(f"  Telegram failed: {e}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== Dip Alert System — started ===")
    run_date = datetime.now().strftime("%Y-%m-%d")

    # ── Market-wide data ──
    log.info("Fetching India VIX...")
    vix = fetch_india_vix()

    log.info("Fetching Nifty Midcap 150...")
    midcap_series = fetch_midcap150()
    momentum_neg = index_momentum_is_negative(midcap_series)

    summary = []

    # ── Per-fund loop ──
    for fund_name, fund_cfg in FUNDS.items():
        log.info(f"\nProcessing: {fund_name}")

        try:
            nav_series = fetch_nav_history(fund_name, fund_cfg)
        except Exception as e:
            log.error(f"  NAV fetch failed: {e}")
            continue

        current_nav = float(nav_series.iloc[-1])
        peak_3m = rolling_peak(nav_series, PEAK_WINDOWS["3m"])
        peak_6m = rolling_peak(nav_series, PEAK_WINDOWS["6m"])
        dd_3m = drawdown(current_nav, peak_3m)
        dd_6m = drawdown(current_nav, peak_6m)

        log.info(
            f"  NAV={current_nav:.4f} | Peak3m={peak_3m:.4f} (DD={dd_3m:.2%}) "
            f"| Peak6m={peak_6m:.4f} (DD={dd_6m:.2%}) | VIX={vix} | Momentum={'↓' if momentum_neg else '→'}"
        )

        signal = evaluate_signal(dd_3m, dd_6m, vix, momentum_neg)

        if signal:
            notify(fund_name, signal, current_nav, vix)
        else:
            log.info(f"  No signal triggered for {fund_name}.")

        summary.append({
            "date": run_date,
            "fund": fund_name,
            "nav": current_nav,
            "peak_3m": peak_3m,
            "dd_3m_pct": round(dd_3m * 100, 2),
            "peak_6m": peak_6m,
            "dd_6m_pct": round(dd_6m * 100, 2),
            "vix": vix,
            "momentum_negative": momentum_neg,
            "signal": signal["label"] if signal else "None",
            "allocation": signal.get("allocation") if signal else "None",
        })

    # ── Append to CSV log ──
    log_df = pd.DataFrame(summary)
    csv_path = "signal_log.csv"
    header = not os.path.exists(csv_path)
    log_df.to_csv(csv_path, mode="a", index=False, header=header)
    log.info(f"\nRun complete — results appended to {csv_path}")


if __name__ == "__main__":
    main()
