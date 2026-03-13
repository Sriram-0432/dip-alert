# -*- coding: utf-8 -*-
"""
Mutual Fund Dip Alert System — v2.0
Monitors: Motilal Oswal Midcap Fund, Parag Parikh Flexi Cap Fund

Upgrades in v2.0:
  - Market regime filter (Nifty 50 vs 200DMA)
  - Nifty 50 broader market confirmation
  - Recovery signal detection
  - Signal confidence score
  - Improved VIX thresholds
  - Enhanced alert message
  - Alert cooldown (max 1 alert per fund per day)
"""

import sys
import io
import os
import csv
import logging
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
import pandas as pd
import yfinance as yf

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

FUNDS = {
    "Motilal Oswal Midcap Fund": {
        "mfapi_id": "127042",
        "short_name": "MO_Midcap",
    },
    "Parag Parikh Flexi Cap Fund": {
        "mfapi_id": "122639",
        "short_name": "PP_FlexiCap",
    },
}

NIFTY50_TICKER   = "^NSEI"
NIFTY_MIDCAP_150 = "NIFTYMIDCAP150.NS"
INDIA_VIX_TICKER = "^INDIAVIX"

# Improved VIX thresholds
SIGNAL_RULES = [
    {"label": "Aggressive Buy", "drawdown": 0.20, "vix": 23, "allocation": "Large"},
    {"label": "Strong Buy",     "drawdown": 0.15, "vix": 19, "allocation": "Medium"},
    {"label": "Buy",            "drawdown": 0.10, "vix": 16, "allocation": "Small"},
    {"label": "Alert",          "drawdown": 0.05, "vix": 14, "allocation": None},
]

# Bear market allocation downgrade
BEAR_MARKET_ALLOCATION = {
    "Large":  "Medium",
    "Medium": "Small",
    "Small":  "Very Small",
    None:     None,
}

# Confidence score weights
CONFIDENCE_WEIGHTS = {
    "drawdown": 0.40,
    "vix":      0.25,
    "momentum": 0.20,
    "regime":   0.15,
}

MOMENTUM_LOOKBACK_DAYS  = 10
MOMENTUM_DROP_THRESHOLD = 0.05
MA_WINDOW               = 200
RECOVERY_IMPROVEMENT    = 0.05
PEAK_WINDOWS            = {"3m": 60, "6m": 120}

# ─── NOTIFICATIONS ────────────────────────────────────────────────────────────

EMAIL_ENABLED      = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SENDER       = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD     = os.getenv("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT    = os.getenv("EMAIL_RECIPIENT", "")
SMTP_HOST          = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT          = int(os.getenv("SMTP_PORT", "587"))

TELEGRAM_ENABLED   = os.getenv("TELEGRAM_ENABLED", "false").lower() == "true"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

LOG_FILE          = os.getenv("LOG_FILE", "dip_alerts.log")
SIGNAL_LOG_CSV    = "signal_log.csv"
PREV_DRAWDOWN_CSV = "prev_drawdown.csv"
COOLDOWN_CSV      = "alert_cooldown.csv"

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(stream=sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def ist_now() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def today_str() -> str:
    return ist_now().strftime("%Y-%m-%d")

# ─── DATA FETCHING ────────────────────────────────────────────────────────────

def fetch_nav_history(fund_name: str, fund_cfg: dict) -> pd.Series:
    url = f"https://api.mfapi.in/mf/{fund_cfg['mfapi_id']}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    records = {
        datetime.strptime(d["date"], "%d-%m-%Y").date(): float(d["nav"])
        for d in data
    }
    series = pd.Series(records).sort_index()
    log.info(f"  {fund_name}: {len(series)} NAV records, latest = {series.iloc[-1]:.4f}")
    return series

def fetch_yf(ticker: str, period: str = "1y") -> pd.Series:
    df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
    if df.empty:
        raise ValueError(f"No data for {ticker}")
    series = df["Close"].squeeze()
    series.index = pd.to_datetime(series.index).date
    log.info(f"  {ticker}: {len(series)} rows, latest = {series.iloc[-1]:.2f}")
    return series

def fetch_india_vix() -> float:
    series = fetch_yf(INDIA_VIX_TICKER, period="5d")
    return round(float(series.iloc[-1]), 2)

def fetch_nifty50() -> pd.Series:
    return fetch_yf(NIFTY50_TICKER, period="1y")

def fetch_midcap150() -> pd.Series:
    try:
        return fetch_yf(NIFTY_MIDCAP_150, period="3mo")
    except Exception:
        log.warning("  Midcap 150 failed, falling back to Nifty 50")
        return fetch_yf(NIFTY50_TICKER, period="3mo")

# ─── CALCULATIONS ─────────────────────────────────────────────────────────────

def rolling_peak(nav: pd.Series, window_days: int) -> float:
    return float(nav.iloc[-window_days:].max())

def drawdown(current: float, peak: float) -> float:
    return (peak - current) / peak if peak > 0 else 0.0

def index_momentum_is_negative(index_series: pd.Series) -> bool:
    if len(index_series) < MOMENTUM_LOOKBACK_DAYS + 1:
        log.warning("  Not enough data for momentum — defaulting False")
        return False
    recent = index_series.iloc[-MOMENTUM_LOOKBACK_DAYS:]
    start, end = float(recent.iloc[0]), float(recent.iloc[-1])
    change = (end - start) / start
    log.info(f"  Index momentum ({MOMENTUM_LOOKBACK_DAYS}d): {change:.2%}")
    return change < -MOMENTUM_DROP_THRESHOLD

def get_market_regime(nifty50_series: pd.Series) -> dict:
    if len(nifty50_series) < MA_WINDOW:
        log.warning("  Not enough data for 200DMA — defaulting Neutral")
        return {"label": "Neutral", "pct_vs_ma": 0.0}
    ma200   = float(nifty50_series.iloc[-MA_WINDOW:].mean())
    current = float(nifty50_series.iloc[-1])
    pct     = (current - ma200) / ma200
    label   = "Bull" if pct > 0.02 else "Bear" if pct < -0.02 else "Neutral"
    log.info(f"  Regime: {label} | Nifty={current:.0f} | 200DMA={ma200:.0f} | {pct:.2%} vs MA")
    return {"label": label, "pct_vs_ma": round(pct * 100, 2)}

def get_nifty50_drawdown(nifty50_series: pd.Series) -> float:
    peak    = rolling_peak(nifty50_series, 120)
    current = float(nifty50_series.iloc[-1])
    dd      = drawdown(current, peak)
    log.info(f"  Nifty 50 drawdown from 6m peak: {dd:.2%}")
    return dd

def nifty50_market_phase(nifty50_dd: float) -> str:
    if nifty50_dd >= 0.15:
        return "Panic"
    elif nifty50_dd >= 0.10:
        return "Deeper correction"
    elif nifty50_dd >= 0.05:
        return "Correction"
    else:
        return "Normal"

# ─── CONFIDENCE SCORE ─────────────────────────────────────────────────────────

def calculate_confidence(effective_dd: float, vix: float, momentum_negative: bool, regime: dict) -> int:
    dd_score       = min(effective_dd / 0.20, 1.0)
    vix_score      = min(max((vix - 14) / 16, 0.0), 1.0)
    momentum_score = 1.0 if momentum_negative else 0.0
    regime_score   = {"Bull": 1.0, "Neutral": 0.6, "Bear": 0.3}.get(regime["label"], 0.6)
    raw = (
        dd_score       * CONFIDENCE_WEIGHTS["drawdown"] +
        vix_score      * CONFIDENCE_WEIGHTS["vix"] +
        momentum_score * CONFIDENCE_WEIGHTS["momentum"] +
        regime_score   * CONFIDENCE_WEIGHTS["regime"]
    )
    return round(raw * 100)

# ─── SIGNAL LOGIC ─────────────────────────────────────────────────────────────

def evaluate_signal(dd_3m: float, dd_6m: float, vix: float, momentum_negative: bool) -> dict | None:
    if not momentum_negative:
        return None
    effective_dd = max(dd_3m, dd_6m)
    for rule in SIGNAL_RULES:
        if effective_dd >= rule["drawdown"] and vix >= rule["vix"]:
            return {**rule, "drawdown_used": effective_dd, "dd_3m": dd_3m, "dd_6m": dd_6m}
    return None

def adjust_for_regime(signal: dict, regime: dict) -> dict:
    if regime["label"] == "Bear" and signal.get("allocation"):
        original = signal["allocation"]
        adjusted = BEAR_MARKET_ALLOCATION.get(original, original)
        log.info(f"  Bear market: allocation downgraded {original} -> {adjusted}")
        return {**signal, "allocation": adjusted, "bear_adjusted": True}
    return {**signal, "bear_adjusted": False}

# ─── RECOVERY DETECTION ───────────────────────────────────────────────────────

def load_prev_drawdowns() -> dict:
    if not os.path.exists(PREV_DRAWDOWN_CSV):
        return {}
    result = {}
    with open(PREV_DRAWDOWN_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            result[row["fund"]] = float(row["drawdown"])
    return result

def save_prev_drawdowns(data: dict) -> None:
    with open(PREV_DRAWDOWN_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["fund", "drawdown"])
        writer.writeheader()
        for fund, dd in data.items():
            writer.writerow({"fund": fund, "drawdown": dd})

def check_recovery(fund_name: str, current_dd: float, prev_drawdowns: dict) -> bool:
    prev_dd = prev_drawdowns.get(fund_name)
    if prev_dd is None:
        return False
    improvement      = prev_dd - current_dd
    was_deep         = prev_dd >= 0.10
    recovered_enough = improvement >= RECOVERY_IMPROVEMENT
    if was_deep and recovered_enough:
        log.info(f"  Recovery: {fund_name} | {prev_dd:.2%} -> {current_dd:.2%} | improved {improvement:.2%}")
        return True
    return False

# ─── COOLDOWN ─────────────────────────────────────────────────────────────────

def already_alerted_today(fund_name: str) -> bool:
    if not os.path.exists(COOLDOWN_CSV):
        return False
    with open(COOLDOWN_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["fund"] == fund_name and row["date"] == today_str():
                return True
    return False

def mark_alerted_today(fund_name: str) -> None:
    file_exists = os.path.exists(COOLDOWN_CSV)
    with open(COOLDOWN_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "fund"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({"date": today_str(), "fund": fund_name})

# ─── MESSAGE BUILDERS ─────────────────────────────────────────────────────────

def confidence_bar(score: int) -> str:
    filled = round(score / 20)
    return "🟩" * filled + "⬜" * (5 - filled) + f"  {score}%"

def build_signal_message(fund_name, signal, current_nav, vix, regime, nifty50_dd, confidence) -> str:
    label    = signal["label"]
    dd_3m    = signal["dd_3m"]
    dd_6m    = signal["dd_6m"]
    alloc    = signal.get("allocation") or "Watch"
    bear_tag = " ⚠️ Bear adjusted" if signal.get("bear_adjusted") else ""

    signal_bar = {
        "Alert":          "🟢⚪⚪⚪",
        "Buy":            "🟢🟢⚪⚪",
        "Strong Buy":     "🟢🟢🟢⚪",
        "Aggressive Buy": "🟢🟢🟢🟢",
    }.get(label, "⚪⚪⚪⚪")

    vix_tag = (
        "🔵 Calm"            if vix < 14 else
        "🟡 Moderate"        if vix < 19 else
        "🟠 Elevated stress" if vix < 23 else
        "🔴 Panic zone"
    )

    regime_tag = {
        "Bull":    f"🐂 Bull  ({regime['pct_vs_ma']:+.1f}% vs 200DMA)",
        "Neutral": f"⚖️ Neutral ({regime['pct_vs_ma']:+.1f}% vs 200DMA)",
        "Bear":    f"🐻 Bear  ({regime['pct_vs_ma']:+.1f}% vs 200DMA)",
    }.get(regime["label"], "")

    nifty_phase = nifty50_market_phase(nifty50_dd)
    nifty_tag = {
        "Normal":            "🟢 Stable",
        "Correction":        "🟡 Correction",
        "Deeper correction": "🟠 Deeper correction",
        "Panic":             "🔴 Panic",
    }.get(nifty_phase, nifty_phase)

    sep = "─" * 35

    return (
        f"📊 {label} — {fund_name}\n"
        f"{sep}\n"
        f"Signal      : {signal_bar} {label}\n"
        f"Confidence  : {confidence_bar(confidence)}\n"
        f"Current NAV : ₹{current_nav:.4f}\n"
        f"{sep}\n"
        f"DD (3M)     : {dd_3m:.1%} from 3M peak\n"
        f"DD (6M)     : {dd_6m:.1%} from 6M peak\n"
        f"{sep}\n"
        f"India VIX   : {vix:.2f}  {vix_tag}\n"
        f"Momentum    : ↓ Correction phase\n"
        f"Market      : {regime_tag}\n"
        f"Nifty 50    : {nifty_tag}  ({nifty50_dd:.1%} from peak)\n"
        f"{sep}\n"
        f"Suggested   : {alloc} investment{bear_tag}\n"
        f"{sep}\n"
        f"Date        : {ist_now().strftime('%d %b %Y  %H:%M IST')}"
    )

def build_recovery_message(fund_name, current_dd, prev_dd, current_nav) -> str:
    improvement = prev_dd - current_dd
    return (
        f"[Recovery Alert]\n"
        f"{fund_name}\n"
        f"-----------------------------------\n"
        f"Previous DD : {prev_dd:.1%} from peak\n"
        f"Current DD  : {current_dd:.1%} from peak\n"
        f"Improvement : +{improvement:.1%}\n"
        f"Current NAV : Rs.{current_nav:.4f}\n"
        f"Momentum    : Turning positive\n"
        f"-----------------------------------\n"
        f"Note        : Review existing positions\n"
        f"Date        : {ist_now().strftime('%d %b %Y %H:%M IST')}"
    )

# ─── NOTIFICATIONS ────────────────────────────────────────────────────────────

def send_email(subject: str, body: str) -> None:
    msg = MIMEMultipart()
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_RECIPIENT
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
    log.info("  Email sent.")

def send_telegram(text: str) -> None:
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    resp.raise_for_status()
    log.info("  Telegram message sent.")

def notify(subject: str, msg: str) -> None:
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
    log.info("=== Dip Alert System v2.0 started ===")
    run_date = today_str()

    log.info("Fetching India VIX...")
    vix = fetch_india_vix()

    log.info("Fetching Nifty 50 (regime + confirmation)...")
    nifty50_series = fetch_nifty50()
    regime         = get_market_regime(nifty50_series)
    nifty50_dd     = get_nifty50_drawdown(nifty50_series)

    log.info("Fetching Nifty Midcap 150 (momentum)...")
    midcap_series = fetch_midcap150()
    momentum_neg  = index_momentum_is_negative(midcap_series)

    prev_drawdowns    = load_prev_drawdowns()
    current_drawdowns = {}
    summary           = []

    for fund_name, fund_cfg in FUNDS.items():
        log.info(f"\nProcessing: {fund_name}")

        try:
            nav_series = fetch_nav_history(fund_name, fund_cfg)
        except Exception as e:
            log.error(f"  NAV fetch failed: {e}")
            continue

        current_nav  = float(nav_series.iloc[-1])
        peak_3m      = rolling_peak(nav_series, PEAK_WINDOWS["3m"])
        peak_6m      = rolling_peak(nav_series, PEAK_WINDOWS["6m"])
        dd_3m        = drawdown(current_nav, peak_3m)
        dd_6m        = drawdown(current_nav, peak_6m)
        effective_dd = max(dd_3m, dd_6m)

        log.info(
            f"  NAV={current_nav:.4f} | Peak3m={peak_3m:.4f} (DD={dd_3m:.2%}) "
            f"| Peak6m={peak_6m:.4f} (DD={dd_6m:.2%}) | VIX={vix} "
            f"| Momentum={'Down' if momentum_neg else 'Flat'}"
        )

        current_drawdowns[fund_name] = effective_dd

        if already_alerted_today(fund_name):
            log.info(f"  Cooldown active — already alerted for {fund_name} today.")
            continue

        if check_recovery(fund_name, effective_dd, prev_drawdowns):
            msg = build_recovery_message(
                fund_name, effective_dd,
                prev_drawdowns[fund_name], current_nav
            )
            notify(f"[Recovery] {fund_name}", msg)
            mark_alerted_today(fund_name)

        signal = evaluate_signal(dd_3m, dd_6m, vix, momentum_neg)

        if signal:
            signal     = adjust_for_regime(signal, regime)
            confidence = calculate_confidence(effective_dd, vix, momentum_neg, regime)
            msg        = build_signal_message(
                fund_name, signal, current_nav, vix,
                regime, nifty50_dd, confidence,
            )
            notify(f"[Dip Alert] {signal['label']} - {fund_name}", msg)
            mark_alerted_today(fund_name)
        else:
            log.info(f"  No signal for {fund_name}.")

        summary.append({
            "date":              run_date,
            "fund":              fund_name,
            "nav":               current_nav,
            "peak_3m":           peak_3m,
            "dd_3m_pct":         round(dd_3m * 100, 2),
            "peak_6m":           peak_6m,
            "dd_6m_pct":         round(dd_6m * 100, 2),
            "vix":               vix,
            "regime":            regime["label"],
            "nifty50_dd_pct":    round(nifty50_dd * 100, 2),
            "momentum_negative": momentum_neg,
            "signal":            signal["label"] if signal else "None",
            "allocation":        signal.get("allocation") if signal else "None",
            "confidence":        calculate_confidence(effective_dd, vix, momentum_neg, regime) if signal else 0,
        })

    save_prev_drawdowns(current_drawdowns)

    log_df = pd.DataFrame(summary)
    header = not os.path.exists(SIGNAL_LOG_CSV)
    log_df.to_csv(SIGNAL_LOG_CSV, mode="a", index=False, header=header)
    log.info(f"\nRun complete — results appended to {SIGNAL_LOG_CSV}")


if __name__ == "__main__":
    main()