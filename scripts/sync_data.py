"""Sync COT, CBOE, prices, curves, and derived features into ./data."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yfinance as yf


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
COT_DIR = DATA_DIR / "cot"
CBOE_DIR = DATA_DIR / "cboe"
PRICE_DIR = DATA_DIR / "prices"
CURVE_DIR = DATA_DIR / "curves"
FEATURE_DIR = DATA_DIR / "features"
MANIFEST_PATH = DATA_DIR / "manifest.json"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cotflow-data")

CFTC_DISAGG_URL = "https://publicreporting.cftc.gov/resource/kh3c-gbw2.json"
CFTC_TFF_URL = "https://publicreporting.cftc.gov/resource/yw9f-hn96.json"
CBOE_CHAIN_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.cboe.com/",
    "Origin": "https://www.cboe.com",
}

MONTH_CODE = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M", 7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}

COT_CONTRACTS: dict[str, dict[str, str]] = {
    "gold": {"cftc_code": "088691", "name": "Gold", "yahoo": "GC=F", "report": "disagg"},
    "silver": {"cftc_code": "084691", "name": "Silver", "yahoo": "SI=F", "report": "disagg"},
    "copper": {"cftc_code": "085692", "name": "Copper", "yahoo": "HG=F", "report": "disagg"},
    "platinum": {"cftc_code": "076651", "name": "Platinum", "yahoo": "PL=F", "report": "disagg"},
    "palladium": {"cftc_code": "075651", "name": "Palladium", "yahoo": "PA=F", "report": "disagg"},
    "cocoa": {"cftc_code": "073732", "name": "Cocoa", "yahoo": "CC=F", "report": "disagg"},
    "coffee": {"cftc_code": "083731", "name": "Coffee", "yahoo": "KC=F", "report": "disagg"},
    "sugar": {"cftc_code": "080732", "name": "Sugar #11", "yahoo": "SB=F", "report": "disagg"},
    "cotton": {"cftc_code": "033661", "name": "Cotton #2", "yahoo": "CT=F", "report": "disagg"},
    "oj": {"cftc_code": "040701", "name": "Orange Juice", "yahoo": "OJ=F", "report": "disagg"},
    "crude_oil": {"cftc_code": "067651", "name": "Crude Oil", "yahoo": "CL=F", "report": "disagg"},
    "nat_gas": {"cftc_code": "023651", "name": "Natural Gas", "yahoo": "NG=F", "report": "disagg"},
    "rbob": {"cftc_code": "111659", "name": "Gasoline", "yahoo": "RB=F", "report": "disagg"},
    "heating": {"cftc_code": "022651", "name": "Heating Oil", "yahoo": "HO=F", "report": "disagg"},
    "wheat": {"cftc_code": "001602", "name": "Wheat", "yahoo": "ZW=F", "report": "disagg"},
    "corn": {"cftc_code": "002602", "name": "Corn", "yahoo": "ZC=F", "report": "disagg"},
    "soybeans": {"cftc_code": "005602", "name": "Soybeans", "yahoo": "ZS=F", "report": "disagg"},
    "soy_oil": {"cftc_code": "007601", "name": "Soybean Oil", "yahoo": "ZL=F", "report": "disagg"},
    "soy_meal": {"cftc_code": "026603", "name": "Soybean Meal", "yahoo": "ZM=F", "report": "disagg"},
    "wheat_hrw": {"cftc_code": "001612", "name": "Wheat HRW", "yahoo": "KW=F", "report": "disagg"},
    "wheat_spring": {"cftc_code": "001626", "name": "Wheat Spring", "yahoo": "MWE=F", "report": "disagg"},
    "rough_rice": {"cftc_code": "039601", "name": "Rough Rice", "yahoo": "ZR=F", "report": "disagg"},
    "canola": {"cftc_code": "135731", "name": "Canola", "yahoo": "RS=F", "report": "disagg"},
    "live_cattle": {"cftc_code": "057642", "name": "Live Cattle", "yahoo": "LE=F", "report": "disagg"},
    "lean_hogs": {"cftc_code": "054642", "name": "Lean Hogs", "yahoo": "HE=F", "report": "disagg"},
    "milk": {"cftc_code": "052641", "name": "Milk Class III", "yahoo": "DC=F", "report": "disagg"},
    "eurusd": {"cftc_code": "099741", "name": "EUR/USD", "yahoo": "EURUSD=X", "report": "tff"},
    "gbpusd": {"cftc_code": "096742", "name": "GBP/USD", "yahoo": "GBPUSD=X", "report": "tff"},
    "usdjpy": {"cftc_code": "097741", "name": "USD/JPY", "yahoo": "JPY=X", "report": "tff"},
    "usdchf": {"cftc_code": "092741", "name": "USD/CHF", "yahoo": "CHF=X", "report": "tff"},
    "usdcad": {"cftc_code": "090741", "name": "USD/CAD", "yahoo": "CAD=X", "report": "tff"},
    "audusd": {"cftc_code": "232741", "name": "AUD/USD", "yahoo": "AUDUSD=X", "report": "tff"},
    "nzdusd": {"cftc_code": "112741", "name": "NZD/USD", "yahoo": "NZDUSD=X", "report": "tff"},
    "sp500": {"cftc_code": "13874A", "name": "S&P 500", "yahoo": "ES=F", "report": "tff", "cboe_symbol": "SPY"},
    "nasdaq": {"cftc_code": "209742", "name": "Nasdaq 100", "yahoo": "NQ=F", "report": "tff", "cboe_symbol": "QQQ"},
    "dow": {"cftc_code": "124601", "name": "Dow Jones", "yahoo": "YM=F", "report": "tff", "cboe_symbol": "DIA"},
    "russell": {"cftc_code": "239742", "name": "Russell 2000", "yahoo": "RTY=F", "report": "tff", "cboe_symbol": "IWM"},
    "vix": {"cftc_code": "1170E1", "name": "VIX", "yahoo": "^VIX", "report": "tff"},
    "us10y": {"cftc_code": "043602", "name": "10Y T-Note", "yahoo": "ZN=F", "report": "tff", "cboe_symbol": "IEF"},
    "us30y": {"cftc_code": "020601", "name": "30Y T-Bond", "yahoo": "ZB=F", "report": "tff", "cboe_symbol": "TLT"},
    "us2y": {"cftc_code": "042601", "name": "2Y T-Note", "yahoo": "ZT=F", "report": "tff", "cboe_symbol": "SHY"},
    "us5y": {"cftc_code": "044601", "name": "5Y T-Note", "yahoo": "ZF=F", "report": "tff"},
    "fed_funds": {"cftc_code": "045601", "name": "Fed Funds", "yahoo": "ZQ=F", "report": "tff"},
    "bitcoin": {"cftc_code": "133741", "name": "Bitcoin", "yahoo": "BTC=F", "report": "tff", "deribit_symbol": "BTC"},
}

CBOE_SYMBOLS = {
    "SPY": "S&P 500 ETF",
    "QQQ": "Nasdaq 100 ETF",
    "IWM": "Russell 2000 ETF",
    "DIA": "Dow Jones ETF",
    "GLD": "Gold ETF",
    "SLV": "Silver ETF",
    "USO": "Crude Oil ETF",
    "UNG": "Natural Gas ETF",
    "TLT": "20Y Treasury ETF",
    "VIX": "VIX Index",
    "COPX": "Copper Miners ETF",
    "IEF": "7-10Y Treasury ETF",
    "SHY": "1-3Y Treasury ETF",
}

CURVE_CONTRACTS = {
    "crude_oil": {"yf_root": "CL", "exchange": "NYM", "months": list(range(1, 13)), "name": "Crude Oil WTI"},
    "nat_gas": {"yf_root": "NG", "exchange": "NYM", "months": list(range(1, 13)), "name": "Natural Gas"},
    "rbob": {"yf_root": "RB", "exchange": "NYM", "months": list(range(1, 13)), "name": "RBOB Gasoline"},
    "heating": {"yf_root": "HO", "exchange": "NYM", "months": list(range(1, 13)), "name": "Heating Oil"},
    "gold": {"yf_root": "GC", "exchange": "CMX", "months": [2, 4, 6, 8, 10, 12], "name": "Gold"},
    "silver": {"yf_root": "SI", "exchange": "CMX", "months": [3, 5, 7, 9, 12], "name": "Silver"},
    "copper": {"yf_root": "HG", "exchange": "CMX", "months": [3, 5, 7, 9, 12], "name": "Copper"},
    "platinum": {"yf_root": "PL", "exchange": "NYM", "months": [1, 4, 7, 10], "name": "Platinum"},
    "palladium": {"yf_root": "PA", "exchange": "NYM", "months": [3, 6, 9, 12], "name": "Palladium"},
    "corn": {"yf_root": "ZC", "exchange": "CBT", "months": [3, 5, 7, 9, 12], "name": "Corn"},
    "wheat": {"yf_root": "ZW", "exchange": "CBT", "months": [3, 5, 7, 9, 12], "name": "Wheat"},
    "soybeans": {"yf_root": "ZS", "exchange": "CBT", "months": [1, 3, 5, 7, 8, 9, 11], "name": "Soybeans"},
    "soy_oil": {"yf_root": "ZL", "exchange": "CBT", "months": [1, 3, 5, 7, 8, 9, 10, 12], "name": "Soybean Oil"},
    "soy_meal": {"yf_root": "ZM", "exchange": "CBT", "months": [1, 3, 5, 7, 8, 9, 10, 12], "name": "Soybean Meal"},
    "live_cattle": {"yf_root": "LE", "exchange": "CME", "months": [2, 4, 6, 8, 10, 12], "name": "Live Cattle"},
    "lean_hogs": {"yf_root": "HE", "exchange": "CME", "months": [2, 4, 5, 6, 7, 8, 10, 12], "name": "Lean Hogs"},
    "wheat_hrw": {"yf_root": "KW", "exchange": "CBT", "months": [3, 5, 7, 9, 12], "name": "Wheat HRW"},
    "rough_rice": {"yf_root": "ZR", "exchange": "CBT", "months": [1, 3, 5, 7, 9, 11], "name": "Rough Rice"},
    "wheat_spring": {"yf_root": "MWE", "exchange": "MGE", "months": [3, 5, 7, 9, 12], "name": "Wheat Spring"},
    "vix": {"yf_root": "VX", "exchange": "CFE", "months": list(range(1, 9)), "name": "VIX", "invert_signal": True},
}


def ensure_dirs() -> None:
    for path in [COT_DIR, CBOE_DIR, PRICE_DIR, CURVE_DIR, FEATURE_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, (float,)) and math.isnan(obj):
        return None
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            return str(obj)
    return str(obj)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default), encoding="utf-8")


def save_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys = sorted({k for row in rows for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def safe_num(value: Any) -> float | int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        iv = int(value)
        if float(value) == iv:
            return iv
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def fetch_cot(contract_key: str, cfg: dict[str, str]) -> pd.DataFrame:
    code = cfg["cftc_code"]
    report = cfg["report"]
    if report == "disagg":
        params = {
            "$where": f"cftc_contract_market_code='{code}'",
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$limit": 5000,
            "$select": ",".join([
                "report_date_as_yyyy_mm_dd",
                "open_interest_all",
                "m_money_positions_long_all",
                "m_money_positions_short_all",
                "prod_merc_positions_long",
                "prod_merc_positions_short",
                "swap_positions_long_all",
                "swap__positions_short_all",
                "other_rept_positions_long",
                "other_rept_positions_short",
            ]),
        }
        resp = requests.get(CFTC_DISAGG_URL, params=params, timeout=60)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        if df.empty:
            return df
        df = df.rename(columns={
            "report_date_as_yyyy_mm_dd": "date",
            "open_interest_all": "oi",
            "m_money_positions_long_all": "mm_long",
            "m_money_positions_short_all": "mm_short",
            "prod_merc_positions_long": "pm_long",
            "prod_merc_positions_short": "pm_short",
            "swap_positions_long_all": "sd_long",
            "swap__positions_short_all": "sd_short",
            "other_rept_positions_long": "other_long",
            "other_rept_positions_short": "other_short",
        })
        for col in ["oi", "mm_long", "mm_short", "pm_long", "pm_short", "sd_long", "sd_short", "other_long", "other_short"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["mm_net"] = df["mm_long"] - df["mm_short"]
        df["pm_net"] = df["pm_long"] - df["pm_short"]
        df["sd_net"] = df["sd_long"] - df["sd_short"]
        df["other_net"] = df["other_long"] - df["other_short"]
    else:
        params = {
            "$where": f"cftc_contract_market_code='{code}'",
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$limit": 5000,
            "$select": ",".join([
                "report_date_as_yyyy_mm_dd",
                "open_interest_all",
                "asset_mgr_positions_long",
                "asset_mgr_positions_short",
                "dealer_positions_long_all",
                "dealer_positions_short_all",
                "lev_money_positions_long",
                "lev_money_positions_short",
                "other_rept_positions_long",
                "other_rept_positions_short",
            ]),
        }
        resp = requests.get(CFTC_TFF_URL, params=params, timeout=60)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        if df.empty:
            return df
        df = df.rename(columns={
            "report_date_as_yyyy_mm_dd": "date",
            "open_interest_all": "oi",
            "asset_mgr_positions_long": "am_long",
            "asset_mgr_positions_short": "am_short",
            "dealer_positions_long_all": "dealer_long",
            "dealer_positions_short_all": "dealer_short",
            "lev_money_positions_long": "lev_long",
            "lev_money_positions_short": "lev_short",
            "other_rept_positions_long": "other_long",
            "other_rept_positions_short": "other_short",
        })
        for col in ["oi", "am_long", "am_short", "dealer_long", "dealer_short", "lev_long", "lev_short", "other_long", "other_short"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["mm_long"] = df["am_long"] + df["lev_long"]
        df["mm_short"] = df["am_short"] + df["lev_short"]
        df["mm_net"] = df["mm_long"] - df["mm_short"]
        df["pm_long"] = df["dealer_long"]
        df["pm_short"] = df["dealer_short"]
        df["pm_net"] = df["pm_long"] - df["pm_short"]
        df["sd_long"] = df["lev_long"]
        df["sd_short"] = df["lev_short"]
        df["sd_net"] = df["sd_long"] - df["sd_short"]
        df["other_net"] = df["other_long"] - df["other_short"]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def save_cot(contract_key: str, df: pd.DataFrame, cfg: dict[str, str]) -> None:
    path = COT_DIR / f"{contract_key}.csv"
    df.to_csv(path, index=False)
    save_json(COT_DIR / f"{contract_key}.latest.json", df.tail(1).to_dict(orient="records")[0] if not df.empty else {})
    logger.info("COT saved %s rows=%s", contract_key, len(df))


def fetch_cboe(symbol: str) -> dict[str, Any]:
    resp = requests.get(CBOE_CHAIN_URL.format(symbol=symbol), headers=HEADERS, timeout=60)
    resp.raise_for_status()
    raw = resp.json()
    data = raw.get("data", {})
    options = data.get("options", [])

    def parse_strike(code: str) -> float | None:
        m = re.search(r"[CP](\d{8})$", code or "")
        return int(m.group(1)) / 1000.0 if m else None

    calls_oi = puts_oi = calls_vol = puts_vol = 0
    atm_calls_iv_sum = atm_puts_iv_sum = 0.0
    atm_calls_iv_count = atm_puts_iv_count = 0
    spot = safe_num(data.get("current_price")) or 0.0
    atm_lo, atm_hi = spot * 0.85, spot * 1.15

    by_expiry: dict[str, dict[str, int]] = {}
    by_strike: dict[float, dict[str, float]] = {}

    for opt in options:
        code = opt.get("option", "")
        oi = safe_num(opt.get("open_interest")) or 0
        vol = safe_num(opt.get("volume")) or 0
        iv = safe_num(opt.get("iv")) or 0
        typ = "C" if re.search(r"\d{6}C", code or "") else "P"
        strike = parse_strike(code)
        m = re.match(r"[A-Z]+(\d{2})(\d{2})(\d{2})[CP]", code or "")
        expiry = f"20{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None

        if typ == "C":
            calls_oi += oi
            calls_vol += vol
        else:
            puts_oi += oi
            puts_vol += vol

        if strike is not None:
            bucket = by_strike.setdefault(strike, {"put_oi": 0.0, "call_oi": 0.0})
            if typ == "C":
                bucket["call_oi"] += oi
            else:
                bucket["put_oi"] += oi

        if expiry:
            eb = by_expiry.setdefault(expiry, {"calls_oi": 0, "puts_oi": 0})
            if typ == "C":
                eb["calls_oi"] += oi
            else:
                eb["puts_oi"] += oi

        if iv > 0 and spot > 0 and strike is not None and atm_lo <= strike <= atm_hi:
            if typ == "C":
                atm_calls_iv_sum += iv
                atm_calls_iv_count += 1
            else:
                atm_puts_iv_sum += iv
                atm_puts_iv_count += 1

    pcr_oi = round(puts_oi / calls_oi, 3) if calls_oi else None
    pcr_volume = round(puts_vol / calls_vol, 3) if calls_vol else None
    avg_call_iv = round((atm_calls_iv_sum / atm_calls_iv_count) * 100, 2) if atm_calls_iv_count else None
    avg_put_iv = round((atm_puts_iv_sum / atm_puts_iv_count) * 100, 2) if atm_puts_iv_count else None

    walls = {
        "put_walls": [{"strike": s, "oi": int(v["put_oi"])} for s, v in sorted(by_strike.items(), key=lambda x: x[1]["put_oi"], reverse=True)[:5]],
        "call_walls": [{"strike": s, "oi": int(v["call_oi"])} for s, v in sorted(by_strike.items(), key=lambda x: x[1]["call_oi"], reverse=True)[:5]],
        "max_pain": None,
    }
    if by_strike:
        candidates = sorted(by_strike.keys())
        min_pain = float("inf")
        best = candidates[0]
        for candidate in candidates:
            pain = sum(max(0.0, candidate - s) * v["call_oi"] + max(0.0, s - candidate) * v["put_oi"] for s, v in by_strike.items())
            if pain < min_pain:
                min_pain = pain
                best = candidate
        walls["max_pain"] = best

    return {
        "symbol": symbol,
        "timestamp": raw.get("timestamp"),
        "current_price": data.get("current_price"),
        "pcr_oi": pcr_oi,
        "pcr_volume": pcr_volume,
        "avg_call_iv_pct": avg_call_iv,
        "avg_put_iv_pct": avg_put_iv,
        "by_expiry": [{"expiry": k, "calls_oi": v["calls_oi"], "puts_oi": v["puts_oi"], "pcr_oi": round(v["puts_oi"] / v["calls_oi"], 3) if v["calls_oi"] else None} for k, v in sorted(by_expiry.items())],
        "walls": walls,
        "source": "CBOE delayed quotes",
        "options_count": len(options),
    }


def save_cboe(symbol: str, payload: dict[str, Any]) -> None:
    save_json(CBOE_DIR / f"{symbol}.json", payload)
    summary_path = CBOE_DIR / "summary.csv"
    row = {
        "symbol": symbol,
        "name": CBOE_SYMBOLS.get(symbol, symbol),
        "timestamp": payload.get("timestamp"),
        "current_price": payload.get("current_price"),
        "pcr_oi": payload.get("pcr_oi"),
        "pcr_volume": payload.get("pcr_volume"),
        "avg_call_iv_pct": payload.get("avg_call_iv_pct"),
        "avg_put_iv_pct": payload.get("avg_put_iv_pct"),
        "options_count": payload.get("options_count"),
    }
    rows: list[dict[str, Any]] = []
    if summary_path.exists():
        with summary_path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    rows = [r for r in rows if r.get("symbol") != symbol] + [row]
    save_csv(summary_path, rows)


def _next_active_months(root: str, exchange: str, months: list[int], n: int = 8) -> list[tuple[int, int]]:
    today = datetime.now(timezone.utc)
    year, month = today.year, today.month
    active = set(months)
    result = []
    for _ in range(24):
        if month in active:
            result.append((year, month))
            if len(result) >= n:
                break
        month += 1
        if month > 12:
            month = 1
            year += 1
    return result


def fetch_curve(contract_key: str, cfg: dict[str, Any]) -> dict[str, Any] | None:
    months = _next_active_months(cfg["yf_root"], cfg["exchange"], cfg["months"], n=8)
    if not months:
        return None
    tickers = [f'{cfg["yf_root"]}{MONTH_CODE[m]}{str(y)[-2:]}.{cfg["exchange"]}' for y, m in months]
    df = yf.download(tickers, period="5d", interval="1d", auto_adjust=True, progress=False)
    if df.empty:
        return None
    close = df["Close"] if isinstance(df.columns, pd.MultiIndex) else df[["Close"]]
    last = close.ffill().iloc[-1]
    curve = []
    for ticker, (year, month) in zip(tickers, months):
        if ticker in last.index and pd.notna(last[ticker]) and last[ticker] > 0:
            curve.append({"expiry": f"{year}-{month:02d}", "price": float(last[ticker])})
    if len(curve) < 2:
        return None
    prices = [c["price"] for c in curve]
    spread_m1m3 = round((prices[2] - prices[0]) / prices[0] * 100, 4) if len(prices) >= 3 else None
    spread_m1m6 = round((prices[5] - prices[0]) / prices[0] * 100, 4) if len(prices) >= 6 else None
    slope = None
    if len(prices) >= 3:
        x = list(range(len(prices)))
        slope = round(float(pd.Series(prices).reset_index(drop=True).corr(pd.Series(x)) or 0.0), 4)
    signal = "CONTANGO" if (spread_m1m3 or 0) > 0 else "BACKWARDATION"
    payload = {
        "contract": contract_key,
        "name": cfg["name"],
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "curve_months": curve,
        "m1_price": prices[0],
        "m2_price": prices[1] if len(prices) > 1 else None,
        "m3_price": prices[2] if len(prices) > 2 else None,
        "m6_price": prices[5] if len(prices) > 5 else None,
        "spread_m1m3": spread_m1m3,
        "spread_m1m6": spread_m1m6,
        "curve_slope_proxy": slope,
        "signal": signal,
        "source": "yfinance futures",
    }
    return payload


def save_curve(contract_key: str, payload: dict[str, Any]) -> None:
    save_json(CURVE_DIR / f"{contract_key}.json", payload)
    rows = []
    csv_path = CURVE_DIR / f"{contract_key}.csv"
    if csv_path.exists():
        rows = list(csv.DictReader(csv_path.open("r", encoding="utf-8")))
    rows = [r for r in rows if r.get("date") != payload["date"]]
    rows.append({
        "date": payload["date"],
        "m1_price": payload["m1_price"],
        "m2_price": payload["m2_price"],
        "m3_price": payload["m3_price"],
        "m6_price": payload["m6_price"],
        "spread_m1m3": payload["spread_m1m3"],
        "spread_m1m6": payload["spread_m1m6"],
        "signal": payload["signal"],
    })
    save_csv(csv_path, rows)


def fetch_prices(contract_key: str, yahoo: str) -> dict[str, Any]:
    df = yf.download(yahoo, period="2y", progress=False, auto_adjust=True)
    if df.empty:
        return {}
    if isinstance(df.columns, pd.MultiIndex):
        close = df["Close"].iloc[:, 0]
    else:
        close = df["Close"]
    close = close.dropna()
    payload = {
        "contract": contract_key,
        "yahoo": yahoo,
        "latest": float(close.iloc[-1]),
        "date": close.index[-1].strftime("%Y-%m-%d"),
        "returns": {
            "1w": pct_change(close, 5),
            "1m": pct_change(close, 22),
            "3m": pct_change(close, 66),
            "6m": pct_change(close, 132),
            "1y": pct_change(close, 252),
        },
        "source": "yfinance",
    }
    return payload


def pct_change(series: pd.Series, periods: int) -> float | None:
    if len(series) <= periods:
        return None
    old = float(series.iloc[-(periods + 1)])
    if not old:
        return None
    return round((float(series.iloc[-1]) - old) / old * 100, 2)


def save_prices(contract_key: str, payload: dict[str, Any]) -> None:
    save_json(PRICE_DIR / f"{contract_key}.json", payload)


def build_feature_row(contract_key: str) -> dict[str, Any]:
    cot_path = COT_DIR / f"{contract_key}.csv"
    price_path = PRICE_DIR / f"{contract_key}.json"
    curve_path = CURVE_DIR / f"{contract_key}.json"
    cboe_key = next((sym for sym, c in COT_CONTRACTS.items() if c.get("cboe_symbol") and sym == contract_key), None)
    feature: dict[str, Any] = {"contract": contract_key}

    if cot_path.exists():
        df = pd.read_csv(cot_path, parse_dates=["date"])
        if not df.empty:
            last = df.iloc[-1]
            prev_4 = df.iloc[-5] if len(df) > 4 else last
            last_mm = safe_num(last.get("mm_net"))
            prev_mm = safe_num(prev_4.get("mm_net"))
            feature.update({
                "cot_date": str(last["date"])[:10],
                "cot_mm_net": last_mm,
                "cot_mm_wow": last_mm - prev_mm if last_mm is not None and prev_mm is not None else None,
                "cot_index_proxy": rolling_index(df["mm_net"]),
            })

    if price_path.exists():
        p = json.loads(price_path.read_text(encoding="utf-8"))
        feature.update({
            "price_date": p.get("date"),
            "price_latest": p.get("latest"),
            "price_1w": p.get("returns", {}).get("1w"),
            "price_1m": p.get("returns", {}).get("1m"),
            "price_3m": p.get("returns", {}).get("3m"),
            "price_6m": p.get("returns", {}).get("6m"),
            "price_1y": p.get("returns", {}).get("1y"),
        })

    if curve_path.exists():
        c = json.loads(curve_path.read_text(encoding="utf-8"))
        feature.update({
            "curve_signal": c.get("signal"),
            "curve_spread_m1m3": c.get("spread_m1m3"),
            "curve_spread_m1m6": c.get("spread_m1m6"),
        })
        if c.get("spread_m1m3") is not None:
            feature["contango_state"] = "contango" if c["spread_m1m3"] > 0 else "backwardation"

    if contract_key in COT_CONTRACTS and COT_CONTRACTS[contract_key].get("cboe_symbol"):
        sym = COT_CONTRACTS[contract_key]["cboe_symbol"]
        cboe_path = CBOE_DIR / f"{sym}.json"
        if cboe_path.exists():
            o = json.loads(cboe_path.read_text(encoding="utf-8"))
            feature.update({
                "cboe_symbol": sym,
                "pcr_oi": o.get("pcr_oi"),
                "pcr_volume": o.get("pcr_volume"),
                "iv_skew": o.get("avg_put_iv_pct") - o.get("avg_call_iv_pct") if o.get("avg_put_iv_pct") is not None and o.get("avg_call_iv_pct") is not None else None,
            })
    return feature


def rolling_index(series: pd.Series, lookback: int = 52) -> float | None:
    if series.empty:
        return None
    tail = series.tail(lookback)
    lo = tail.min()
    hi = tail.max()
    val = tail.iloc[-1]
    if pd.isna(lo) or pd.isna(hi) or pd.isna(val) or hi == lo:
        return None
    return round(float((val - lo) / (hi - lo) * 100), 1)


def sync(mode: str) -> dict[str, Any]:
    ensure_dirs()
    now = datetime.now(timezone.utc).isoformat()
    manifest: dict[str, Any] = {"generated_at": now, "mode": mode, "counts": {}}

    if mode in {"all", "cot"}:
        cot_count = 0
        for key, cfg in COT_CONTRACTS.items():
            try:
                df = fetch_cot(key, cfg)
                if df.empty:
                    continue
                save_cot(key, df, cfg)
                cot_count += 1
            except Exception as e:
                logger.warning("COT failed %s: %s", key, e)
        manifest["counts"]["cot"] = cot_count

    if mode in {"all", "cboe"}:
        cboe_count = 0
        for sym in sorted(CBOE_SYMBOLS):
            try:
                payload = fetch_cboe(sym)
                save_cboe(sym, payload)
                cboe_count += 1
            except Exception as e:
                logger.warning("CBOE failed %s: %s", sym, e)
        manifest["counts"]["cboe"] = cboe_count

    if mode in {"all", "prices"}:
        price_count = 0
        for key, cfg in COT_CONTRACTS.items():
            try:
                payload = fetch_prices(key, cfg["yahoo"])
                if not payload:
                    continue
                save_prices(key, payload)
                price_count += 1
            except Exception as e:
                logger.warning("Price failed %s: %s", key, e)
        manifest["counts"]["prices"] = price_count

    if mode in {"all", "curves"}:
        curve_count = 0
        for key, cfg in CURVE_CONTRACTS.items():
            try:
                payload = fetch_curve(key, cfg)
                if not payload:
                    continue
                save_curve(key, payload)
                curve_count += 1
            except Exception as e:
                logger.warning("Curve failed %s: %s", key, e)
        manifest["counts"]["curves"] = curve_count

    if mode in {"all", "features"}:
        rows = [build_feature_row(key) for key in COT_CONTRACTS.keys()]
        save_csv(FEATURE_DIR / "latest_features.csv", rows)
        save_json(FEATURE_DIR / "latest_features.json", rows)
        manifest["counts"]["features"] = len(rows)

    save_json(MANIFEST_PATH, manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["all", "cot", "cboe", "prices", "curves", "features"], default="all")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    manifest = sync(args.mode)
    logger.info("Sync complete: %s", manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
