"""
MarketBoard
===========
A focused Streamlit research dashboard with five pages:
  1. Market Overview  — Indices, Fear & Greed, Sector Heatmap, Hedge Risk Gauge, Movers
  2. Stock Research   — Candlestick/Valuation chart, financials, news, IV summary
  3. Value Investing  — Phil Town Big Five, Sticker Price, Buffett Checklist, DCF
  4. Watchlist        — Named watchlists with price table and candlestick chart
  5. Screener         — Filter S&P 500 / NASDAQ-100 universe by fundamentals

Data source: yFinance (delayed) + CNN Fear & Greed API.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime
import warnings
import requests as _requests
import jwt as pyjwt
from streamlit_oauth import OAuth2Component

import db  # local SQLite persistence layer (db.py)

warnings.filterwarnings("ignore")

# ─── PAGE CONFIG ─────────────────────────────────────────────
st.set_page_config(
    page_title="MarketBoard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── DARK TRADING THEME ──────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0e1117; color: #e2e8f0; }

    [data-testid="metric-container"] {
        background-color: #1a1f2e;
        border: 1px solid #2d3250;
        border-radius: 10px;
        padding: 16px;
    }

    .stTabs [data-baseweb="tab-list"] {
        background-color: #161b27;
        border-radius: 10px;
        padding: 4px;
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        color: #94a3b8;
        font-weight: 600;
        border-radius: 8px;
        padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #2d3250 !important;
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] { background-color: #161b27; }
    .stDataFrame { border-radius: 8px; }

    div[data-testid="stExpander"] {
        background-color: #1a1f2e;
        border: 1px solid #2d3250;
        border-radius: 10px;
    }

    .check-pass {
        background-color: rgba(34, 197, 94, 0.12);
        border-left: 3px solid #22c55e;
        padding: 8px 14px;
        border-radius: 4px;
        margin: 4px 0;
        color: #e2e8f0;
    }
    .check-fail {
        background-color: rgba(239, 68, 68, 0.12);
        border-left: 3px solid #ef4444;
        padding: 8px 14px;
        border-radius: 4px;
        margin: 4px 0;
        color: #e2e8f0;
    }
    .check-warn {
        background-color: rgba(245, 158, 11, 0.12);
        border-left: 3px solid #f59e0b;
        padding: 8px 14px;
        border-radius: 4px;
        margin: 4px 0;
        color: #e2e8f0;
    }

    h1, h2, h3 { color: #e2e8f0; }
</style>
""", unsafe_allow_html=True)

# Initialise the SQLite database before any session state that reads from it.
db.init_db()

# ─── SESSION STATE ────────────────────────────────────────────
if "watchlist_names" not in st.session_state:
    st.session_state.watchlist_names = db.get_watchlist_names()
if "active_watchlist" not in st.session_state:
    names = st.session_state.watchlist_names
    st.session_state.active_watchlist = names[0] if names else "My Watchlist"
if "watchlist" not in st.session_state:
    st.session_state.watchlist     = db.get_watchlist(st.session_state.active_watchlist)
    st.session_state.watchlist_qty = db.get_watchlist_qty(st.session_state.active_watchlist)
if "vi_cache" not in st.session_state:
    st.session_state.vi_cache = {}

# ─── CONSTANTS ───────────────────────────────────────────────
INDICES = {
    "S&P 500 (SPX)":    "^GSPC",
    "NASDAQ 100 (NQ)":  "^NDX",
    "VIX":              "^VIX",
    "Dow Jones":        "^DJI",
    "Russell 2000":     "^RUT",
    "10Y Yield":        "^TNX",
    "Gold":             "GC=F",
    "Crude Oil":        "CL=F",
    "US Dollar":        "DX-Y.NYB",
}

FUTURES = {
    "S&P 500 Fut (ES)":  "ES=F",
    "NASDAQ Fut (NQ)":   "NQ=F",
    "VIX Fut (VIXY)":    "VIXY",
    "Dow Fut (YM)":      "YM=F",
    "Russell Fut (RTY)": "RTY=F",
    "10Y Note Fut (ZN)": "ZN=F",
    "Gold Fut (GC)":     "GC=F",
    "Oil Fut (CL)":      "CL=F",
    "USD Index Fut (DX)":"DX=F",
}

SECTORS = {
    "Technology":       "XLK",
    "Healthcare":       "XLV",
    "Financials":       "XLF",
    "Energy":           "XLE",
    "Consumer Disc.":   "XLY",
    "Consumer Staples": "XLP",
    "Industrials":      "XLI",
    "Materials":        "XLB",
    "Utilities":        "XLU",
    "Real Estate":      "XLRE",
    "Communication":    "XLC",
}

COLORS = {
    "up":     "#22c55e",
    "down":   "#ef4444",
    "warn":   "#f59e0b",
    "accent": "#6366f1",
    "bg":     "#0e1117",
    "card":   "#1a1f2e",
    "border": "#2d3250",
    "text":   "#e2e8f0",
    "muted":  "#94a3b8",
}

# Tickers scanned for unusual options activity
UNUSUAL_SCAN_TICKERS = (
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO",
    "COST", "NFLX", "AMD", "ADBE", "QCOM", "INTC", "PEP", "INTU", "AMAT",
    "CSCO", "CMCSA", "TXN", "HON", "AMGN", "BKNG", "SBUX", "GILD", "ADI",
    "LRCX", "MDLZ", "REGN", "ISRG", "VRTX", "MU", "PANW", "KLAC", "SNPS",
    "CDNS", "PYPL", "MELI", "ASML", "ORLY", "ABNB", "MAR", "CTAS", "NXPI",
    "FTNT", "MRVL", "PCAR", "ADP", "CPRT", "ROST", "CHTR", "PAYX", "DXCM",
    "MCHP", "BIIB", "IDXX", "FAST", "DLTR", "ODFL", "KDP", "GEHC",
    "VRSK", "EXC", "FANG", "ON", "CEG", "ZS", "CCEP", "ANSS", "TTWO",
    "CRWD", "DDOG", "TEAM", "WDAY", "PLTR", "RBLX", "ZM", "OKTA",
    "SPY", "QQQ", "IWM", "XLF", "XLE", "XLK", "XLV", "XLI", "GLD", "TLT",
    "COIN", "MARA", "RIOT", "SOFI", "GME", "AMC", "RIVN",
)

# Screener universe — S&P 500 large/mid caps + NASDAQ-100 + high-interest names
_SCREENER_UNIVERSE = (
    # Mega-cap Tech
    "AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA","AVGO","ORCL","ADBE",
    "CRM","AMD","INTC","QCOM","TXN","MU","AMAT","NOW","INTU","PANW",
    "CRWD","SNPS","CDNS","MRVL","LRCX","KLAC","ADI","NXPI","FTNT","ZS",
    "DDOG","SNOW","WDAY","TEAM","OKTA","PLTR","ARM","SHOP","NET","HUBS",
    # Financials
    "JPM","BAC","WFC","GS","MS","BLK","C","AXP","V","MA",
    "COF","USB","BAM","BX","KKR","APO","SCHW","SPGI","MCO","ICE",
    # Healthcare
    "UNH","JNJ","LLY","ABBV","MRK","PFE","TMO","ABT","DHR","BMY",
    "AMGN","GILD","REGN","VRTX","ISRG","SYK","BSX","MDT","BIIB","IDXX",
    # Consumer Cyclical
    "HD","MCD","SBUX","NKE","TGT","WMT","COST","LOW","TJX","BKNG",
    "ABNB","MAR","HLT","EBAY","ETSY","RVTY","DECK","TPR",
    # Communication
    "NFLX","DIS","CMCSA","SPOT","RBLX","SNAP","TTWO","EA","MTCH",
    # Energy
    "XOM","CVX","COP","SLB","EOG","PSX","VLO","MPC","OXY","HES",
    # Industrials
    "CAT","DE","HON","GE","RTX","LMT","NOC","BA","UPS","FDX","CSX","NSC",
    # Utilities & REITs
    "NEE","DUK","SO","AMT","PLD","SPG","O","WELL",
    # Materials
    "LIN","APD","NEM","FCX","GOLD","ALB",
    # High-interest / Growth
    "COIN","HOOD","SOFI","MARA","RIOT","DKNG","ROKU","LYFT","RIVN",
)


# ─── DATA FETCHING ────────────────────────────────────────────

def _empty_quote():
    return {"price": 0, "change": 0, "pct_change": 0,
            "volume": 0, "high": 0, "low": 0, "open": 0}


@st.cache_data(ttl=60)
def fetch_quotes(tickers: tuple) -> dict:
    """Fetch current-day OHLCV for a list of tickers via yFinance."""
    data = {}
    syms = list(dict.fromkeys(tickers))
    if not syms:
        return data

    def _parse_batch(raw, syms):
        result = {}
        for ticker in syms:
            try:
                hist = raw if len(syms) == 1 else (
                    raw[ticker] if ticker in raw.columns.get_level_values(0) else None
                )
                if hist is None or hist.empty:
                    continue
                hist = hist.dropna(how="all")
                if hist.empty:
                    continue
                curr = float(hist["Close"].iloc[-1])
                if not curr:
                    continue
                prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else curr
                chg  = curr - prev
                pct  = (chg / prev) * 100 if prev else 0
                result[ticker] = {
                    "price":      round(curr, 2),
                    "change":     round(chg, 2),
                    "pct_change": round(pct, 2),
                    "volume":     int(hist["Volume"].sum()),
                    "high":       round(float(hist["High"].max()), 2),
                    "low":        round(float(hist["Low"].min()), 2),
                    "open":       round(float(hist["Open"].iloc[0]), 2),
                }
            except Exception:
                pass
        return result

    try:
        raw = yf.download(
            syms, period="1d", interval="2m",
            group_by="ticker", auto_adjust=True, progress=False, threads=True,
        )
        data.update(_parse_batch(raw, syms))
    except Exception:
        pass

    missing = [t for t in syms if t not in data or data[t]["price"] == 0]
    if missing:
        try:
            raw = yf.download(
                missing, period="5d", interval="1d",
                group_by="ticker", auto_adjust=True, progress=False, threads=True,
            )
            data.update(_parse_batch(raw, missing))
        except Exception:
            pass

    missing = [t for t in syms if t not in data or data[t]["price"] == 0]
    for ticker in missing:
        try:
            t_obj = yf.Ticker(ticker)
            lp = t_obj.fast_info.last_price
            if lp:
                prev_close = t_obj.fast_info.previous_close or lp
                chg = lp - prev_close
                pct = (chg / prev_close * 100) if prev_close else 0
                data[ticker] = {
                    "price":      round(float(lp), 2),
                    "change":     round(float(chg), 2),
                    "pct_change": round(float(pct), 2),
                    "volume":     int(t_obj.fast_info.three_month_average_volume or 0),
                    "high":       round(float(t_obj.fast_info.day_high or lp), 2),
                    "low":        round(float(t_obj.fast_info.day_low  or lp), 2),
                    "open":       round(float(t_obj.fast_info.open or lp), 2),
                }
            else:
                data[ticker] = _empty_quote()
        except Exception:
            data[ticker] = _empty_quote()

    return data


@st.cache_data(ttl=300)
def fetch_history(ticker: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    try:
        return yf.Ticker(ticker).history(period=period, interval=interval)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def fetch_info(ticker: str) -> dict:
    try:
        return yf.Ticker(ticker).info or {}
    except Exception:
        return {}


@st.cache_data(ttl=900)
def fetch_news(ticker: str) -> list:
    try:
        return yf.Ticker(ticker).news or []
    except Exception:
        return []


@st.cache_data(ttl=3600)
def fetch_financials(ticker: str) -> dict:
    try:
        t = yf.Ticker(ticker)
        inc = getattr(t, "income_stmt", None)
        if inc is None or (hasattr(inc, "empty") and inc.empty):
            inc = t.financials
        return {
            "financials":  inc,
            "balance":     t.balance_sheet,
            "cashflow":    t.cashflow,
        }
    except Exception:
        return {}


@st.cache_data(ttl=900)
def fetch_fear_greed() -> dict:
    import urllib.request, json as _json
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://edition.cnn.com/markets/fear-and-greed",
            "Origin":          "https://edition.cnn.com",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = _json.loads(resp.read())
        fg = data.get("fear_and_greed", {})
        return {
            "score":      fg.get("score"),
            "rating":     fg.get("rating", ""),
            "prev_close": fg.get("previous_close"),
            "prev_week":  fg.get("previous_1_week"),
            "prev_month": fg.get("previous_1_month"),
            "prev_year":  fg.get("previous_1_year"),
            "timestamp":  fg.get("timestamp", ""),
        }
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=300)
def compute_market_momentum_score() -> dict:
    """Compute a 1–10 Market Momentum Score from six independent market signals."""
    try:
        spy_raw = yf.download("SPY", period="90d", interval="1d",
                               progress=False, auto_adjust=True)
        vix_raw = yf.download("^VIX", period="90d", interval="1d",
                               progress=False, auto_adjust=True)

        def _close(df):
            if isinstance(df.columns, pd.MultiIndex):
                df = df.droplevel(1, axis=1)
            return df["Close"].dropna()

        spy_close = _close(spy_raw)
        vix_close = _close(vix_raw)

        if len(spy_close) < 50:
            return {"error": "Not enough SPY history"}

        sma50  = float(spy_close.rolling(50).mean().iloc[-1])
        sma200 = float(spy_close.rolling(200).mean().iloc[-1]) if len(spy_close) >= 200 else sma50
        price  = float(spy_close.iloc[-1])
        vix    = float(vix_close.iloc[-1]) if len(vix_close) else 20.0

        delta = spy_close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, 1e-9)
        rsi   = float((100 - 100 / (1 + rs)).iloc[-1])

        ema12    = spy_close.ewm(span=12, adjust=False).mean()
        ema26    = spy_close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_bullish = bool(macd_line.iloc[-1] > signal_line.iloc[-1])

        roc = float((spy_close.iloc[-1] / spy_close.iloc[-21] - 1) * 100) if len(spy_close) >= 21 else 0.0

        trend_score = (1.0 if price > sma50 else 0.0) + (1.0 if price > sma200 else 0.0)

        if vix < 15:        vol_score = 2.0
        elif vix < 20:      vol_score = 1.5
        elif vix < 25:      vol_score = 1.0
        elif vix < 30:      vol_score = 0.5
        else:               vol_score = 0.0

        if rsi > 65:        rsi_score = 2.0
        elif rsi > 55:      rsi_score = 1.5
        elif rsi > 45:      rsi_score = 1.0
        elif rsi > 35:      rsi_score = 0.5
        else:               rsi_score = 0.0

        macd_score = 1.0 if macd_bullish else 0.0
        roc_score  = 1.0 if roc > 0 else 0.0

        breadth_tickers = ("AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META",
                           "TSLA", "PLTR", "ARM",  "SMCI",  "BRK-B", "JPM")
        try:
            bq = fetch_quotes(breadth_tickers)
            pct_up = sum(1 for t in breadth_tickers if (bq.get(t, {}).get("pct_change") or 0) > 0)
            pct_up_ratio = pct_up / len(breadth_tickers)
            if pct_up_ratio >= 0.70:   breadth_score = 2.0
            elif pct_up_ratio >= 0.50: breadth_score = 1.0
            else:                      breadth_score = 0.0
        except Exception:
            breadth_score = 1.0

        raw = trend_score + vol_score + rsi_score + macd_score + roc_score + breadth_score
        score = round(max(1.0, min(10.0, raw)), 1)

        if score <= 2:    label, color = "Extreme Bearish", "#dc2626"
        elif score <= 4:  label, color = "Bearish",         "#f97316"
        elif score <= 6:  label, color = "Neutral",         "#eab308"
        elif score <= 8:  label, color = "Bullish",         "#84cc16"
        else:             label, color = "Extreme Bullish", "#22c55e"

        return {
            "score": score, "label": label, "color": color,
            "components": {
                "Trend (SMA50/200)":  (trend_score,   2.0),
                "Volatility (VIX)":   (vol_score,     2.0),
                "Momentum (RSI-14)":  (rsi_score,     2.0),
                "MACD Crossover":     (macd_score,    1.0),
                "Rate of Change":     (roc_score,     1.0),
                "Market Breadth":     (breadth_score, 2.0),
            },
            "detail": {
                "SPY Price": f"${price:.2f}",
                "SMA-50":    f"${sma50:.2f}",
                "SMA-200":   f"${sma200:.2f}",
                "VIX":       f"{vix:.1f}",
                "RSI-14":    f"{rsi:.1f}",
                "MACD":      "Bullish" if macd_bullish else "Bearish",
                "20d ROC":   f"{roc:+.2f}%",
                "Breadth":   f"{int(pct_up_ratio * 100)}% up",
            },
        }
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=300)
def compute_hedge_signals() -> dict:
    """Compute 8 independent hedge-risk signals."""
    import math

    def _sig(name, desc, triggered, value, error=None):
        return {"name": name, "desc": desc,
                "triggered": bool(triggered), "value": value, "error": error}

    def _flatten(df):
        if isinstance(df.columns, pd.MultiIndex):
            df = df.droplevel(1, axis=1)
        return df

    def _ticker_close(multi_df, ticker):
        try:
            return multi_df[ticker]["Close"].dropna()
        except Exception:
            return pd.Series(dtype=float)

    def _ticker_ohlc(multi_df, ticker):
        try:
            return (multi_df[ticker]["Close"].dropna(),
                    multi_df[ticker]["High"].dropna(),
                    multi_df[ticker]["Low"].dropna())
        except Exception:
            empty = pd.Series(dtype=float)
            return empty, empty, empty

    signals = []

    # Signal 1: Price > 2 ATRs above 21 EMA
    try:
        raw1 = yf.download(["SPY", "QQQ", "IWM", "IYT"], period="90d", interval="1d",
                            progress=False, auto_adjust=True, group_by="ticker")
        extended = []
        readings = []
        for tk in ["SPY", "QQQ", "IWM", "IYT"]:
            close, high, low = _ticker_ohlc(raw1, tk)
            if len(close) < 22:
                continue
            ema21 = close.ewm(span=21, adjust=False).mean()
            prev_c = close.shift(1)
            tr = pd.concat([high - low, (high - prev_c).abs(), (low - prev_c).abs()], axis=1).max(axis=1)
            atr14 = tr.rolling(14).mean()
            now, e21, a14 = float(close.iloc[-1]), float(ema21.iloc[-1]), float(atr14.iloc[-1])
            pct_above = (now - e21) / a14 if a14 > 0 else 0
            readings.append(f"{tk} {pct_above:+.1f}ATR")
            if now > e21 + 2 * a14:
                extended.append(tk)
        triggered = len(extended) > 0
        value = (", ".join(extended) + " extended") if extended else ("  ".join(readings) if readings else "N/A")
        signals.append(_sig("Price > 2 ATRs above 21 EMA",
                             "Any of SPY / QQQ / IWM / IYT more than 2 ATRs above its 21-day EMA?",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("Price > 2 ATRs above 21 EMA",
                             "Any of SPY / QQQ / IWM / IYT more than 2 ATRs above its 21-day EMA?",
                             False, "N/A", str(ex)))

    # Signal 2: A-D Line Divergence
    try:
        raw2 = yf.download(["^GSPC", "^NYAD"], period="60d", interval="1d",
                            progress=False, auto_adjust=True, group_by="ticker")
        spx  = _ticker_close(raw2, "^GSPC")
        nyad = _ticker_close(raw2, "^NYAD")
        if len(spx) >= 20 and len(nyad) >= 10:
            spx_pct  = float(spx.iloc[-1])  / float(spx.rolling(20).max().iloc[-1])  * 100
            nyad_pct = float(nyad.iloc[-1]) / float(nyad.rolling(10).max().iloc[-1]) * 100
            triggered = spx_pct > 98 and nyad_pct < 97
            value = f"SPX {spx_pct:.0f}% of 20d-high · A-D {nyad_pct:.0f}% of 10d-high"
        else:
            triggered, value = False, "Insufficient data"
        signals.append(_sig("A-D Line Divergence",
                             "SPX near 20-day high while NYSE Advance-Decline line is lagging?",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("A-D Line Divergence",
                             "SPX near 20-day high while NYSE Advance-Decline line is lagging?",
                             False, "N/A", str(ex)))

    # Signal 3: Put/Call Ratio < 0.80
    try:
        raw3 = _flatten(yf.download("^PCALL", period="30d", interval="1d",
                                     progress=False, auto_adjust=True))
        pc = raw3["Close"].dropna()
        if len(pc) >= 10:
            ma10      = float(pc.rolling(10).mean().iloc[-1])
            latest_pc = float(pc.iloc[-1])
            triggered = ma10 < 0.80
            value     = f"10d MA = {ma10:.2f}  (latest {latest_pc:.2f})"
        else:
            triggered, value = False, "Insufficient data"
        signals.append(_sig("Put/Call Ratio < 0.80",
                             "10-day MA of CBOE total Put/Call ratio below 0.80?",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("Put/Call Ratio < 0.80",
                             "10-day MA of CBOE total Put/Call ratio below 0.80?",
                             False, "N/A", str(ex)))

    # Signal 4: Dollar Strength
    try:
        raw4 = yf.download(["DX-Y.NYB", "AUDUSD=X", "AUDJPY=X"], period="30d", interval="1d",
                            progress=False, auto_adjust=True, group_by="ticker")
        dxy    = _ticker_close(raw4, "DX-Y.NYB")
        audusd = _ticker_close(raw4, "AUDUSD=X")
        audjpy = _ticker_close(raw4, "AUDJPY=X")
        dxy_strong  = len(dxy)    >= 10 and float(dxy.iloc[-1])    > float(dxy.rolling(10).mean().iloc[-1])
        audusd_weak = len(audusd) >= 10 and float(audusd.iloc[-1]) < float(audusd.rolling(10).mean().iloc[-1])
        audjpy_weak = len(audjpy) >= 10 and float(audjpy.iloc[-1]) < float(audjpy.rolling(10).mean().iloc[-1])
        triggered   = dxy_strong and (audusd_weak or audjpy_weak)
        parts = []
        if len(dxy)    >= 1: parts.append(f"DXY {float(dxy.iloc[-1]):.2f}")
        if len(audusd) >= 1: parts.append(f"AUD/USD {float(audusd.iloc[-1]):.4f}")
        if len(audjpy) >= 1: parts.append(f"AUD/JPY {float(audjpy.iloc[-1]):.2f}")
        value = "  ·  ".join(parts) if parts else "N/A"
        signals.append(_sig("Dollar Strength",
                             "DXY rising above 10-day SMA while AUD/USD and/or AUD/JPY declining?",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("Dollar Strength",
                             "DXY rising above 10-day SMA while AUD/USD and/or AUD/JPY declining?",
                             False, "N/A", str(ex)))

    # Signal 5: VIX near Lower Bollinger Band
    try:
        raw5  = _flatten(yf.download("^VIX", period="60d", interval="1d",
                                      progress=False, auto_adjust=True))
        vix_s = raw5["Close"].dropna()
        if len(vix_s) >= 20:
            sma20    = float(vix_s.rolling(20).mean().iloc[-1])
            std20    = float(vix_s.rolling(20).std().iloc[-1])
            lower_bb = sma20 - 2 * std20
            vix_now  = float(vix_s.iloc[-1])
            triggered = vix_now < sma20 and vix_now <= lower_bb * 1.12
            value     = f"VIX {vix_now:.1f}  |  SMA20 {sma20:.1f}  |  Lower BB {lower_bb:.1f}"
        else:
            triggered, value = False, "Insufficient data"
        signals.append(_sig("VIX Near Lower Bollinger Band",
                             "VIX below its 20-day midline and close to lower Bollinger Band?",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("VIX Near Lower Bollinger Band",
                             "VIX below its 20-day midline and close to lower Bollinger Band?",
                             False, "N/A", str(ex)))

    # Signal 6: VUG/VTV declining
    try:
        raw6 = yf.download(["VUG", "VTV"], period="30d", interval="1d",
                            progress=False, auto_adjust=True, group_by="ticker")
        vug = _ticker_close(raw6, "VUG")
        vtv = _ticker_close(raw6, "VTV")
        if len(vug) >= 11 and len(vtv) >= 11:
            ratio_now = float(vug.iloc[-1])  / float(vtv.iloc[-1])
            ratio_10d = float(vug.iloc[-10]) / float(vtv.iloc[-10])
            triggered = ratio_now < ratio_10d
            chg       = (ratio_now / ratio_10d - 1) * 100
            value     = f"VUG/VTV {ratio_now:.3f}  ({chg:+.1f}% vs 10d ago)"
        else:
            triggered, value = False, "Insufficient data"
        signals.append(_sig("Growth vs Value Declining",
                             "VUG/VTV ratio falling? (rotation from growth to value = risk-off)",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("Growth vs Value Declining",
                             "VUG/VTV ratio falling? (rotation from growth to value = risk-off)",
                             False, "N/A", str(ex)))

    # Signal 7: CBOE Skew > 135
    try:
        raw7  = _flatten(yf.download("^SKEW", period="10d", interval="1d",
                                      progress=False, auto_adjust=True))
        skew_s = raw7["Close"].dropna()
        if len(skew_s) >= 1:
            skew_now  = float(skew_s.iloc[-1])
            triggered = skew_now > 135
            value     = f"SKEW = {skew_now:.1f}"
        else:
            triggered, value = False, "No data"
        signals.append(_sig("CBOE Skew > 135",
                             "CBOE Skew Index above 135? (elevated tail-risk)",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("CBOE Skew > 135",
                             "CBOE Skew Index above 135? (elevated tail-risk)",
                             False, "N/A", str(ex)))

    # Signal 8: CNN Fear & Greed > 85
    try:
        fg = fetch_fear_greed()
        if fg.get("score") is not None:
            score     = float(fg["score"])
            triggered = score > 85
            value     = f"Score = {score:.0f}  ({fg.get('rating', '')})"
        else:
            triggered, value = False, "Unavailable"
        signals.append(_sig("Fear & Greed > 85",
                             "CNN Fear & Greed above 85 (Extreme Greed = contrarian caution)?",
                             triggered, value))
    except Exception as ex:
        signals.append(_sig("Fear & Greed > 85",
                             "CNN Fear & Greed above 85?",
                             False, "N/A", str(ex)))

    n = sum(1 for s in signals if s["triggered"])
    if n <= 2:
        rec_color, rec_label, rec_text = (
            "#22c55e", "Low Risk",
            "Market conditions are calm. No hedge required at this time.",
        )
    elif n <= 4:
        rec_color, rec_label, rec_text = (
            "#f59e0b", "Moderate Risk",
            "Some warning signs present. Consider light protection — 5-10% of portfolio.",
        )
    elif n <= 6:
        rec_color, rec_label, rec_text = (
            "#f97316", "Elevated Risk",
            "Multiple signals active. Hedge recommended — SPY/QQQ puts or reduce equity 20-30%.",
        )
    else:
        rec_color, rec_label, rec_text = (
            "#ef4444", "High Risk",
            "Strong hedge case. Consider inverse ETFs, index puts, or 30-50% equity reduction.",
        )

    return {
        "signals":         signals,
        "triggered_count": n,
        "rec_color":       rec_color,
        "rec_label":       rec_label,
        "rec_text":        rec_text,
    }


@st.cache_data(ttl=120)
def fetch_options_summary(ticker: str) -> dict:
    """Fetch ATM IV, IV skew, and put/call OI ratio for the nearest valid expiry."""
    try:
        t    = yf.Ticker(ticker)
        exps = list(t.options)
        if not exps:
            return {}
        today = datetime.now()
        valid = [e for e in exps if (datetime.strptime(e, "%Y-%m-%d") - today).days >= 7]
        exp = valid[0] if valid else exps[0]
        chain  = t.option_chain(exp)
        calls  = chain.calls.dropna(subset=["impliedVolatility"])
        puts   = chain.puts.dropna(subset=["impliedVolatility"])
        if calls.empty:
            return {}
        spot = float(t.fast_info.last_price or t.fast_info.previous_close or 0)
        if not spot:
            return {}

        def _atm(df):
            return df.iloc[(df["strike"] - spot).abs().argsort().iloc[0]]

        def _iv_at(df, strike):
            row = df[df["strike"] == strike]
            return float(row["impliedVolatility"].iloc[0]) if not row.empty else None

        atm_strike = _atm(calls)["strike"]
        call_iv    = _iv_at(calls, atm_strike)
        put_iv     = _iv_at(puts,  atm_strike)
        atm_iv     = ((call_iv + put_iv) / 2 if call_iv and put_iv else call_iv or put_iv)

        otm_puts  = puts[puts["strike"]  <= spot * 0.96]
        otm_calls = calls[calls["strike"] >= spot * 1.04]
        skew_put_iv  = _iv_at(puts,  _atm(otm_puts)["strike"])  if not otm_puts.empty  else None
        skew_call_iv = _iv_at(calls, _atm(otm_calls)["strike"]) if not otm_calls.empty else None
        skew = (skew_put_iv - skew_call_iv) if skew_put_iv and skew_call_iv else None

        call_oi  = float(calls["openInterest"].fillna(0).sum())
        put_oi   = float(puts["openInterest"].fillna(0).sum())
        pc_ratio = put_oi / call_oi if call_oi else None

        return {
            "exp": exp, "atm_strike": atm_strike,
            "atm_iv": atm_iv, "call_iv": call_iv, "put_iv": put_iv,
            "skew": skew, "skew_put_iv": skew_put_iv, "skew_call_iv": skew_call_iv,
            "pc_ratio": pc_ratio, "total_call_oi": call_oi, "total_put_oi": put_oi,
            "calls": calls, "puts": puts,
        }
    except Exception:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def fetch_unusual_options(
    scan_tickers: tuple = UNUSUAL_SCAN_TICKERS,
    min_volume: int = 150,
    min_ratio: float = 2.0,
    top_n: int = 10,
) -> pd.DataFrame:
    """Scan options chains for unusual activity (Vol/OI ratio)."""
    records = []
    today = datetime.now()

    for ticker in scan_tickers:
        try:
            t    = yf.Ticker(ticker)
            exps = list(t.options)
            if not exps:
                continue
            try:
                fi   = t.fast_info
                spot = fi.last_price or fi.regular_market_price or 0.0
            except Exception:
                spot = 0.0

            valid_exps = [
                e for e in exps
                if (datetime.strptime(e, "%Y-%m-%d") - today).days >= 1
            ][:3]
            if not valid_exps:
                valid_exps = exps[:1]

            for exp in valid_exps:
                chain = t.option_chain(exp)
                for opt_type, df in (("CALL", chain.calls), ("PUT", chain.puts)):
                    if df.empty:
                        continue
                    df = df.copy()
                    vol = df["volume"].fillna(0)
                    oi  = df["openInterest"].fillna(0).replace(0, float("nan"))
                    df["_ratio"] = vol / oi
                    hits = df[(vol >= min_volume) & (df["_ratio"] >= min_ratio)]
                    for _, row in hits.iterrows():
                        records.append({
                            "Ticker":   ticker,
                            "Type":     opt_type,
                            "Stock $":  round(float(spot), 2) if spot else None,
                            "Strike":   row["strike"],
                            "Expiry":   exp,
                            "Volume":   int(row["volume"]),
                            "OI":       int(row["openInterest"]),
                            "Vol/OI":   round(row["_ratio"], 1),
                            "IV %":     round(float(row["impliedVolatility"]) * 100, 1)
                                        if row.get("impliedVolatility") else None,
                            "Last $":   round(float(row["lastPrice"]), 2)
                                        if row.get("lastPrice") else None,
                        })
        except Exception:
            continue

    if not records:
        return pd.DataFrame()

    return (pd.DataFrame(records)
            .sort_values("Vol/OI", ascending=False)
            .head(top_n)
            .reset_index(drop=True))


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_screener_data(tickers: tuple) -> pd.DataFrame:
    """Fetch fundamental screener data for tickers via yfinance. Cached 1 hour."""
    import concurrent.futures

    def _fetch_one(sym):
        try:
            t   = yf.Ticker(sym)
            inf = t.info or {}
            fi  = t.fast_info
            price   = fi.last_price or inf.get("currentPrice") or 0
            prev    = fi.previous_close or price
            chg_pct = ((price - prev) / prev * 100) if prev else 0
            mkt_cap = inf.get("marketCap") or 0
            def _pct(v):
                return round(float(v) * 100, 2) if v and float(v) == float(v) else None
            def _f(v, dec=2):
                try:
                    f = float(v)
                    return round(f, dec) if f == f else None
                except Exception:
                    return None
            return {
                "Symbol":        sym,
                "Company":       inf.get("shortName", sym),
                "Sector":        inf.get("sector") or "—",
                "Industry":      inf.get("industry") or "—",
                "Price":         _f(price, 2),
                "Chg %":         round(float(chg_pct), 2),
                "Mkt Cap ($B)":  round(mkt_cap / 1e9, 1) if mkt_cap else None,
                "P/E":           _f(inf.get("trailingPE"), 1),
                "Fwd P/E":       _f(inf.get("forwardPE"), 1),
                "EPS ($)":       _f(inf.get("trailingEps"), 2),
                "EPS Growth %":  _pct(inf.get("earningsGrowth")),
                "Rev Growth %":  _pct(inf.get("revenueGrowth")),
                "Div Yield %":   _pct(inf.get("dividendYield")),
                "ROE %":         _pct(inf.get("returnOnEquity")),
                "Debt/Eq":       _f(inf.get("debtToEquity"), 2),
                "Net Margin %":  _pct(inf.get("profitMargins")),
                "P/B":           _f(inf.get("priceToBook"), 2),
                "Beta":          _f(inf.get("beta"), 2),
            }
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(_fetch_one, tickers))

    rows = [r for r in results if r]
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ─── FINANCIAL ANALYSIS HELPERS ──────────────────────────────

def _get_row(df: pd.DataFrame, keys: list):
    if df is None or df.empty:
        return None
    for k in keys:
        if k in df.index:
            return df.loc[k]
    return None


def _cagr(series: pd.Series, years: int):
    vals = series.dropna()
    if len(vals) <= years or years <= 0:
        return None
    end   = float(vals.iloc[0])
    start = float(vals.iloc[years])
    if start == 0 or (start < 0 and end < 0):
        return None
    if start < 0:
        return None
    return (end / start) ** (1.0 / years) - 1


def _growth_dict(series) -> dict:
    if series is None:
        return {}
    return {p: _cagr(series, p) for p in (1, 3, 5, 10)}


def _roic_series(fin, bs):
    try:
        ebit   = _get_row(fin, ["EBIT", "Operating Income", "Operating Profit"])
        equity = _get_row(bs, ["Stockholders Equity", "Total Stockholder Equity",
                                "Common Stock Equity", "Total Equity Gross Minority Interest"])
        if ebit is None or equity is None:
            return None
        debt = _get_row(bs, ["Long Term Debt", "Long-Term Debt",
                              "Long Term Debt And Capital Lease Obligation"])
        idx = ebit.index.intersection(equity.index)
        if len(idx) == 0:
            return None
        nopat = ebit[idx] * 0.79
        ic = equity[idx] + (debt[idx].reindex(idx).fillna(0) if debt is not None else 0)
        return nopat / ic
    except Exception:
        return None


def run_value_analysis(ticker: str) -> dict:
    r = {"ticker": ticker}
    try:
        info = fetch_info(ticker)
        fd   = fetch_financials(ticker)
        fin  = fd.get("financials")
        bs   = fd.get("balance")
        cf   = fd.get("cashflow")

        r["info"]     = info
        r["name"]     = info.get("longName") or ticker
        r["sector"]   = info.get("sector", "—")
        r["industry"] = info.get("industry", "—")

        shares = info.get("sharesOutstanding", 1) or 1

        rev = _get_row(fin, ["Total Revenue", "Revenue"])
        r["revenue"]        = rev
        r["revenue_growth"] = _growth_dict(rev)

        ni = _get_row(fin, ["Net Income", "Net Income Common Stockholders",
                             "Net Income Including Noncontrolling Interests"])
        r["net_income"] = ni
        r["eps_ttm"]    = info.get("trailingEps")

        eps_series = _get_row(fin, ["Diluted EPS", "Basic EPS", "EPS",
                                    "Diluted Normalized EPS", "Basic Normalized EPS"])
        if eps_series is None and ni is not None:
            eps_series = ni / shares
        r["eps_growth"] = _growth_dict(eps_series)

        eq = _get_row(bs, ["Stockholders Equity", "Total Stockholder Equity",
                            "Common Stock Equity", "Total Equity Gross Minority Interest"])
        bvps_s = (eq / shares) if eq is not None else None
        r["equity_growth"] = _growth_dict(eq)
        r["bvps"] = float(bvps_s.iloc[0]) if bvps_s is not None and not bvps_s.empty \
                    else info.get("bookValue")

        fcf = _get_row(cf, ["Free Cash Flow"])
        if fcf is None:
            op  = _get_row(cf, ["Operating Cash Flow", "Total Cash From Operating Activities",
                                 "Cash From Operations"])
            cap = _get_row(cf, ["Capital Expenditure", "Purchase Of PPE", "Capital Expenditures"])
            if op is not None and cap is not None:
                fcf = op + cap
        r["fcf"]        = fcf
        r["fcf_growth"] = _growth_dict(fcf)
        fcf_curr        = float(fcf.iloc[0]) if fcf is not None and not fcf.empty else 0

        roic_s           = _roic_series(fin, bs)
        r["roic_series"] = roic_s
        r["roic_growth"] = _growth_dict(roic_s)
        r["roic_current"] = float(roic_s.iloc[0]) if roic_s is not None and not roic_s.empty \
                            else info.get("returnOnEquity")

        ltd = _get_row(bs, ["Long Term Debt", "Long-Term Debt",
                             "Long Term Debt And Capital Lease Obligation"])
        ltd_curr = float(ltd.iloc[0]) if ltd is not None and not ltd.empty else 0
        r["long_term_debt"]    = ltd_curr
        r["debt_payoff_years"] = (ltd_curr / fcf_curr) if fcf_curr > 0 else None

        dep = _get_row(cf, ["Depreciation And Amortization", "Depreciation Amortization Depletion",
                             "Depreciation & Amortization"])
        cap = _get_row(cf, ["Capital Expenditure", "Purchase Of PPE", "Capital Expenditures"])
        if ni is not None and not ni.empty:
            ni0  = float(ni.iloc[0])
            da0  = float(dep.iloc[0]) if dep is not None and not dep.empty else 0
            cap0 = float(cap.iloc[0]) if cap is not None and not cap.empty else 0
            oe   = ni0 + da0 + cap0
            r["owner_earnings"]    = oe
            r["owner_earnings_ps"] = oe / shares
        else:
            r["owner_earnings"] = r["owner_earnings_ps"] = None

        for k in ("returnOnEquity", "returnOnAssets", "trailingPE", "forwardPE", "pegRatio",
                  "priceToBook", "debtToEquity", "currentRatio", "grossMargins",
                  "operatingMargins", "profitMargins", "earningsGrowth",
                  "currentPrice", "marketCap", "totalCash", "totalDebt"):
            r[k] = info.get(k)
        r["price"] = r["currentPrice"] or info.get("regularMarketPrice", 0)

        if not r.get("grossMargins"):
            gp = _get_row(fin, ["Gross Profit"])
            if gp is not None and rev is not None and not rev.empty:
                try:
                    gp0  = float(gp.iloc[0])
                    rev0 = float(rev.iloc[0])
                    if rev0 != 0:
                        r["grossMargins"] = gp0 / rev0
                except Exception:
                    pass

        eps      = r["eps_ttm"] or 0
        hist_g   = (r["eps_growth"].get(5) or r["eps_growth"].get(3) or r["eps_growth"].get(1))
        peg      = info.get("pegRatio")
        tpe      = info.get("trailingPE") or 0
        anal_g_peg = (tpe / (peg * 100)) if (peg and peg > 0 and tpe > 0) else None
        anal_g_ttm = info.get("earningsGrowth") or info.get("revenueGrowth")
        anal_g     = anal_g_peg if anal_g_peg is not None else anal_g_ttm
        rates      = [x for x in [hist_g, anal_g] if x is not None]
        g_est      = min(rates) if rates else None
        if g_est is not None:
            g_est = min(g_est, 0.25)
        r["estimated_growth"] = g_est

        trailing_pe = info.get("trailingPE") or 0
        forward_pe  = info.get("forwardPE")  or 0
        pe_candidates = [p for p in [trailing_pe, forward_pe] if p > 0]
        hist_avg_pe = sum(pe_candidates) / len(pe_candidates) if pe_candidates else 15

        if g_est and eps:
            future_pe  = max(min(2 * g_est * 100, hist_avg_pe), 8)
            future_eps = eps * (1 + g_est) ** 10
            future_px  = future_eps * future_pe
            sticker    = future_px / (1.15 ** 10)
            r["future_pe"]      = round(future_pe, 1)
            r["future_eps_10y"] = round(future_eps, 2)
            r["future_px_10y"]  = round(future_px, 2)
            r["sticker_price"]  = round(sticker, 2)
            r["mos_price"]      = round(sticker / 2, 2)
        else:
            r["future_pe"] = r["future_eps_10y"] = r["future_px_10y"] = \
            r["sticker_price"] = r["mos_price"] = None

        bvps = r["bvps"]
        eps_ttm = r["eps_ttm"]
        if eps_ttm and bvps and eps_ttm > 0 and bvps > 0:
            r["graham_number"] = round((22.5 * eps_ttm * bvps) ** 0.5, 2)
        else:
            r["graham_number"] = None

        if fcf_curr > 0 and shares > 0:
            g_dcf = min(g_est or 0.10, 0.20)
            dr    = 0.10
            tr    = 0.03
            pvs   = [fcf_curr * (1 + g_dcf) ** yr / (1 + dr) ** yr for yr in range(1, 11)]
            tv    = fcf_curr * (1 + g_dcf) ** 10 * (1 + tr) / (dr - tr)
            pv_tv = tv / (1 + dr) ** 10
            cash  = r["totalCash"] or 0
            debt  = r["totalDebt"] or 0
            r["dcf_intrinsic"] = round((sum(pvs) + pv_tv + cash - debt) / shares, 2)
            r["dcf_pvs"]       = pvs
            r["dcf_pv_tv"]     = pv_tv
            r["dcf_g"]         = g_dcf
        else:
            r["dcf_intrinsic"] = r["dcf_pvs"] = r["dcf_pv_tv"] = r["dcf_g"] = None

    except Exception as e:
        r["_error"] = str(e)

    lynch = _classify_peter_lynch(r, r.get("info", {}))
    r["lynch_category"] = lynch["category"]
    r["lynch_subtitle"] = lynch["subtitle"]
    r["lynch_reason"]   = lynch["reason"]
    r["lynch_tips"]     = lynch["tips"]

    return r


def _classify_peter_lynch(data: dict, info: dict) -> dict:
    sector   = data.get("sector", "") or ""
    industry = (info.get("industry") or data.get("industry", "") or "").lower()
    beta     = info.get("beta")
    pb       = info.get("priceToBook")
    trailing_eps = info.get("trailingEps")

    def _best_growth(growth_dict):
        for yrs in (5, 3, 1):
            v = (growth_dict or {}).get(yrs)
            if v is not None:
                return v
        return None

    eps_g = _best_growth(data.get("eps_growth"))
    rev_g = _best_growth(data.get("revenue_growth"))
    g = (eps_g if eps_g is not None else rev_g
         if rev_g is not None else info.get("earningsGrowth")
         if info.get("earningsGrowth") is not None else info.get("revenueGrowth"))

    CYCLICAL_SECTORS  = {"Basic Materials", "Energy", "Industrials", "Consumer Cyclical"}
    CYCLICAL_KEYWORDS = ["airline","auto","steel","chemical","semiconductor",
                         "mining","oil","gas","lumber","fertilizer","shipping"]
    ASSET_SECTORS     = {"Real Estate", "Financials", "Basic Materials"}

    is_turnaround = trailing_eps is not None and trailing_eps < 0
    is_asset_play = pb is not None and pb < 1.5 and sector in ASSET_SECTORS
    is_cyclical   = (sector in CYCLICAL_SECTORS or
                     any(kw in industry for kw in CYCLICAL_KEYWORDS)) and (beta is not None and beta > 1.2)

    if is_turnaround:
        return {"category": "Turnaround", "subtitle": "Recovery Play",
                "reason": f"Trailing EPS is negative (${trailing_eps:.2f}). Lynch's 'Turnarounds' can snap back sharply.",
                "tips": ["Check cash vs. burn rate — liquidity is the #1 risk.",
                         "Look for a concrete catalyst: new management, cost cuts, asset sales.",
                         "Lynch says turnarounds can be the most profitable — and most dangerous — plays.",
                         "Avoid averaging down blindly; wait for signs the bleeding has stopped."]}

    if is_asset_play:
        return {"category": "Asset Play", "subtitle": "Hidden Value",
                "reason": f"P/B is {pb:.2f}x in an asset-heavy sector ({sector}). Trades below the value of what the company owns.",
                "tips": ["Identify the hidden asset: real estate, patents, cash hoard, subsidiary stake.",
                         "Check whether management is incentivised to unlock that value.",
                         "Catalysts: spin-offs, buybacks, activist investors, or a breakup."]}

    if is_cyclical:
        return {"category": "Cyclical", "subtitle": "Timing Key",
                "reason": f"Sector '{sector}' is economically sensitive. Lynch warns buying at the wrong point in the cycle is a common mistake.",
                "tips": ["Lynch's rule: buy cyclicals when P/E is HIGH (earnings trough), sell when P/E is LOW.",
                         "Track inventory levels and capacity-utilisation — they lead earnings.",
                         "Avoid holding through a full downturn; gains can evaporate quickly."]}

    g_pct = g * 100 if g is not None else None

    if g is not None and g >= 0.20:
        return {"category": "Fast Grower", "subtitle": "High Growth",
                "reason": f"5-year EPS/Revenue CAGR ≈ {g_pct:.1f}% — above Lynch's 20% threshold.",
                "tips": ["P/EG ratio (P/E ÷ growth rate) — Lynch liked P/EG < 1.0.",
                         "Verify the growth story is still intact.",
                         "Sell when the growth story ends or the stock becomes widely discovered."]}

    if g is not None and g >= 0.10:
        return {"category": "Stalwart", "subtitle": "Steady",
                "reason": f"5-year EPS/Revenue CAGR ≈ {g_pct:.1f}% — solid but not explosive.",
                "tips": ["Lynch aims for 30–50% gains then rotates into a cheaper name.",
                         "P/E relative to the 5-year average P/E is a useful entry signal.",
                         "Great defensive holding; don't expect a ten-bagger."]}

    return {"category": "Slow Grower", "subtitle": "Mature",
            "reason": (f"5-year EPS/Revenue CAGR ≈ {g_pct:.1f}% — " if g_pct is not None else "Growth data limited — likely ")
                      + "below Lynch's 10% bar.",
            "tips": ["Lynch owns slow growers only for the dividend.",
                     "Check the dividend payout ratio; above 60–70% can signal a cut coming.",
                     "Watch for management returning cash via buybacks."]}


# ─── CHART HELPERS ────────────────────────────────────────────

def _layout(margin=None, **kwargs):
    base = dict(
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        margin=margin if margin is not None else dict(l=60, r=40, t=60, b=40),
    )
    base.update(kwargs)
    return base


def make_candlestick(df: pd.DataFrame, ticker: str, period: str = "") -> go.Figure:
    fig = go.Figure()
    df  = df.copy()

    for n, col, span in [("EMA 8",  "#ef4444", 8),
                          ("EMA 21", "#facc15", 21),
                          ("EMA 34", "#f97316", 34)]:
        if len(df) >= span:
            df[n] = df["Close"].ewm(span=span, adjust=False).mean()
            fig.add_trace(go.Scatter(x=df.index, y=df[n], name=n,
                                     line=dict(color=col, width=1.5),
                                     hovertemplate=f"{n}: %{{y:.2f}}<extra></extra>"))

    for n, col, minp in [("SMA 50",  "#22c55e", 50),
                          ("SMA 100", "#60a5fa", 100),
                          ("SMA 200", "#a855f7", 200)]:
        if len(df) >= minp:
            df[n] = df["Close"].rolling(minp).mean()
            fig.add_trace(go.Scatter(x=df.index, y=df[n], name=n,
                                     line=dict(color=col, width=1.5, dash="dash"),
                                     hovertemplate=f"{n}: %{{y:.2f}}<extra></extra>"))

    fig.add_trace(go.Candlestick(
        x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
        name=ticker,
        increasing=dict(line=dict(color=COLORS["up"]),   fillcolor=COLORS["up"]),
        decreasing=dict(line=dict(color=COLORS["down"]), fillcolor=COLORS["down"]),
    ))
    vol_colors = [COLORS["up"] if c >= o else COLORS["down"]
                  for c, o in zip(df["Close"], df["Open"])]
    fig.add_trace(go.Bar(x=df.index, y=df["Volume"], name="Volume",
                         marker_color=vol_colors, yaxis="y2", opacity=0.3))

    fig.update_layout(
        title=f"<b>{ticker}</b>  {period}",
        yaxis=dict(title="Price ($)", gridcolor=COLORS["border"]),
        yaxis2=dict(title="Volume", overlaying="y", side="right", showgrid=False),
        xaxis=dict(rangeslider=dict(visible=False), gridcolor=COLORS["border"]),
        height=500, showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        **_layout(),
    )
    return fig


def make_sector_bar(sector_data: dict) -> go.Figure:
    names  = list(sector_data.keys())
    pcts   = [sector_data[n].get("pct_change", 0) for n in names]
    colors = [COLORS["up"] if p >= 0 else COLORS["down"] for p in pcts]
    fig = go.Figure(go.Bar(
        x=names, y=pcts, marker_color=colors,
        text=[f"{p:+.2f}%" for p in pcts],
        textposition="outside", textfont=dict(color=COLORS["text"], size=11),
    ))
    fig.update_layout(title="<b>Sector Performance</b>",
                      yaxis=dict(title="% Change", gridcolor=COLORS["border"]),
                      xaxis=dict(tickangle=-30), height=380, **_layout())
    return fig


def make_lightweight_chart(ticker: str, hist: pd.DataFrame) -> str:
    """Return self-contained HTML using TradingView Lightweight Charts.
    Includes: Candlestick, Volume, 8 EMA, 21 EMA, 50 SMA, 200 SMA, RSI(14).
    No indicator limits — all data pre-calculated in Python and injected as JSON.
    """
    import json

    close = hist["Close"]

    # ── Indicators ────────────────────────────────────────────
    ema8   = close.ewm(span=8,   adjust=False).mean()
    ema21  = close.ewm(span=21,  adjust=False).mean()
    sma50  = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()

    # RSI-14 using Wilder smoothing (EWM with com=13)
    delta  = close.diff()
    gain   = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss   = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    rsi    = (100 - 100 / (1 + gain / loss.replace(0, float("nan")))).round(2)

    # ── Serialisers ───────────────────────────────────────────
    def _ts(idx):
        try:    return int(idx.timestamp())
        except: return int(pd.Timestamp(idx).timestamp())  # noqa: E722

    def _candles(df):
        rows = []
        for ts, row in df.iterrows():
            o, h, l, c, v = row["Open"], row["High"], row["Low"], row["Close"], row.get("Volume", 0)
            if any(pd.isna(x) for x in [o, h, l, c]):
                continue
            rows.append({"time": _ts(ts),
                         "open":  round(float(o), 4), "high": round(float(h), 4),
                         "low":   round(float(l), 4), "close": round(float(c), 4),
                         "volume": int(v) if pd.notna(v) else 0})
        return rows

    def _line(series):
        return [{"time": _ts(ts), "value": round(float(v), 4)}
                for ts, v in series.dropna().items()]

    candles  = json.dumps(_candles(hist))
    vol_data = json.dumps([{"time": r["time"], "value": r["volume"],
                             "color": "#22c55e44" if hist["Close"].iloc[i] >= hist["Open"].iloc[i]
                             else "#ef444444"}
                            for i, r in enumerate(_candles(hist))])
    ema8_d   = json.dumps(_line(ema8))
    ema21_d  = json.dumps(_line(ema21))
    sma50_d  = json.dumps(_line(sma50))
    sma200_d = json.dumps(_line(sma200))
    rsi_d    = json.dumps(_line(rsi))
    rsi_times = [r["time"] for r in _line(rsi)]
    ob_d     = json.dumps([{"time": t, "value": 70} for t in rsi_times])
    os_d     = json.dumps([{"time": t, "value": 30} for t in rsi_times])
    mid_d    = json.dumps([{"time": t, "value": 50} for t in rsi_times])

    return f"""
<!DOCTYPE html><html><head>
<meta charset="utf-8">
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0e1117; }}
  #wrapper {{ width: 100%; display: flex; flex-direction: column; gap: 2px; }}
  #chart_main {{ width: 100%; height: 420px; }}
  #chart_rsi  {{ width: 100%; height: 160px; }}
  .legend {{ position: absolute; top: 6px; left: 8px; z-index: 10;
             display: flex; gap: 10px; flex-wrap: wrap; font-size: 11px;
             font-family: monospace; pointer-events: none; }}
  .leg {{ display: flex; align-items: center; gap: 4px; }}
  .leg-dot {{ width: 18px; height: 2px; border-radius: 1px; }}
  #rsi_label {{ position: absolute; bottom: 6px; left: 8px; z-index: 10;
                font-size: 11px; font-family: monospace; color: #f59e0b; }}
</style>
</head><body>
<div id="wrapper">
  <div style="position:relative">
    <div id="chart_main"></div>
    <div class="legend">
      <div class="leg"><div class="leg-dot" style="background:#f59e0b"></div><span style="color:#f59e0b">8 EMA</span></div>
      <div class="leg"><div class="leg-dot" style="background:#22c55e"></div><span style="color:#22c55e">21 EMA</span></div>
      <div class="leg"><div class="leg-dot" style="background:#60a5fa"></div><span style="color:#60a5fa">50 SMA</span></div>
      <div class="leg"><div class="leg-dot" style="background:#a78bfa"></div><span style="color:#a78bfa">200 SMA</span></div>
    </div>
  </div>
  <div style="position:relative">
    <div id="chart_rsi"></div>
    <div id="rsi_label">RSI 14</div>
  </div>
</div>
<script>
const BG = '#0e1117', GRID = '#1e293b', TEXT = '#94a3b8', BORDER = '#2d3748';

function makeChart(el, height) {{
  return LightweightCharts.createChart(el, {{
    width:  el.clientWidth,
    height: height,
    layout:         {{ background: {{ color: BG }}, textColor: TEXT }},
    grid:           {{ vertLines: {{ color: GRID }}, horzLines: {{ color: GRID }} }},
    crosshair:      {{ mode: LightweightCharts.CrosshairMode.Normal }},
    rightPriceScale: {{ borderColor: BORDER }},
    timeScale:      {{ borderColor: BORDER, timeVisible: true, rightOffset: 5 }},
    handleScroll:   true,
    handleScale:    true,
  }});
}}

const mainEl = document.getElementById('chart_main');
const rsiEl  = document.getElementById('chart_rsi');
const mainChart = makeChart(mainEl, 420);
const rsiChart  = makeChart(rsiEl,  160);

// ── Candlestick ───────────────────────────────────────────────
const candleSeries = mainChart.addCandlestickSeries({{
  upColor: '#22c55e', downColor: '#ef4444',
  borderUpColor: '#22c55e', borderDownColor: '#ef4444',
  wickUpColor: '#22c55e', wickDownColor: '#ef4444',
}});
const candleData = {candles};
candleSeries.setData(candleData.map(d => ({{time:d.time,open:d.open,high:d.high,low:d.low,close:d.close}})));

// ── Volume ────────────────────────────────────────────────────
const volSeries = mainChart.addHistogramSeries({{
  priceFormat: {{ type: 'volume' }},
  priceScaleId: 'vol',
}});
mainChart.priceScale('vol').applyOptions({{
  scaleMargins: {{ top: 0.85, bottom: 0 }},
}});
volSeries.setData({vol_data});

// ── Moving Averages ───────────────────────────────────────────
const lineOpts = (color, width=1) => ({{ color, lineWidth: width, priceLineVisible: false, lastValueVisible: false }});
mainChart.addLineSeries(lineOpts('#f59e0b')).setData({ema8_d});
mainChart.addLineSeries(lineOpts('#22c55e')).setData({ema21_d});
mainChart.addLineSeries(lineOpts('#60a5fa')).setData({sma50_d});
mainChart.addLineSeries(lineOpts('#a78bfa')).setData({sma200_d});

// ── RSI ───────────────────────────────────────────────────────
const rsiLine = rsiChart.addLineSeries({{ color: '#f59e0b', lineWidth: 1, priceLineVisible: false, lastValueVisible: true }});
rsiLine.setData({rsi_d});
const lvlOpts = (color) => ({{ color, lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Dashed, priceLineVisible: false, lastValueVisible: false }});
rsiChart.addLineSeries(lvlOpts('#ef444488')).setData({ob_d});
rsiChart.addLineSeries(lvlOpts('#22c55e88')).setData({os_d});
rsiChart.addLineSeries(lvlOpts('#ffffff22')).setData({mid_d});
rsiChart.priceScale('right').applyOptions({{ autoScale: false, minimum: 0, maximum: 100 }});

// ── Sync crosshair ────────────────────────────────────────────
mainChart.subscribeCrosshairMove(p => {{
  if (!p.time) {{ rsiChart.clearCrosshairPosition(); return; }}
  const rsiVal = p.seriesData?.get(rsiLine)?.value;
  if (rsiVal !== undefined)
    rsiChart.setCrosshairPosition(rsiVal, p.time, rsiLine);
}});
rsiChart.subscribeCrosshairMove(p => {{
  if (!p.time) {{ mainChart.clearCrosshairPosition(); return; }}
  const cv = p.seriesData?.get(candleSeries) ?? p.seriesData?.values().next().value;
  if (cv)
    mainChart.setCrosshairPosition(cv.close ?? cv.value, p.time, candleSeries);
}});

// ── Sync time range ───────────────────────────────────────────
let syncing = false;
mainChart.timeScale().subscribeVisibleTimeRangeChange(r => {{
  if (syncing || !r) return; syncing = true;
  rsiChart.timeScale().setVisibleRange(r); syncing = false;
}});
rsiChart.timeScale().subscribeVisibleTimeRangeChange(r => {{
  if (syncing || !r) return; syncing = true;
  mainChart.timeScale().setVisibleRange(r); syncing = false;
}});

// ── Fit + resize ──────────────────────────────────────────────
mainChart.timeScale().fitContent();
rsiChart.timeScale().fitContent();

window.addEventListener('resize', () => {{
  const w = document.getElementById('wrapper').clientWidth;
  mainChart.resize(w, 420);
  rsiChart.resize(w, 160);
}});
</script>
</body></html>
"""


def make_bar_history(series: pd.Series, title: str) -> go.Figure:
    if series is None or series.empty:
        return go.Figure()
    vals  = series.dropna().iloc[::-1]
    scale = 1e9 if vals.abs().max() > 1e9 else (1e6 if vals.abs().max() > 1e6 else 1)
    unit  = "B" if scale == 1e9 else ("M" if scale == 1e6 else "")
    xlab  = [str(d.year) if hasattr(d, "year") else str(d) for d in vals.index]
    fig = go.Figure(go.Bar(
        x=xlab, y=vals / scale,
        marker_color=[COLORS["up"] if v >= 0 else COLORS["down"] for v in vals],
        text=[f"${v/scale:.1f}{unit}" for v in vals],
        textposition="outside", textfont=dict(size=10, color=COLORS["text"]),
    ))
    fig.update_layout(title=f"<b>{title}</b>",
                      yaxis=dict(gridcolor=COLORS["border"],
                                 title=f"(${unit})" if unit else "$"),
                      height=280, **_layout(margin=dict(l=40, r=20, t=50, b=40)))
    return fig


# ─── SMALL HELPERS ───────────────────────────────────────────

def _clr(v):
    return "color: #22c55e" if v > 0 else ("color: #ef4444" if v < 0 else "")


def _badge(val):
    if val is None:
        return "N/A"
    try:
        pct = float(val) * 100
    except (TypeError, ValueError):
        return "N/A"
    import math
    if math.isnan(pct):
        return "N/A"
    if pct >= 10:  return f"🟢 {pct:+.1f}%"
    if pct >= 0:   return f"🟡 {pct:+.1f}%"
    return             f"🔴 {pct:+.1f}%"


def _fmt_b(val):
    if val is None: return "—"
    try:
        f = float(val)
    except (TypeError, ValueError):
        return "—"
    if f != f: return "—"
    return f"${f/1e9:.2f}B"


# ─── SIDEBAR ─────────────────────────────────────────────────

def render_sidebar():
    with st.sidebar:
        st.markdown("## MarketBoard")
        st.caption(f"Updated: {datetime.now().strftime('%a %b %d  %H:%M')}")
        st.divider()
        st.info("Data: **yFinance** (delayed)\n\nNot financial advice.")
        if st.button("Refresh All Data", width="stretch", type="primary"):
            st.cache_data.clear()
            st.session_state.vi_cache = {}
            try:
                db.clear_screener_cache()
            except Exception:
                pass
            st.rerun()
        st.divider()
        st.caption("**Methodologies included:**")
        st.caption("- Phil Town – Rule #1 / Big Five")
        st.caption("- Warren Buffett – Moat + Owner Earnings")
        st.caption("- DCF – Discounted Cash Flow")
        st.divider()
        st.caption("Tip: Value Investing tab caches results per ticker for the session.")


# ─── TAB 1: MARKET OVERVIEW ──────────────────────────────────

def render_market_overview():
    st.markdown("## Market Overview")

    all_index_syms = tuple(dict.fromkeys(
        list(FUTURES.values()) + list(INDICES.values())
    ))
    with st.spinner("Loading futures & indices…"):
        q = fetch_quotes(all_index_syms)

    # Futures row
    st.caption("**Futures**")
    fut_cols = st.columns(len(FUTURES))
    for i, (label, sym) in enumerate(FUTURES.items()):
        d = q.get(sym, _empty_quote())
        if d["price"] and d["price"] > 0:
            fut_cols[i].metric(label=label, value=f"{d['price']:,.2f}",
                               delta=f"{d['pct_change']:+.2f}%")
        else:
            fut_cols[i].metric(label=label, value="—", delta=None)

    st.divider()

    # Spot indices row
    cols = st.columns(len(INDICES))
    for i, (label, sym) in enumerate(INDICES.items()):
        d = q.get(sym, _empty_quote())
        delta_color = "inverse" if sym == "^VIX" else "normal"
        cols[i].metric(label=label, value=f"{d['price']:,.2f}",
                       delta=f"{d['pct_change']:+.2f}%", delta_color=delta_color)

    st.divider()

    # CNN Fear & Greed + Market Momentum
    with st.spinner("Loading sentiment & momentum…"):
        fg = fetch_fear_greed()
        mm = compute_market_momentum_score()

    st.markdown("#### Market Sentiment & Momentum")
    col_mm, col_fg = st.columns(2)

    with col_fg:
        if fg.get("error"):
            st.warning(f"Fear & Greed unavailable: {fg['error']}")
        elif fg.get("score") is not None:
            score = fg["score"]
            if score <= 25:   fg_color, fg_label = "#dc2626", "Extreme Fear"
            elif score <= 45: fg_color, fg_label = "#f97316", "Fear"
            elif score <= 55: fg_color, fg_label = "#eab308", "Neutral"
            elif score <= 75: fg_color, fg_label = "#84cc16", "Greed"
            else:             fg_color, fg_label = "#22c55e", "Extreme Greed"

            st.markdown(f"<p style='text-align:center;font-size:0.85rem;color:{COLORS['muted']};margin-bottom:0'>CNN Fear & Greed Index</p>", unsafe_allow_html=True)

            fig_fg = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(score, 1),
                title={"text": f"<b>{fg_label}</b>", "font": {"size": 15, "color": fg_color}},
                number={"font": {"color": fg_color, "size": 44}, "valueformat": ".1f"},
                gauge={
                    "axis": {"range": [0, 100], "tickvals": [0, 25, 45, 55, 75, 100],
                             "ticktext": ["0", "25", "45", "55", "75", "100"],
                             "tickcolor": COLORS["muted"]},
                    "bar": {"color": fg_color, "thickness": 0.25},
                    "bgcolor": COLORS["card"], "bordercolor": COLORS["border"],
                    "steps": [
                        {"range": [0,  25],  "color": "rgba(220,38,38,0.18)"},
                        {"range": [25, 45],  "color": "rgba(249,115,22,0.18)"},
                        {"range": [45, 55],  "color": "rgba(234,179,8,0.18)"},
                        {"range": [55, 75],  "color": "rgba(132,204,22,0.18)"},
                        {"range": [75, 100], "color": "rgba(34,197,94,0.18)"},
                    ],
                },
            ))
            fig_fg.update_layout(height=240, paper_bgcolor=COLORS["bg"],
                                 font=dict(color=COLORS["text"]),
                                 margin=dict(l=20, r=20, t=40, b=0))
            st.plotly_chart(fig_fg, width="stretch")

            fig_fg_bar = go.Figure()
            for lbl, lo, hi, col in [
                ("Extreme Fear", 0, 25, "#dc2626"), ("Fear", 25, 45, "#f97316"),
                ("Neutral", 45, 55, "#eab308"), ("Greed", 55, 75, "#84cc16"),
                ("Extreme Greed", 75, 100, "#22c55e"),
            ]:
                fig_fg_bar.add_trace(go.Bar(
                    x=[hi - lo], y=[""], orientation="h", name=lbl,
                    marker_color=col, text=lbl, textposition="inside",
                    insidetextanchor="middle", textfont=dict(size=9, color="white"),
                ))
            fig_fg_bar.add_vline(x=score, line_color="white", line_width=3,
                                 annotation_text=f"  {score:.0f}",
                                 annotation_font_color="white", annotation_font_size=12)
            fig_fg_bar.update_layout(
                barmode="stack",
                xaxis=dict(range=[0, 100], showticklabels=False),
                yaxis=dict(showticklabels=False),
                height=50, showlegend=False,
                paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
                margin=dict(l=10, r=10, t=0, b=0),
            )
            st.plotly_chart(fig_fg_bar, width="stretch")

            st.markdown("<p style='font-size:0.8rem;color:#94a3b8;margin:8px 0 4px'>Historical</p>", unsafe_allow_html=True)
            history = [("Yesterday", fg.get("prev_close")), ("1 Week", fg.get("prev_week")),
                       ("1 Month", fg.get("prev_month")), ("1 Year", fg.get("prev_year"))]
            h_cols = st.columns(4)
            for i, (lbl, val) in enumerate(history):
                if val is not None:
                    diff  = score - val
                    arrow = "▲" if diff > 0 else "▼"
                    clr   = COLORS["up"] if diff > 0 else COLORS["down"]
                    h_cols[i].markdown(
                        f"<div style='text-align:center'>"
                        f"<div style='font-size:0.7rem;color:{COLORS['muted']}'>{lbl}</div>"
                        f"<div style='font-size:1rem;font-weight:600'>{val:.0f}</div>"
                        f"<div style='font-size:0.7rem;color:{clr}'>{arrow}{abs(diff):.0f}</div>"
                        f"</div>", unsafe_allow_html=True,
                    )

    with col_mm:
        if mm.get("error"):
            st.warning(f"Momentum score unavailable: {mm['error']}")
        else:
            mm_score = mm["score"]
            mm_color = mm["color"]
            mm_label = mm["label"]

            st.markdown(f"<p style='text-align:center;font-size:0.85rem;color:{COLORS['muted']};margin-bottom:0'>Market Momentum Score</p>", unsafe_allow_html=True)

            fig_mm = go.Figure(go.Indicator(
                mode="gauge+number",
                value=mm_score,
                title={"text": f"<b>{mm_label}</b>", "font": {"size": 15, "color": mm_color}},
                number={"font": {"color": mm_color, "size": 44}, "valueformat": ".1f"},
                gauge={
                    "axis": {"range": [1, 10],
                             "tickvals": [1, 3, 5, 7, 9, 10],
                             "ticktext": ["1", "3", "5", "7", "9", "10"],
                             "tickcolor": COLORS["muted"]},
                    "bar": {"color": mm_color, "thickness": 0.25},
                    "bgcolor": COLORS["card"], "bordercolor": COLORS["border"],
                    "steps": [
                        {"range": [1,  3],  "color": "rgba(220,38,38,0.18)"},
                        {"range": [3,  5],  "color": "rgba(249,115,22,0.18)"},
                        {"range": [5,  7],  "color": "rgba(234,179,8,0.18)"},
                        {"range": [7,  9],  "color": "rgba(132,204,22,0.18)"},
                        {"range": [9, 10],  "color": "rgba(34,197,94,0.18)"},
                    ],
                },
            ))
            fig_mm.update_layout(height=240, paper_bgcolor=COLORS["bg"],
                                 font=dict(color=COLORS["text"]),
                                 margin=dict(l=20, r=20, t=40, b=0))
            st.plotly_chart(fig_mm, width="stretch")

            fig_mm_bar = go.Figure()
            for lbl, lo, hi, col in [
                ("Ext. Bearish", 1, 3, "#dc2626"), ("Bearish", 3, 5, "#f97316"),
                ("Neutral", 5, 7, "#eab308"), ("Bullish", 7, 9, "#84cc16"),
                ("Ext. Bullish", 9, 10, "#22c55e"),
            ]:
                fig_mm_bar.add_trace(go.Bar(
                    x=[hi - lo], y=[""], orientation="h", name=lbl,
                    marker_color=col, text=lbl, textposition="inside",
                    insidetextanchor="middle", textfont=dict(size=9, color="white"),
                ))
            fig_mm_bar.add_vline(x=mm_score - 1, line_color="white", line_width=3,
                                 annotation_text=f"  {mm_score:.1f}",
                                 annotation_font_color="white", annotation_font_size=12)
            fig_mm_bar.update_layout(
                barmode="stack",
                xaxis=dict(range=[0, 9], showticklabels=False),
                yaxis=dict(showticklabels=False),
                height=50, showlegend=False,
                paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
                margin=dict(l=10, r=10, t=0, b=0),
            )
            st.plotly_chart(fig_mm_bar, width="stretch")

            st.markdown("<p style='font-size:0.8rem;color:#94a3b8;margin:8px 0 4px'>Signal Breakdown</p>", unsafe_allow_html=True)
            sig_cols = st.columns(2)
            for i, (sig_name, (sig_val, sig_max)) in enumerate(mm["components"].items()):
                filled  = int(round(sig_val / sig_max * 5))
                dots    = "●" * filled + "○" * (5 - filled)
                dot_clr = mm_color if filled >= 3 else ("#f97316" if filled >= 2 else "#dc2626")
                short_name = sig_name.split("(")[0].strip()
                sig_cols[i % 2].markdown(
                    f"<div style='font-size:0.72rem;color:{COLORS['muted']}'>{short_name}</div>"
                    f"<div style='color:{dot_clr};letter-spacing:1px;font-size:0.85rem'>{dots} "
                    f"<span style='font-size:0.7rem;color:{COLORS['muted']}'>{sig_val:.1f}/{sig_max:.0f}</span></div>",
                    unsafe_allow_html=True,
                )

    st.divider()

    # Sector heatmap
    with st.spinner("Loading sectors…"):
        sq = fetch_quotes(tuple(SECTORS.values()))
    sect_data = {n: sq.get(etf, _empty_quote()) for n, etf in SECTORS.items()}

    c1, c2 = st.columns([3, 2])
    with c1:
        st.plotly_chart(make_sector_bar(sect_data), width="stretch")
    with c2:
        st.markdown("#### Sector Summary")
        rows = [{"Sector": n, "ETF": SECTORS[n],
                 "Price": sect_data[n]["price"],
                 "% Chg": sect_data[n]["pct_change"]}
                for n in SECTORS]
        df = pd.DataFrame(rows).sort_values("% Chg", ascending=False)
        st.dataframe(
            df.style.map(_clr, subset=["% Chg"])
              .format({"Price": "${:.2f}", "% Chg": "{:+.2f}%"}),
            hide_index=True, width="stretch", height=380,
        )

    st.divider()

    # Hedge Risk Gauge
    st.markdown("#### Hedge Risk Gauge")
    st.caption("Scores 8 independent market signals to determine whether conditions warrant portfolio hedges.")

    with st.spinner("Computing hedge signals…"):
        hd = compute_hedge_signals()

    n_triggered = hd["triggered_count"]
    h_signals   = hd["signals"]

    hcol_gauge, hcol_checks = st.columns([1, 2])

    with hcol_gauge:
        fig_hedge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=n_triggered,
            number={"font": {"color": hd["rec_color"], "size": 52}, "suffix": "/8"},
            title={"text": f"<b>{hd['rec_label']}</b>",
                   "font": {"size": 14, "color": hd["rec_color"]}},
            gauge={
                "axis": {"range": [0, 8], "tickvals": [0, 2, 4, 6, 8],
                         "ticktext": ["0", "2", "4", "6", "8"], "tickcolor": COLORS["muted"]},
                "bar":       {"color": hd["rec_color"], "thickness": 0.22},
                "bgcolor":   COLORS["card"], "bordercolor": COLORS["border"],
                "steps": [
                    {"range": [0, 2], "color": "rgba(34,197,94,0.18)"},
                    {"range": [2, 4], "color": "rgba(245,158,11,0.18)"},
                    {"range": [4, 6], "color": "rgba(249,115,22,0.18)"},
                    {"range": [6, 8], "color": "rgba(239,68,68,0.22)"},
                ],
                "threshold": {"line": {"color": hd["rec_color"], "width": 3},
                              "thickness": 0.75, "value": n_triggered},
            },
        ))
        fig_hedge.update_layout(height=240, paper_bgcolor=COLORS["bg"],
                                font=dict(color=COLORS["text"]),
                                margin=dict(l=20, r=20, t=50, b=10))
        st.plotly_chart(fig_hedge, width="stretch")

        for zone_label, zone_range, zone_col in [
            ("0–2  Low Risk",      [0, 2], "#22c55e"),
            ("3–4  Moderate Risk", [2, 4], "#f59e0b"),
            ("5–6  Elevated Risk", [4, 6], "#f97316"),
            ("7–8  High Risk",     [6, 8], "#ef4444"),
        ]:
            dot = "●" if n_triggered > zone_range[0] and n_triggered <= zone_range[1] else "○"
            st.markdown(f'<span style="color:{zone_col};font-size:0.82rem">{dot} {zone_label}</span>',
                        unsafe_allow_html=True)

    with hcol_checks:
        left_sigs  = h_signals[:4]
        right_sigs = h_signals[4:]
        sc_l, sc_r = st.columns(2)

        def _render_signal_card(col, sig):
            icon   = "✅" if sig["triggered"] else "⬜"
            bg     = "rgba(34,197,94,0.08)"  if sig["triggered"] else "rgba(255,255,255,0.03)"
            border = "#22c55e"               if sig["triggered"] else COLORS["border"]
            err_note = (f'<br><span style="color:#ef4444;font-size:0.72rem">⚠ {sig["error"][:40]}</span>'
                        if sig.get("error") else "")
            col.markdown(
                f'<div style="background:{bg};border:1px solid {border};border-radius:8px;'
                f'padding:10px 12px;margin-bottom:8px;">'
                f'<div style="font-size:0.82rem;font-weight:700;color:{COLORS["text"]}">'
                f'{icon} {sig["name"]}</div>'
                f'<div style="font-size:0.75rem;color:{COLORS["muted"]};margin-top:2px">'
                f'{sig["desc"]}</div>'
                f'<div style="font-size:0.78rem;color:{"#22c55e" if sig["triggered"] else COLORS["muted"]};'
                f'margin-top:4px;font-family:monospace">{sig["value"]}{err_note}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        for sig in left_sigs:
            _render_signal_card(sc_l, sig)
        for sig in right_sigs:
            _render_signal_card(sc_r, sig)

    st.markdown(
        f'<div style="background:{hd["rec_color"]}22;border-left:4px solid {hd["rec_color"]};'
        f'border-radius:6px;padding:10px 16px;margin-top:4px;">'
        f'<span style="color:{hd["rec_color"]};font-weight:700">{hd["rec_label"]}  '
        f'({n_triggered}/8 signals)</span>'
        f'<span style="color:{COLORS["text"]};font-size:0.88rem">  —  {hd["rec_text"]}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    st.divider()

    # Movers
    LARGE_CAPS = ("AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META",
                  "TSLA", "PLTR", "ARM",  "SMCI",  "BRK-B", "JPM")
    MID_CAPS   = ("DKNG", "ROKU", "SNAP", "LYFT",  "RBLX", "RIVN",
                  "SOFI", "HOOD", "MARA", "PATH",  "W",    "BILL")
    SMALL_CAPS = ("PLUG", "AMC",  "GME",  "IONQ",  "LAZR", "WKHS",
                  "NKLA", "SPWR", "OPEN", "GRPN",  "CLOV", "OPRA")

    ALL_MOVERS = LARGE_CAPS + MID_CAPS + SMALL_CAPS
    with st.spinner("Loading movers…"):
        mq = fetch_quotes(ALL_MOVERS)

    def _mover_table(tickers):
        rows = [{"Ticker": t,
                 "Price":  mq[t]["price"],  "Open":  mq[t]["open"],
                 "High":   mq[t]["high"],   "Low":   mq[t]["low"],
                 "Chg":    mq[t]["change"], "% Chg": mq[t]["pct_change"],
                 "Volume": mq[t]["volume"]} for t in tickers if t in mq]
        st.dataframe(
            pd.DataFrame(rows)
              .style.map(_clr, subset=["Chg", "% Chg"])
              .format({"Price": "${:.2f}", "Open": "${:.2f}", "High": "${:.2f}",
                       "Low":   "${:.2f}", "Chg":  "{:+.2f}", "% Chg": "{:+.2f}%",
                       "Volume": "{:,.0f}"}),
            hide_index=True, width="stretch",
        )

    with st.expander("Large-Cap Movers", expanded=False):
        st.caption("AAPL · MSFT · NVDA · GOOGL · AMZN · META · TSLA · PLTR · ARM · SMCI · BRK-B · JPM")
        _mover_table(LARGE_CAPS)

    with st.expander("Mid-Cap Movers", expanded=False):
        st.caption("DKNG · ROKU · SNAP · LYFT · RBLX · RIVN · SOFI · HOOD · MARA · PATH · W · BILL")
        _mover_table(MID_CAPS)

    with st.expander("Small-Cap Movers", expanded=False):
        st.caption("PLUG · AMC · GME · IONQ · LAZR · WKHS · NKLA · SPWR · OPEN · GRPN · CLOV · OPRA")
        _mover_table(SMALL_CAPS)



# ─── TAB 2: STOCK RESEARCH ───────────────────────────────────

def _make_valuation_chart(ticker: str, info: dict,
                           val_pe: float = 15.0,
                           yf_period: str = "5y",
                           metric: str = "eps") -> go.Figure | None:
    try:
        price_hist = fetch_history(ticker, period=yf_period, interval="1d")
        if price_hist.empty:
            return None

        fd     = fetch_financials(ticker)
        fin    = fd.get("financials")
        cf     = fd.get("cashflow")
        shares = info.get("sharesOutstanding", 1) or 1
        current_year = pd.Timestamp.today().year

        per_share_by_year: dict[int, float] = {}

        if metric == "fcf_share":
            if cf is not None and not cf.empty:
                for col in cf.columns:
                    try:
                        year = col.year if hasattr(col, "year") else int(str(col)[:4])
                    except Exception:
                        continue
                    val = None
                    if "Free Cash Flow" in cf.index and pd.notna(cf.loc["Free Cash Flow", col]):
                        val = float(cf.loc["Free Cash Flow", col])
                    else:
                        ocf = next((float(cf.loc[k, col]) for k in
                                    ["Operating Cash Flow", "Total Cash From Operating Activities"]
                                    if k in cf.index and pd.notna(cf.loc[k, col])), None)
                        cap = next((float(cf.loc[k, col]) for k in
                                    ["Capital Expenditure", "Purchase Of Property Plant And Equipment"]
                                    if k in cf.index and pd.notna(cf.loc[k, col])), None)
                        if ocf is not None and cap is not None:
                            val = ocf + cap
                    if val is not None and val > 0:
                        per_share_by_year[year] = val / shares
            ttm_fcf = info.get("freeCashflow")
            if ttm_fcf and float(ttm_fcf) > 0:
                if not per_share_by_year or max(per_share_by_year) < current_year:
                    per_share_by_year[current_year] = float(ttm_fcf) / shares
            metric_label = "FCF/Share"
        else:
            if fin is not None and not fin.empty:
                for col in fin.columns:
                    try:
                        year = col.year if hasattr(col, "year") else int(str(col)[:4])
                    except Exception:
                        continue
                    val = None
                    for key in ["Diluted EPS", "Basic EPS"]:
                        if key in fin.index and pd.notna(fin.loc[key, col]):
                            val = float(fin.loc[key, col])
                            break
                    if val is None:
                        for k in ["Net Income", "Net Income Common Stockholders"]:
                            if k in fin.index and pd.notna(fin.loc[k, col]):
                                val = float(fin.loc[k, col]) / shares
                                break
                    if val is not None and val > 0:
                        per_share_by_year[year] = val
            eps_ttm = info.get("trailingEps")
            if eps_ttm and float(eps_ttm) > 0:
                if not per_share_by_year or max(per_share_by_year) < current_year:
                    per_share_by_year[current_year] = float(eps_ttm)
            metric_label = "EPS"

        if not per_share_by_year:
            return None

        sorted_years = sorted(per_share_by_year.keys())
        dates  = price_hist.index
        prices = price_hist["Close"].values
        iv_vals = []
        for dt in dates:
            yr  = dt.year if hasattr(dt, "year") else int(str(dt)[:4])
            val = per_share_by_year.get(sorted_years[0])
            for y in sorted(sorted_years, reverse=True):
                if y <= yr:
                    val = per_share_by_year[y]
                    break
            iv_vals.append(max(val * val_pe, 0))

        zeros = [0.0] * len(dates)
        fig   = go.Figure()

        fig.add_trace(go.Scatter(
            x=list(dates) + list(dates)[::-1],
            y=iv_vals + zeros[::-1],
            fill="toself", fillcolor="rgba(99,102,241,0.22)",
            line=dict(color="rgba(0,0,0,0)"),
            name=f"Intrinsic Value ({metric_label} × {val_pe:.1f}×)",
            hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=list(dates), y=iv_vals, mode="lines",
            line=dict(color=COLORS["warn"], width=1.5, dash="dash"),
            name=f"Valuation Line ({metric_label} × {val_pe:.1f}×)",
        ))
        fig.add_trace(go.Scatter(
            x=list(dates), y=list(prices), mode="lines",
            line=dict(color=COLORS["text"], width=1.8), name="Stock Price",
        ))

        fig.update_layout(
            title=f"<b>Valuation Chart</b>  —  Price vs Intrinsic Value ({metric_label})",
            yaxis=dict(tickprefix="$", gridcolor=COLORS["border"]),
            legend=dict(orientation="h", y=-0.18, font=dict(size=11)),
            hovermode="x unified", height=420,
            **_layout(margin=dict(l=60, r=30, t=50, b=70)),
        )
        return fig
    except Exception:
        return None


def _make_sr_financial_charts(ticker: str) -> list:
    try:
        fd  = fetch_financials(ticker)
        fin = fd.get("financials")
        bs  = fd.get("balance")
        if fin is None:
            return [None] * 6
    except Exception:
        return [None] * 6

    def _ylab(s):
        if s is None or s.empty:
            return [], [], "B", 1e9
        v = s.dropna().iloc[::-1]
        mx = v.abs().max()
        sc = 1e9 if mx > 1e9 else (1e6 if mx > 1e6 else 1)
        un = "B" if sc == 1e9 else ("M" if sc == 1e6 else "")
        xl = [str(d.year) if hasattr(d, "year") else str(d) for d in v.index]
        return v, xl, un, sc

    rev = _get_row(fin, ["Total Revenue", "Revenue"])
    ni  = _get_row(fin, ["Net Income", "Net Income Common Stockholders",
                          "Net Income Including Noncontrolling Interests"])
    eps_s = _get_row(fin, ["Diluted EPS", "Basic EPS", "Diluted Normalized EPS"])
    fcf_s = None
    try:
        cf  = fd.get("cashflow")
        fcf_s = _get_row(cf, ["Free Cash Flow"])
        if fcf_s is None:
            op  = _get_row(cf, ["Operating Cash Flow", "Total Cash From Operating Activities"])
            cap = _get_row(cf, ["Capital Expenditure", "Purchase Of PPE"])
            if op is not None and cap is not None:
                fcf_s = op + cap
    except Exception:
        pass

    eq  = _get_row(bs, ["Stockholders Equity", "Total Stockholder Equity",
                          "Common Stock Equity", "Total Equity Gross Minority Interest"])
    ltd = _get_row(bs, ["Long Term Debt", "Long-Term Debt",
                         "Long Term Debt And Capital Lease Obligation"])

    # 1. Revenue vs Net Income
    fig1 = go.Figure()
    if rev is not None:
        rv, xl, un, sc = _ylab(rev)
        fig1.add_trace(go.Bar(x=xl, y=rv / sc, marker_color=COLORS["accent"], name="Revenue",
                              text=[f"${v/sc:.1f}{un}" for v in rv],
                              textposition="outside", textfont=dict(size=9)))
    if ni is not None:
        nv, xl2, un2, sc2 = _ylab(ni)
        fig1.add_trace(go.Scatter(x=xl2, y=nv / sc2, mode="lines+markers",
                                  line=dict(color=COLORS["up"], width=2),
                                  marker=dict(size=6, color=COLORS["up"]),
                                  name="Net Income", yaxis="y2"))
        fig1.update_layout(yaxis2=dict(overlaying="y", side="right", showgrid=False,
                                       tickprefix="$", ticksuffix=un2, color=COLORS["muted"]))
    fig1.update_layout(title="<b>Revenue vs Net Income</b>",
                       yaxis=dict(tickprefix="$", ticksuffix=un if rev is not None else "",
                                  gridcolor=COLORS["border"]),
                       legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
                       height=300, **_layout(margin=dict(l=50, r=50, t=45, b=50)))

    # 2. EPS
    fig2 = go.Figure()
    if eps_s is not None:
        ev, xl, un, sc = _ylab(eps_s)
        fig2.add_trace(go.Bar(x=xl, y=ev / sc,
                              marker_color=[COLORS["up"] if v >= 0 else COLORS["down"] for v in ev],
                              text=[f"${v/sc:.2f}" for v in ev],
                              textposition="outside", textfont=dict(size=9), name="EPS"))
    fig2.update_layout(title="<b>EPS (Diluted)</b>",
                       yaxis=dict(tickprefix="$", gridcolor=COLORS["border"]),
                       showlegend=False, height=300,
                       **_layout(margin=dict(l=50, r=20, t=45, b=50)))

    # 3. Free Cash Flow
    fig3 = go.Figure()
    if fcf_s is not None:
        fv, xl, un, sc = _ylab(fcf_s)
        fig3.add_trace(go.Bar(x=xl, y=fv / sc,
                              marker_color=[COLORS["accent"] if v >= 0 else COLORS["down"] for v in fv],
                              text=[f"${v/sc:.1f}{un}" for v in fv],
                              textposition="outside", textfont=dict(size=9), name="FCF"))
    fig3.update_layout(title="<b>Free Cash Flow</b>",
                       yaxis=dict(tickprefix="$", ticksuffix=un if fcf_s is not None else "",
                                  gridcolor=COLORS["border"]),
                       showlegend=False, height=300,
                       **_layout(margin=dict(l=50, r=20, t=45, b=50)))

    # 4. Profit Margins
    fig4 = go.Figure()
    if rev is not None and not rev.empty:
        gross_p = _get_row(fin, ["Gross Profit"])
        op_inc  = _get_row(fin, ["EBIT", "Operating Income", "Operating Profit"])
        rv = rev.dropna()
        for series, label, color in [
            (gross_p, "Gross Margin",     COLORS["up"]),
            (op_inc,  "Operating Margin", COLORS["warn"]),
            (ni,      "Net Margin",       COLORS["accent"]),
        ]:
            if series is not None:
                idx = rv.index.intersection(series.index)
                if len(idx):
                    margin = (series[idx] / rv[idx] * 100).dropna().iloc[::-1]
                    xl = [str(d.year) if hasattr(d, "year") else str(d) for d in margin.index]
                    fig4.add_trace(go.Scatter(x=xl, y=margin.values, mode="lines+markers",
                                              line=dict(color=color, width=2),
                                              marker=dict(size=6), name=label))
    fig4.update_layout(title="<b>Profit Margins</b>",
                       yaxis=dict(ticksuffix="%", gridcolor=COLORS["border"]),
                       legend=dict(orientation="h", y=-0.3, font=dict(size=9)),
                       height=300, **_layout(margin=dict(l=50, r=20, t=45, b=65)))

    # 5. Debt vs Equity
    fig5 = go.Figure()
    if ltd is not None:
        dv, xl, un, sc = _ylab(ltd)
        fig5.add_trace(go.Bar(x=xl, y=dv / sc, marker_color=COLORS["down"], name="Long-Term Debt",
                              text=[f"${v/sc:.1f}{un}" for v in dv],
                              textposition="outside", textfont=dict(size=9)))
    if eq is not None:
        qv, xl2, un2, sc2 = _ylab(eq)
        fig5.add_trace(go.Scatter(x=xl2, y=qv / sc2, mode="lines+markers",
                                  line=dict(color=COLORS["up"], width=2),
                                  marker=dict(size=6, color=COLORS["up"]),
                                  name="Equity", yaxis="y2"))
        fig5.update_layout(yaxis2=dict(overlaying="y", side="right", showgrid=False,
                                       tickprefix="$", ticksuffix=un2 if eq is not None else "",
                                       color=COLORS["muted"]))
    fig5.update_layout(title="<b>Debt vs Equity</b>",
                       yaxis=dict(tickprefix="$", ticksuffix=un if ltd is not None else "",
                                  gridcolor=COLORS["border"]),
                       legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
                       height=300, **_layout(margin=dict(l=50, r=50, t=45, b=50)))

    # 6. Return on Equity
    fig6 = go.Figure()
    if ni is not None and eq is not None:
        idx = ni.index.intersection(eq.index)
        if len(idx):
            roe = (ni[idx] / eq[idx].replace(0, float("nan")) * 100).dropna().iloc[::-1]
            xl  = [str(d.year) if hasattr(d, "year") else str(d) for d in roe.index]
            fig6.add_trace(go.Bar(x=xl, y=roe.values,
                                  marker_color=[COLORS["warn"] if v >= 0 else COLORS["down"] for v in roe],
                                  text=[f"{v:.1f}%" for v in roe],
                                  textposition="outside", textfont=dict(size=9), name="ROE %"))
    fig6.update_layout(title="<b>Return on Equity (%)</b>",
                       yaxis=dict(ticksuffix="%", gridcolor=COLORS["border"]),
                       showlegend=False, height=300,
                       **_layout(margin=dict(l=50, r=20, t=45, b=50)))

    return [fig1, fig2, fig3, fig4, fig5, fig6]


def render_stock_research():
    st.markdown("## Stock Research")

    VALID_INTERVALS = {
        "5d":  ["15m", "30m", "1h", "1d"],
        "1mo": ["1h",  "1d",  "1wk"],
        "3mo": ["1d",  "1wk"],
        "6mo": ["1d",  "1wk"],
        "1y":  ["1d",  "1wk"],
        "2y":  ["1d",  "1wk", "1mo"],
        "5y":  ["1d",  "1wk", "1mo"],
    }

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        ticker = st.text_input("Ticker Symbol", value="AAPL", key="sr_ticker").upper().strip()
    with c2:
        period = st.selectbox("Period", ["5d", "1mo", "3mo", "6mo", "1y", "2y", "5y"], index=4)
    with c3:
        interval = st.selectbox("Interval", VALID_INTERVALS[period], key=f"sr_interval_{period}")

    if not ticker:
        return

    with st.spinner(f"Loading {ticker}…"):
        hist = fetch_history(ticker, period=period, interval=interval)
        info = fetch_info(ticker)

    if hist.empty:
        st.error(f"No data for **{ticker}**. Check the symbol.")
        return

    name   = info.get("longName", ticker)
    close  = hist["Close"].dropna()
    last   = float(close.iloc[-1]) if not close.empty else float(
             info.get("currentPrice") or info.get("regularMarketPrice") or
             yf.Ticker(ticker).fast_info.last_price or 0)
    prev   = float(close.iloc[-2]) if len(close) > 1 else float(
             info.get("previousClose") or info.get("regularMarketPreviousClose") or last)
    chg    = last - prev
    pct    = (chg / prev * 100) if prev else 0

    eps_ttm  = info.get("trailingEps") or 0
    pe_ttm   = info.get("trailingPE")  or 0
    div_yld  = info.get("dividendYield") or 0
    eg_raw   = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
    sector   = info.get("sector", "")
    exchange = info.get("exchange", info.get("quoteType", ""))

    val_pe_hdr = 15.0
    iv_hdr     = eps_ttm * val_pe_hdr if eps_ttm > 0 else 0
    if iv_hdr > 0:
        val_badge = (f'<span style="background:#ef4444;color:#fff;padding:2px 8px;'
                     f'border-radius:4px;font-size:11px;font-weight:700">OVERVALUED</span>'
                     if last > iv_hdr else
                     f'<span style="background:#22c55e;color:#000;padding:2px 8px;'
                     f'border-radius:4px;font-size:11px;font-weight:700">UNDERVALUED</span>')
    else:
        val_badge = ""

    sector_badge = (f'<span style="background:#1e3a5f;color:#93c5fd;padding:2px 8px;'
                    f'border-radius:4px;font-size:11px">{sector}</span> ' if sector else "")
    exch_badge   = (f'<span style="background:#2d3250;color:#94a3b8;padding:2px 8px;'
                    f'border-radius:4px;font-size:11px">{exchange}</span> ' if exchange else "")

    st.markdown(f"### {name} &nbsp; `{ticker}`")
    st.markdown(f"{sector_badge}{exch_badge}{val_badge}", unsafe_allow_html=True)
    st.write("")

    hc = st.columns(7)
    hc[0].metric("Price",      f"${last:,.2f}",   f"{pct:+.2f}%")
    hc[1].metric("P/E (TTM)",  f"{pe_ttm:.1f}×"   if pe_ttm else "—")
    hc[2].metric("EPS (TTM)",  f"${eps_ttm:.2f}"  if eps_ttm else "—")
    hc[3].metric("EPS Growth", f"{eg_raw*100:+.1f}%" if eg_raw else "—")
    hc[4].metric("Div Yield",  f"{div_yld*100:.2f}%" if div_yld else "—")
    hc[5].metric("Val. P/E",   f"{val_pe_hdr:.1f}×")
    hc[6].metric("Mkt Cap",    _fmt_b(info.get("marketCap")))

    st.divider()

    # Valuation Chart
    vc_col, pe_col = st.columns([5, 1])
    with pe_col:
        val_pe    = st.number_input("Val. P/E ×", min_value=5.0, max_value=60.0,
                                    value=15.0, step=0.5, key="sr_val_pe")
        vc_metric = st.radio("Metric", ["EPS", "FCF/Share"], index=0, key="sr_vc_metric")
        vc_period = st.radio("Period", ["1Y", "3Y", "5Y", "10Y"], index=2, key="sr_vc_period")
    vc_period_map = {"1Y": "1y", "3Y": "3y", "5Y": "5y", "10Y": "10y"}
    with vc_col:
        vc = _make_valuation_chart(ticker, info,
                                   val_pe=float(val_pe),
                                   yf_period=vc_period_map[vc_period],
                                   metric=vc_metric.lower().replace("/", "_"))
        if vc:
            st.plotly_chart(vc, width="stretch")
        else:
            st.caption("Valuation chart unavailable — no historical EPS/FCF data.")

    # Annual Fundamentals table
    st.markdown("#### Annual Fundamentals")
    try:
        _fd  = fetch_financials(ticker)
        _fin = _fd.get("financials")
        _bs  = _fd.get("balance")
        _cf  = _fd.get("cashflow")
        _shares = info.get("sharesOutstanding", 1) or 1
        if _fin is not None:
            _rev  = _get_row(_fin, ["Total Revenue", "Revenue"])
            _ni   = _get_row(_fin, ["Net Income", "Net Income Common Stockholders",
                                    "Net Income Including Noncontrolling Interests"])
            _eps  = _get_row(_fin, ["Diluted EPS", "Basic EPS", "Diluted Normalized EPS"])
            _gp   = _get_row(_fin, ["Gross Profit"])
            _oi   = _get_row(_fin, ["EBIT", "Operating Income", "Operating Profit"])
            _fcf  = _get_row(_cf,  ["Free Cash Flow"]) if _cf is not None else None
            if _fcf is None and _cf is not None:
                _op  = _get_row(_cf, ["Operating Cash Flow", "Total Cash From Operating Activities"])
                _cap = _get_row(_cf, ["Capital Expenditure", "Purchase Of PPE"])
                if _op is not None and _cap is not None:
                    _fcf = _op + _cap
            _eq  = _get_row(_bs, ["Stockholders Equity", "Total Stockholder Equity",
                                   "Common Stock Equity"]) if _bs is not None else None
            _ltd = _get_row(_bs, ["Long Term Debt", "Long-Term Debt",
                                   "Long Term Debt And Capital Lease Obligation"]) if _bs is not None else None
            _div = info.get("dividendRate")

            def _sc(v, d=2):
                if v is None or (hasattr(v, '__float__') and np.isnan(float(v))):
                    return "—"
                f = float(v)
                if abs(f) >= 1e9: return f"${f/1e9:.1f}B"
                if abs(f) >= 1e6: return f"${f/1e6:.0f}M"
                return f"${f:.{d}f}"

            all_idx = set()
            for s in [_rev, _ni, _eps, _fcf, _gp, _oi, _eq, _ltd]:
                if s is not None:
                    all_idx.update(s.dropna().index.tolist())

            def _ok(v):
                if v is None: return None
                try:
                    f = float(v)
                    return None if (f != f) else f
                except Exception:
                    return None

            fund_rows = []
            for dt in sorted(all_idx, reverse=True):
                def _gv(s):
                    if s is None: return None
                    return s.get(dt) if dt in s.index else None
                rev_v = _ok(_gv(_rev)); ni_v  = _ok(_gv(_ni));  eps_v = _ok(_gv(_eps))
                fcf_v = _ok(_gv(_fcf)); gp_v  = _ok(_gv(_gp));  oi_v  = _ok(_gv(_oi))
                eq_v  = _ok(_gv(_eq));  ltd_v = _ok(_gv(_ltd))
                gm  = (gp_v  / rev_v * 100) if (gp_v  is not None and rev_v) else None
                nm  = (ni_v  / rev_v * 100) if (ni_v  is not None and rev_v) else None
                roe = (ni_v  / eq_v  * 100) if (ni_v  is not None and eq_v and eq_v != 0) else None
                de  = (ltd_v / eq_v)        if (ltd_v is not None and eq_v and eq_v != 0) else None
                fund_rows.append({
                    "Year":           str(dt.year) if hasattr(dt, "year") else str(dt),
                    "EPS":            f"${eps_v:.2f}"           if eps_v is not None else "—",
                    "FCF/Share":      f"${fcf_v/_shares:.2f}"   if fcf_v is not None else "—",
                    "Revenue":        _sc(rev_v),
                    "Net Income":     _sc(ni_v),
                    "Free Cash Flow": _sc(fcf_v),
                    "Gross Margin":   f"{gm:.1f}%"  if gm  is not None else "—",
                    "Net Margin":     f"{nm:.1f}%"  if nm  is not None else "—",
                    "ROE":            f"{roe:.1f}%" if roe is not None else "—",
                    "Debt/Equity":    f"{de:.2f}"   if de  is not None else "—",
                    "Dividend":       f"${_div:.2f}" if _div else "—",
                })
            if fund_rows:
                st.dataframe(pd.DataFrame(fund_rows), hide_index=True, width="stretch")
            else:
                st.caption("Annual fundamentals unavailable.")
    except Exception:
        st.caption("Annual fundamentals unavailable.")

    # Financial History Charts
    st.divider()
    st.markdown("#### Financial History")
    with st.spinner("Loading financial charts…"):
        fcharts = _make_sr_financial_charts(ticker)

    if any(f is not None for f in fcharts):
        titles = ["Revenue vs Net Income", "EPS (Diluted)",
                  "Free Cash Flow", "Profit Margins", "Debt vs Equity", "Return on Equity (%)"]
        row1 = st.columns(3)
        row2 = st.columns(3)
        for i, (col, fig) in enumerate(zip(row1 + row2, fcharts)):
            with col:
                if fig is not None:
                    st.plotly_chart(fig, width="stretch")
                else:
                    st.caption(f"{titles[i]} — data unavailable")
    else:
        st.caption("Financial history charts unavailable for this ticker.")

    st.divider()

    # Market Sentiment & Implied Volatility
    st.markdown("#### Market Sentiment & Implied Volatility")
    with st.spinner("Loading options & sentiment…"):
        opt = fetch_options_summary(ticker)

    hv30 = None
    if len(hist) >= 22:
        hv30 = float(hist["Close"].pct_change().dropna().tail(22).std() * (252 ** 0.5))

    sent_col, iv_col = st.columns(2)

    with sent_col:
        st.markdown("##### Analyst Sentiment")
        rec_mean   = info.get("recommendationMean")
        rec_key    = (info.get("recommendationKey") or "").replace("_", " ").title()
        n_analysts = info.get("numberOfAnalystOpinions") or 0

        if rec_mean:
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=rec_mean,
                number={"valueformat": ".2f", "font": {"size": 28, "color": "#e2e8f0"}},
                gauge={
                    "axis": {"range": [1, 5], "tickvals": [1, 2, 3, 4, 5],
                             "ticktext": ["Str. Buy", "Buy", "Hold", "Sell", "Str. Sell"],
                             "tickcolor": "#94a3b8", "tickfont": {"size": 11, "color": "#cbd5e1"}},
                    "bar": {"color": "#ffffff", "thickness": 0.25},
                    "bgcolor": "#1a1f2e", "bordercolor": "#2d3250",
                    "steps": [
                        {"range": [1.0, 1.8], "color": "#14532d"},
                        {"range": [1.8, 2.6], "color": "#166534"},
                        {"range": [2.6, 3.4], "color": "#854d0e"},
                        {"range": [3.4, 4.2], "color": "#991b1b"},
                        {"range": [4.2, 5.0], "color": "#7f1d1d"},
                    ],
                    "threshold": {"line": {"color": "#f8fafc", "width": 3},
                                  "thickness": 0.85, "value": rec_mean},
                },
                title={"text": f"{rec_key or 'Analyst'} · {n_analysts} analysts",
                       "font": {"size": 13, "color": "#94a3b8"}},
                domain={"x": [0, 1], "y": [0.1, 1]},
            ))
            fig_gauge.update_layout(height=260, margin=dict(t=50, b=55, l=40, r=40),
                                    paper_bgcolor="#0e1117", font_color="#e2e8f0")
            st.plotly_chart(fig_gauge, width="stretch")
        else:
            st.caption("No analyst rating available for this ticker.")

        short_pct  = info.get("shortPercentOfFloat")
        short_days = info.get("shortRatio")
        inst_pct   = info.get("heldPercentInstitutions")
        ins_pct    = info.get("heldPercentInsiders")

        sa, sb = st.columns(2)
        sa.metric("Short Float %",   f"{short_pct*100:.1f}%"  if short_pct  else "—")
        sb.metric("Days to Cover",   f"{short_days:.1f}d"     if short_days else "—")
        sc, sd = st.columns(2)
        sc.metric("Institutional %", f"{inst_pct*100:.1f}%"   if inst_pct   else "—")
        sd.metric("Insider %",       f"{ins_pct*100:.1f}%"    if ins_pct    else "—")

    with iv_col:
        st.markdown("##### Implied Volatility")
        if opt:
            atm_iv     = opt.get("atm_iv")
            iv_premium = (atm_iv - hv30) if atm_iv and hv30 else None
            skew       = opt.get("skew")
            pc_ratio   = opt.get("pc_ratio")
            exp_label  = opt.get("exp", "")

            ia, ib = st.columns(2)
            ia.metric("ATM IV",          f"{atm_iv*100:.1f}%"      if atm_iv     else "—")
            ib.metric("HV30 (Realized)", f"{hv30*100:.1f}%"        if hv30       else "—")
            ic, id_ = st.columns(2)
            ic.metric("IV Premium",  f"{iv_premium*100:+.1f}%"     if iv_premium else "—",
                      delta=("Rich" if iv_premium and iv_premium > 0 else "Cheap"),
                      delta_color="inverse")
            id_.metric("IV Skew",   f"{skew*100:+.1f}%"            if skew       else "—")
            ie, _ = st.columns(2)
            ie.metric("Put/Call OI", f"{pc_ratio:.2f}x"             if pc_ratio   else "—")

            calls_smile = (opt["calls"][["strike", "impliedVolatility"]]
                           .dropna()
                           .query("strike >= @last * 0.75 and strike <= @last * 1.30"))
            puts_smile  = (opt["puts"][["strike", "impliedVolatility"]]
                           .dropna()
                           .query("strike >= @last * 0.75 and strike <= @last * 1.30"))

            fig_smile = go.Figure()
            if not puts_smile.empty:
                fig_smile.add_trace(go.Scatter(
                    x=puts_smile["strike"], y=puts_smile["impliedVolatility"] * 100,
                    mode="lines+markers", name="Puts",
                    line=dict(color="#ef4444", width=2), marker=dict(size=5),
                ))
            if not calls_smile.empty:
                fig_smile.add_trace(go.Scatter(
                    x=calls_smile["strike"], y=calls_smile["impliedVolatility"] * 100,
                    mode="lines+markers", name="Calls",
                    line=dict(color="#22c55e", width=2), marker=dict(size=5),
                ))
            fig_smile.add_vline(x=last, line_dash="dash", line_color="#f59e0b",
                                annotation_text=f"Spot ${last:,.2f}",
                                annotation_position="top right",
                                annotation_font=dict(color="#f59e0b", size=11))
            fig_smile.update_layout(
                title=dict(text=f"IV Smile · {exp_label}", font=dict(size=13)),
                xaxis_title="Strike", yaxis_title="IV (%)",
                height=270, margin=dict(t=40, b=30, l=40, r=20),
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                font_color="#e2e8f0", legend=dict(orientation="h", y=1.12),
                xaxis=dict(gridcolor="#1e293b"), yaxis=dict(gridcolor="#1e293b"),
            )
            st.plotly_chart(fig_smile, width="stretch")
        else:
            st.caption(f"No options data for **{ticker}**.")
            if hv30:
                st.metric("HV30 (Realized Vol)", f"{hv30*100:.1f}%")

    st.divider()

    cl, cr = st.columns([1, 2])
    with cl:
        st.markdown("#### Key Metrics")
        def _v(key, fmt):
            val = info.get(key)
            return fmt(val) if val is not None else "—"

        kv = {
            "Sector":            info.get("sector", "—"),
            "Industry":          info.get("industry", "—"),
            "Revenue (TTM)":     _v("totalRevenue",       lambda v: f"${v/1e9:.2f}B"),
            "Net Income":        _v("netIncomeToCommon",  lambda v: f"${v/1e9:.2f}B"),
            "Gross Margin":      _v("grossMargins",       lambda v: f"{v*100:.1f}%"),
            "Operating Margin":  _v("operatingMargins",   lambda v: f"{v*100:.1f}%"),
            "Net Margin":        _v("profitMargins",      lambda v: f"{v*100:.1f}%"),
            "ROE":               _v("returnOnEquity",     lambda v: f"{v*100:.1f}%"),
            "ROA":               _v("returnOnAssets",     lambda v: f"{v*100:.1f}%"),
            "EPS (TTM)":         _v("trailingEps",        lambda v: f"${v:.2f}"),
            "P/E (TTM)":         _v("trailingPE",         lambda v: f"{v:.1f}x"),
            "P/E (Fwd)":         _v("forwardPE",          lambda v: f"{v:.1f}x"),
            "PEG Ratio":         _v("pegRatio",           lambda v: f"{v:.2f}"),
            "Price/Book":        _v("priceToBook",        lambda v: f"{v:.2f}x"),
            "Debt/Equity":       _v("debtToEquity",       lambda v: f"{v/100:.2f}x"),
            "Current Ratio":     _v("currentRatio",       lambda v: f"{v:.2f}"),
            "Div. Yield":        _v("dividendYield",      lambda v: f"{v:.2f}%"),
            "Beta":              _v("beta",               lambda v: f"{v:.2f}"),
        }
        for k, v in kv.items():
            st.write(f"**{k}:** {v}")

    with cr:
        st.markdown("#### Business Summary")
        desc = info.get("longBusinessSummary", "No description available.")
        st.write(desc[:1200] + "…" if len(desc) > 1200 else desc)

        st.markdown("#### Recent News")
        try:
            news = fetch_news(ticker)
            shown = 0
            for a in news:
                c = a.get("content", a)
                title = c.get("title", "")
                url   = (c.get("canonicalUrl") or c.get("clickThroughUrl") or {}).get("url", "#")
                pub   = (c.get("provider") or {}).get("displayName", "") or c.get("publisher", "")
                if title:
                    st.markdown(f"- [{title}]({url})  —  *{pub}*")
                    shown += 1
                if shown >= 6:
                    break
            if shown == 0:
                st.caption("No news available.")
        except Exception:
            st.caption("News unavailable.")

    # ── Major Holders & Insider Transactions ─────────────────────────────────
    st.divider()
    st.markdown("#### Major Holders")

    t = yf.Ticker(ticker)

    def _fmt_shares(v):
        try:
            v = float(v)
            if v >= 1e9: return f"{v/1e9:.2f}B"
            if v >= 1e6: return f"{v/1e6:.2f}M"
            if v >= 1e3: return f"{v/1e3:.1f}K"
            return f"{v:,.0f}"
        except Exception:
            return str(v)

    def _fmt_val(v):
        try:
            v = float(v)
            if v >= 1e9: return f"${v/1e9:.2f}B"
            if v >= 1e6: return f"${v/1e6:.1f}M"
            return f"${v:,.0f}"
        except Exception:
            return str(v)

    def _fmt_pct_col(v):
        try:
            f = float(v)
            return f"{f*100:.2f}%" if f < 2 else f"{f:.2f}%"
        except Exception:
            return str(v)

    col_mh, col_inst = st.columns([1, 2])

    with col_mh:
        st.caption("Ownership Breakdown")
        try:
            mh = t.major_holders
            if mh is not None and not mh.empty:
                rows = []
                cols = list(mh.columns)
                for idx, row in mh.iterrows():
                    if "Breakdown" in cols:
                        raw_val = row["Value"] if "Value" in cols else row.iloc[0]
                        lbl     = row["Breakdown"]
                    elif len(row) >= 2:
                        raw_val = row.iloc[0]
                        lbl     = row.iloc[1]
                    else:
                        raw_val = row.iloc[0]
                        lbl     = idx
                    try:
                        fv = float(str(raw_val).replace("%","").replace(",",""))
                        val_str = f"{fv:,.0f}" if fv > 10 else f"{fv*100:.2f}%"
                    except Exception:
                        val_str = str(raw_val)
                    rows.append({"Category": str(lbl), "Value": val_str})
                st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
            else:
                st.caption("No ownership data available.")
        except Exception as e:
            st.caption(f"Ownership data unavailable. ({e})")

    with col_inst:
        st.caption("Top Institutional Holders")
        try:
            inst = t.institutional_holders
            if inst is not None and not inst.empty:
                inst.columns = [c.strip() for c in inst.columns]
                top = inst.head(6).copy()
                COL_MAP = {
                    "Holder":        "Holder",
                    "Shares":        "Shares",
                    "Date Reported": "Date",
                    "dateReported":  "Date",
                    "% Out":         "% Out",
                    "pctHeld":       "% Out",
                    "Value":         "Value",
                }
                keep, rename, seen = [], {}, set()
                for src, dst in COL_MAP.items():
                    if src in top.columns and dst not in seen:
                        keep.append(src); rename[src] = dst; seen.add(dst)
                top = top[keep].rename(columns=rename)
                if "Shares" in top.columns:
                    top["Shares"] = top["Shares"].apply(lambda v: _fmt_shares(v) if pd.notna(v) else "—")
                if "% Out" in top.columns:
                    top["% Out"] = top["% Out"].apply(lambda v: _fmt_pct_col(v) if pd.notna(v) else "—")
                if "Value" in top.columns:
                    top["Value"] = top["Value"].apply(lambda v: _fmt_val(v) if pd.notna(v) else "—")
                if "Date" in top.columns:
                    top["Date"] = top["Date"].apply(lambda v: str(v)[:10] if pd.notna(v) else "—")
                st.dataframe(top, hide_index=True, use_container_width=True)
            else:
                st.caption("No institutional holder data available.")
        except Exception as e:
            st.caption(f"Institutional data unavailable. ({e})")

    # ── Insider Transactions ──────────────────────────────────────────────────
    st.markdown("#### Insider Transactions")
    try:
        it = t.insider_transactions
        if it is not None and not it.empty:
            it.columns = [c.strip() for c in it.columns]
            INSIDER_MAP = {
                "Insider Trading": "Insider",
                "Name":            "Insider",
                "Insider":         "Insider",
                "Position":        "Position",
                "Transaction":     "Transaction",
                "Text":            "Transaction",
                "Shares":          "Shares",
                "Value":           "Value",
                "Start Date":      "Date",
                "Date":            "Date",
            }
            keep_i, rename_i, seen_i = [], {}, set()
            for src, dst in INSIDER_MAP.items():
                if src in it.columns and dst not in seen_i:
                    keep_i.append(src); rename_i[src] = dst; seen_i.add(dst)
            disp = it[keep_i].rename(columns=rename_i).head(15).copy()

            def _parse_tx(raw_row):
                for col in ("Transaction", "Text", "Insider Trading", "transaction", "text"):
                    raw = raw_row.get(col, "") or ""
                    v = str(raw).strip()
                    if v and v.lower() not in ("nan", "none", ""):
                        vl = v.lower()
                        if any(k in vl for k in ("sale", "sell", "disposed", "s-sale")):
                            return "Sale"
                        if any(k in vl for k in ("purchase", "buy", "acqui", "p-purchase")):
                            return "Purchase"
                        if "award" in vl or "grant" in vl:
                            return "Award"
                        if "exercise" in vl or "option" in vl:
                            return "Exercise"
                        if "conversion" in vl or "convert" in vl:
                            return "Conversion"
                        # Return raw text as-is instead of dropping it
                        return v[:30]
                return "—"

            disp["Transaction"] = it.head(15).apply(_parse_tx, axis=1).values
            if "Shares" in disp.columns:
                disp["Shares"] = disp["Shares"].apply(
                    lambda v: _fmt_shares(v) if pd.notna(v) and str(v) not in ("","0") else "—")
            if "Value" in disp.columns:
                disp["Value"] = disp["Value"].apply(
                    lambda v: _fmt_val(v) if pd.notna(v) and str(v) not in ("","0") else "—")
            if "Date" in disp.columns:
                disp["Date"] = disp["Date"].apply(lambda v: str(v)[:10] if pd.notna(v) else "—")

            def _tx_row_colour(row):
                tx = str(row.get("Transaction", "")).lower()
                if any(k in tx for k in ("sale", "sell", "disposed")):
                    return ["color: #f87171"] * len(row)
                if any(k in tx for k in ("purchase", "buy", "acqui")):
                    return ["color: #4ade80"] * len(row)
                return [""] * len(row)

            styled = disp.style.apply(_tx_row_colour, axis=1)
            st.dataframe(styled, hide_index=True, use_container_width=True)
        else:
            st.caption("No insider transaction data available.")
    except Exception as e:
        st.caption(f"Insider transaction data unavailable. ({e})")


# ─── TAB 3: VALUE INVESTING ──────────────────────────────────

def render_value_investing():
    st.markdown("## Value Investing Research")
    st.caption("Analyze companies using Warren Buffett and Phil Town (Rule #1) frameworks.")

    c1, c2 = st.columns([3, 1])
    with c1:
        vi_ticker = st.text_input("Ticker Symbol", value="AAPL",
                                  key="vi_ticker").upper().strip()
    with c2:
        st.write("")
        run_btn = st.button("Analyze", type="primary", width="stretch")

    if not vi_ticker:
        return

    if run_btn or vi_ticker not in st.session_state.vi_cache:
        with st.spinner(f"Running value analysis for {vi_ticker}…"):
            st.session_state.vi_cache[vi_ticker] = run_value_analysis(vi_ticker)

    data  = st.session_state.vi_cache.get(vi_ticker, {})
    info  = data.get("info", {})
    price = data.get("price") or 0

    if not info:
        st.error("Could not load data. Check the ticker symbol.")
        return

    st.markdown(f"### {data.get('name', vi_ticker)}")
    st.caption(f"{data.get('sector', '—')}  |  {data.get('industry', '—')}")

    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Price",    f"${price:,.2f}" if price else "—")
    h2.metric("Mkt Cap",  _fmt_b(data.get("marketCap")))
    h3.metric("P/E TTM",  f"{data['trailingPE']:.1f}x" if data.get("trailingPE") else "—")
    h4.metric("ROE",      f"{data['returnOnEquity']*100:.1f}%" if data.get("returnOnEquity") else "—")

    st.divider()

    # SECTION 1 — Phil Town Big Five
    st.markdown("### Phil Town – Big Five Numbers")
    st.caption("All five metrics should grow **>10% annually** for 1, 3, 5, and 10 years. "
               "🟢 ≥10%  🟡 0–10%  🔴 negative")

    big5 = [
        ("ROIC",                   data.get("roic_growth",    {})),
        ("EPS Growth",             data.get("eps_growth",     {})),
        ("Sales (Revenue) Growth", data.get("revenue_growth", {})),
        ("Book Value Growth",      data.get("equity_growth",  {})),
        ("Free Cash Flow Growth",  data.get("fcf_growth",     {})),
    ]
    rows = [{"Metric": label, "1Y": _badge(gd.get(1)), "3Y": _badge(gd.get(3)),
             "5Y": _badge(gd.get(5)), "10Y": _badge(gd.get(10))} for label, gd in big5]
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

    roic_c = data.get("roic_current")
    if roic_c is not None:
        pct = roic_c * 100
        if pct >= 15:   st.success(f"Current ROIC: **{pct:.1f}%** — Excellent (>15% = wide moat)")
        elif pct >= 10: st.warning(f"Current ROIC: **{pct:.1f}%** — Acceptable (10–15%)")
        else:           st.error(f"Current ROIC: **{pct:.1f}%** — Below threshold (<10%)")

    with st.expander("Historical Financial Charts", expanded=False):
        ch1, ch2, ch3 = st.columns(3)
        with ch1:
            if data.get("revenue") is not None:
                st.plotly_chart(make_bar_history(data["revenue"], "Annual Revenue"), width="stretch")
        with ch2:
            if data.get("net_income") is not None:
                st.plotly_chart(make_bar_history(data["net_income"], "Net Income"), width="stretch")
        with ch3:
            if data.get("fcf") is not None:
                st.plotly_chart(make_bar_history(data["fcf"], "Free Cash Flow"), width="stretch")

    st.divider()

    # SECTION 2 — Sticker Price Calculator
    st.markdown("### Phil Town – Rule #1 Sticker Price Calculator")
    st.caption("**Sticker Price** = fair value at 15% annual return. "
               "**MOS Price** = buy at 50% of sticker (Margin of Safety).")

    eps     = data.get("eps_ttm") or 0
    est_g   = data.get("estimated_growth") or 0.10
    _tpe    = data.get("trailingPE") or 0
    _fpe    = data.get("info", {}).get("forwardPE") or 0
    _pe_avg = ((_tpe + _fpe) / 2) if (_tpe > 0 and _fpe > 0) else (_tpe or _fpe or 15)
    def_pe  = data.get("future_pe") or max(_pe_avg, 8)

    if st.session_state.get("sp_last_ticker") != vi_ticker:
        st.session_state["sp_eps"]  = float(round(eps, 2)) if eps and not np.isnan(float(eps)) else 0.0
        _sp_g = round(est_g * 100, 1)
        st.session_state["sp_g"]    = max(5.0, min(50.0, _sp_g)) if _sp_g > 0 else 5.0
        st.session_state["sp_pe"]   = float(min(round(def_pe, 1), 60))
        st.session_state["sp_ret"]  = 15.0
        st.session_state["sp_last_ticker"] = vi_ticker

    col_in, col_gauge = st.columns(2)

    with col_in:
        st.markdown("**Inputs**")
        c_eps = st.number_input("Current EPS ($)",
                                value=float(round(eps, 2)) if eps and not np.isnan(float(eps)) else 0.0,
                                step=0.01, format="%.2f", key="sp_eps")
        c_g   = st.number_input("Estimated Annual Growth Rate (%)",
                                 value=max(0.0, min(50.0, round(est_g * 100, 1))),
                                 min_value=0.0, max_value=50.0, step=0.5, key="sp_g")
        c_pe  = st.number_input("Future P/E Ratio",
                                 value=float(min(round(def_pe, 1), 60)),
                                 min_value=5.0, max_value=100.0, step=0.5, key="sp_pe")
        c_ret = st.number_input("Your Required Annual Return (%)",
                                 value=15.0, min_value=5.0, max_value=30.0, step=1.0, key="sp_ret")

    g = c_g / 100
    r = c_ret / 100
    sticker = mos = f_eps = f_price = None
    if c_eps > 0 and c_pe > 0:
        f_eps   = c_eps * (1 + g) ** 10
        f_price = f_eps * c_pe
        sticker = f_price / (1 + r) ** 10
        mos     = sticker / 2

    with col_gauge:
        if sticker and price > 0:
            axis_max = max(price * 1.6, sticker * 2.2)
            fig_g = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=price,
                delta={"reference": sticker, "valueformat": ".2f", "suffix": " vs sticker"},
                title={"text": "Price vs Sticker Price", "font": {"size": 14, "color": COLORS["text"]}},
                number={"prefix": "$", "font": {"color": COLORS["text"]}},
                gauge={
                    "axis": {"range": [0, axis_max], "tickcolor": COLORS["muted"]},
                    "bar":  {"color": COLORS["accent"]},
                    "bgcolor": COLORS["card"], "bordercolor": COLORS["border"],
                    "steps": [
                        {"range": [0, mos],           "color": "rgba(34,197,94,0.25)"},
                        {"range": [mos, sticker],      "color": "rgba(245,158,11,0.25)"},
                        {"range": [sticker, axis_max], "color": "rgba(239,68,68,0.15)"},
                    ],
                    "threshold": {"line": {"color": "#facc15", "width": 3},
                                  "thickness": 0.8, "value": sticker},
                },
            ))
            fig_g.update_layout(height=300, paper_bgcolor=COLORS["bg"],
                                font=dict(color=COLORS["text"]),
                                margin=dict(l=20, r=20, t=60, b=20))
            st.plotly_chart(fig_g, width="stretch")
        else:
            st.info("Enter EPS and growth rate to see the gauge.")

    st.markdown("**Calculated Values**")
    if sticker:
        cv1, cv2, cv3, cv4 = st.columns(4)
        cv1.metric("Future EPS (10y)",      f"${f_eps:.2f}")
        cv2.metric("Future Price (10y)",     f"${f_price:.2f}")
        cv3.metric("Sticker Price",          f"${sticker:.2f}")
        cv4.metric("MOS Price — Buy Below",  f"${mos:.2f}")

        if price > 0:
            if price <= mos:
                st.success(f"**BUY ZONE** — ${price:.2f} is below MOS ${mos:.2f}")
            elif price <= sticker:
                st.warning(f"**WATCH** — ${price:.2f} is between MOS and Sticker")
            else:
                over = (price - sticker) / sticker * 100
                st.error(f"**OVERVALUED** — ${price:.2f} is {over:.0f}% above Sticker ${sticker:.2f}")
    else:
        st.info("Enter EPS and growth rate to calculate.")

    st.divider()

    # SECTION 3 — Buffett Checklist
    st.markdown("### Warren Buffett – Quality Checklist")

    checks = []
    def _chk(label, condition, detail, note):
        checks.append((label, condition, detail, note))

    roe  = data.get("returnOnEquity")
    if roe is not None:
        _chk("ROE > 15%", roe >= 0.15, f"ROE = {roe*100:.1f}%",
             "Buffett seeks businesses that consistently earn >15% on equity.")

    eps_5 = data.get("eps_growth", {}).get(5)
    if eps_5 is not None:
        _chk("Consistent EPS Growth > 10%", eps_5 >= 0.10, f"5Y EPS CAGR = {eps_5*100:.1f}%",
             "Predictable, growing earnings indicate a business with a durable moat.")

    fcf_5 = data.get("fcf_growth", {}).get(5)
    if fcf_5 is not None:
        _chk("FCF Growth > 10%", fcf_5 >= 0.10, f"5Y FCF CAGR = {fcf_5*100:.1f}%",
             "Free cash flow is the real profit. Consistent FCF growth = pricing power.")

    gm = data.get("grossMargins")
    if gm is not None:
        _chk("Gross Margin > 40%", gm >= 0.40, f"Gross Margin = {gm*100:.1f}%",
             "High gross margins signal pricing power and a durable moat.")

    nm = data.get("profitMargins")
    if nm is not None:
        _chk("Net Margin > 10%", nm >= 0.10, f"Net Margin = {nm*100:.1f}%",
             "Strong net margins reflect efficient capital allocation.")

    roic_c = data.get("roic_current")
    if roic_c is not None:
        _chk("ROIC > 10%", roic_c >= 0.10, f"ROIC = {roic_c*100:.1f}%",
             "ROIC above cost of capital means the business creates value.")

    dy = data.get("debt_payoff_years")
    if dy is not None:
        _chk("LT Debt payable in < 4 years of FCF", dy < 4, f"LT Debt / FCF = {dy:.1f} years",
             "Buffett avoids high debt. Should be able to pay off all LT debt in < 4 years of FCF.")

    dte = data.get("debtToEquity")
    if dte is not None:
        _chk("Debt/Equity < 0.5", dte < 50, f"D/E = {dte/100:.2f}x",
             "Low leverage reduces bankruptcy risk and preserves flexibility.")

    passed = sum(1 for _, ok, _, _ in checks if ok)
    total  = len(checks)

    if total:
        pct_score   = passed / total * 100
        score_color = COLORS["up"] if pct_score >= 70 else \
                      (COLORS["warn"] if pct_score >= 50 else COLORS["down"])

        sc, _ = st.columns([1, 3])
        with sc:
            st.markdown(
                f"""<div style="background:{COLORS['card']};border:1px solid {COLORS['border']};
                border-radius:12px;padding:20px;text-align:center;margin-bottom:12px;">
                <div style="font-size:2.8rem;font-weight:bold;color:{score_color};">{passed}/{total}</div>
                <div style="color:{COLORS['muted']};">checks passed</div>
                <div style="font-size:1.4rem;color:{score_color};margin-top:4px;">{pct_score:.0f}%</div>
                </div>""",
                unsafe_allow_html=True,
            )

        for label, ok, detail, note in checks:
            icon = "✅" if ok else "❌"
            cls  = "check-pass" if ok else "check-fail"
            st.markdown(
                f'<div class="{cls}"><strong>{icon} {label}</strong> — {detail}<br>'
                f'<small style="color:{COLORS["muted"]};">{note}</small></div>',
                unsafe_allow_html=True,
            )

    st.markdown("#### Owner Earnings")
    oe   = data.get("owner_earnings")
    oe_s = data.get("owner_earnings_ps")
    _oe_ok  = oe  is not None and oe  == oe
    _oes_ok = oe_s is not None and oe_s == oe_s
    if _oe_ok:
        oe1, oe2, oe3 = st.columns(3)
        oe1.metric("Owner Earnings (annual)", _fmt_b(oe))
        oe2.metric("Per Share",               f"${oe_s:.2f}" if _oes_ok and oe_s else "—")
        if price and oe_s and oe_s > 0:
            oe3.metric("Price / Owner Earnings", f"{price/oe_s:.1f}x")
        st.caption("Owner Earnings = Net Income + D&A − Capital Expenditures")
    else:
        st.info("Owner earnings data not available for this ticker.")

    st.divider()

    # SECTION 4 — Peter Lynch Classification
    st.markdown("### Peter Lynch – Stock Classification")
    st.caption("Lynch sorted every stock into one of six types. Knowing the type shapes how you analyse and trade it.")

    lynch_cat      = data.get("lynch_category", "")
    lynch_subtitle = data.get("lynch_subtitle", "")
    lynch_reason   = data.get("lynch_reason", "")
    lynch_tips     = data.get("lynch_tips", [])

    ALL_CATEGORIES = [
        ("Slow Grower",  "mature"),
        ("Stalwart",     "steady"),
        ("Fast Grower",  "high growth"),
        ("Cyclical",     "timing key"),
        ("Turnaround",   "recovery play"),
        ("Asset Play",   "hidden value"),
    ]

    cols = st.columns(len(ALL_CATEGORIES))
    for col, (cat, sub) in zip(cols, ALL_CATEGORIES):
        is_active    = cat == lynch_cat
        border_color = "#7b93d4" if is_active else "#2d3250"
        bg_color     = "#2d3a6e" if is_active else "#1a1f2e"
        name_weight  = "700"     if is_active else "400"
        sub_color    = "#c5d0f0" if is_active else "#64748b"
        col.markdown(
            f"""<div style="background:{bg_color};border:2px solid {border_color};
            border-radius:10px;padding:14px 8px;text-align:center;min-height:72px;">
            <div style="font-size:0.95rem;font-weight:{name_weight};color:#e2e8f0;">{cat}</div>
            <div style="font-size:0.72rem;color:{sub_color};margin-top:4px;">{sub}</div>
            </div>""",
            unsafe_allow_html=True,
        )

    st.write("")

    if lynch_cat:
        st.markdown(
            f"<div style='background:#1a1f2e;border:1px solid #2d3250;border-radius:10px;"
            f"padding:16px 20px;margin-top:4px;'>"
            f"<span style='font-size:1.05rem;font-weight:700;color:#e2e8f0;'>{lynch_cat}</span>"
            f"<span style='color:#7b93d4;font-size:0.85rem;margin-left:10px;'>{lynch_subtitle}</span>"
            f"<p style='color:#94a3b8;font-size:0.88rem;margin:10px 0 14px;'>{lynch_reason}</p>"
            + "".join(f"<div style='color:#c5d0f0;font-size:0.84rem;margin:5px 0;'>&#8227;&nbsp;{tip}</div>"
                      for tip in lynch_tips)
            + "</div>",
            unsafe_allow_html=True,
        )

    st.divider()

    # SECTION 5 — DCF Valuation
    st.markdown("### DCF Intrinsic Value Calculator")
    st.caption("Projects free cash flow forward 10 years and discounts back at your required rate.")

    fcf_series = data.get("fcf")
    fcf_curr   = float(fcf_series.iloc[0]) if fcf_series is not None and not fcf_series.empty else 0
    shares_out = info.get("sharesOutstanding", 1) or 1

    if st.session_state.get("dcf_last_ticker") != vi_ticker:
        _est_g = (data.get("estimated_growth") or 0.10) * 100
        st.session_state["dcf_fcf"] = round(fcf_curr / 1e9, 2) if fcf_curr and not np.isnan(fcf_curr) else 0.0
        st.session_state["dcf_g1"]  = max(0.0, min(50.0, round(_est_g, 1)))
        st.session_state["dcf_g2"]  = max(round(_est_g / 2, 1), 3.0)
        st.session_state["dcf_tv"]  = 3.0
        st.session_state["dcf_dr"]  = 10.0
        st.session_state["dcf_last_ticker"] = vi_ticker

    d_in, d_out = st.columns(2)

    with d_in:
        st.markdown("**DCF Inputs**")
        d_fcf = st.number_input("Current FCF ($B)",
                                 value=round(fcf_curr / 1e9, 2) if fcf_curr and not np.isnan(fcf_curr) else 0.0,
                                 step=0.1, format="%.2f", key="dcf_fcf")
        d_g1  = st.number_input("Growth Rate — Years 1–5 (%)",
                                  value=max(0.0, min(50.0, round((data.get("estimated_growth") or 0.10) * 100, 1))),
                                  min_value=0.0, max_value=50.0, step=0.5, key="dcf_g1")
        d_g2  = st.number_input("Growth Rate — Years 6–10 (%)",
                                  value=max(round(((data.get("estimated_growth") or 0.10) * 100) / 2, 1), 3.0),
                                  min_value=0.0, max_value=30.0, step=0.5, key="dcf_g2")
        d_tv  = st.number_input("Terminal Growth Rate (%)",
                                  value=3.0, min_value=0.0, max_value=8.0, step=0.5, key="dcf_tv")
        d_dr  = st.number_input("Discount Rate / WACC (%)",
                                  value=10.0, min_value=5.0, max_value=20.0, step=0.5, key="dcf_dr")

    with d_out:
        st.markdown("**DCF Results**")
        if d_fcf > 0 and shares_out > 0:
            g1 = d_g1 / 100; g2 = d_g2 / 100; tr = d_tv / 100; dr = d_dr / 100
            fcf_b = d_fcf * 1e9
            fcf_proj, pvs = [], []
            for yr in range(1, 11):
                f = fcf_b * (1 + g1) ** min(yr, 5) * (1 + g2) ** max(0, yr - 5)
                fcf_proj.append(f)
                pvs.append(f / (1 + dr) ** yr)

            tv    = fcf_proj[-1] * (1 + tr) / (dr - tr) if dr > tr else 0
            pv_tv = tv / (1 + dr) ** 10
            cash  = data.get("totalCash") or 0
            debt  = data.get("totalDebt") or 0
            eq_v  = sum(pvs) + pv_tv + cash - debt
            dcf_ps = eq_v / shares_out

            st.metric("PV of FCFs (10Y)",         f"${sum(pvs)/1e9:.1f}B")
            st.metric("PV of Terminal Value",      f"${pv_tv/1e9:.1f}B")
            st.metric("Enterprise Value",          f"${(sum(pvs)+pv_tv)/1e9:.1f}B")
            st.metric("DCF Intrinsic Value/Share", f"${dcf_ps:.2f}")

            if price > 0 and dcf_ps > 0:
                mos_pct = (dcf_ps - price) / dcf_ps * 100
                if mos_pct > 30:   st.success(f"{mos_pct:.0f}% margin of safety — significant upside")
                elif mos_pct > 0:  st.warning(f"{mos_pct:.0f}% margin of safety — modest upside")
                else:              st.error(f"Price is {abs(mos_pct):.0f}% above DCF intrinsic value")

            fig_wf = go.Figure(go.Waterfall(
                orientation="v",
                measure=(["relative"] * 10) + ["relative", "relative", "relative", "total"],
                x=[f"Y{i+1}" for i in range(10)] + ["Terminal PV", "+Cash", "−Debt", "Equity Value"],
                y=[pv / 1e9 for pv in pvs] + [pv_tv / 1e9, cash / 1e9, -debt / 1e9, 0],
                connector={"line": {"color": COLORS["border"]}},
                increasing={"marker": {"color": COLORS["up"]}},
                decreasing={"marker": {"color": COLORS["down"]}},
                totals={"marker":    {"color": COLORS["accent"]}},
            ))
            fig_wf.update_layout(title="<b>DCF Value Breakdown ($B)</b>",
                                  yaxis_title="Value ($B)", height=360,
                                  **_layout(margin=dict(l=40, r=20, t=50, b=40)))
            st.plotly_chart(fig_wf, width="stretch")
        else:
            st.info("Enter FCF above to run the DCF model.")

    st.divider()

    # SECTION 6 — Valuation Summary
    st.markdown("### Valuation Summary")

    rows_v = []
    if price:
        rows_v.append({"Method": "Current Market Price", "Value": f"${price:.2f}", "vs Current": "—"})

    sp = sticker if sticker else data.get("sticker_price")
    mp = mos     if mos     else data.get("mos_price")

    for method, val in [
        ("Phil Town – Sticker Price",   sp),
        ("Phil Town – MOS Price (Buy)", mp),
        ("DCF Intrinsic Value",         data.get("dcf_intrinsic")),
    ]:
        if val and float(val) == float(val):
            diff = (val - price) / price * 100 if price else 0
            rows_v.append({"Method": method, "Value": f"${val:.2f}", "vs Current": f"{diff:+.0f}%"})

    if rows_v:
        st.dataframe(pd.DataFrame(rows_v), hide_index=True, width="stretch")

    st.caption("**Disclaimer:** Educational tool only. Not financial advice.")


# ─── TAB 4: WATCHLIST ────────────────────────────────────────

def render_watchlist():
    st.markdown("## Watchlist")

    if "watchlist_qty" not in st.session_state:
        st.session_state.watchlist_qty = db.get_watchlist_qty(st.session_state.active_watchlist)

    wq = {}
    if st.session_state.watchlist:
        with st.spinner("Loading watchlist…"):
            wq = fetch_quotes(tuple(st.session_state.watchlist))

    ctrl_col = st.container()
    with ctrl_col:
        wl_names = st.session_state.watchlist_names or ["My Watchlist"]
        st.caption("Active Watchlist")
        active = st.selectbox(
            "Active Watchlist", wl_names,
            index=wl_names.index(st.session_state.active_watchlist)
                  if st.session_state.active_watchlist in wl_names else 0,
            key="wl_active_select", label_visibility="collapsed",
        )
        if active != st.session_state.active_watchlist:
            st.session_state.active_watchlist = active
            st.session_state.watchlist     = db.get_watchlist(active)
            st.session_state.watchlist_qty = db.get_watchlist_qty(active)
            st.rerun()

        cr1, cr2 = st.columns([3, 1])
        with cr1:
            new_list_name = st.text_input("New watchlist name", placeholder="e.g. Growth Stocks",
                                          key="wl_new_list_input", label_visibility="collapsed").strip()
        with cr2:
            if st.button("Create", width="stretch", key="wl_create_btn"):
                if new_list_name and new_list_name not in wl_names:
                    db.create_watchlist(new_list_name)
                    st.session_state.watchlist_names  = db.get_watchlist_names()
                    st.session_state.active_watchlist = new_list_name
                    st.session_state.watchlist        = []
                    st.session_state.watchlist_qty    = {}
                    st.rerun()

        st.divider()

        add_c1, add_c2, add_c3 = st.columns([3, 1, 1])
        with add_c1:
            new_sym = st.text_input("Add ticker", placeholder="e.g. NVDA",
                                    key="wl_ticker_input", label_visibility="collapsed").upper().strip()
        with add_c2:
            new_qty = st.number_input("Qty", min_value=0.0, value=0.0, step=1.0, format="%.2f",
                                      key="wl_qty_input", label_visibility="collapsed")
        with add_c3:
            if st.button("Add Ticker", width="stretch", key="wl_add_btn") and new_sym:
                if new_sym not in st.session_state.watchlist:
                    db.add_to_watchlist(new_sym, st.session_state.active_watchlist, qty=new_qty)
                    st.session_state.watchlist.append(new_sym)
                    st.session_state.watchlist_qty[new_sym] = new_qty
                    st.rerun()

    if not st.session_state.watchlist:
        st.info("This watchlist is empty. Add a ticker above.")
        return

    st.divider()

    # ── TradingView chart ─────────────────────────────────────
    if "wl_chart_sym" not in st.session_state:
        st.session_state.wl_chart_sym = st.session_state.watchlist[0]
    if st.session_state.wl_chart_sym not in st.session_state.watchlist:
        st.session_state.wl_chart_sym = st.session_state.watchlist[0]

    cc1, cc2, cc3 = st.columns([3, 2, 2])
    with cc1:
        selected = st.selectbox(
            "Chart ticker", st.session_state.watchlist,
            index=st.session_state.watchlist.index(st.session_state.wl_chart_sym),
            key="wl_chart_select",
        )
        st.session_state.wl_chart_sym = selected
    with cc2:
        wl_period = st.selectbox("Period", ["1mo", "3mo", "6mo", "1y", "2y", "5y"],
                                 index=3, key="wl_period")
    with cc3:
        wl_interval = st.selectbox("Interval", ["1d", "1wk"], index=0, key="wl_interval")

    hist = fetch_history(selected, period=wl_period, interval=wl_interval)
    if hist.empty:
        st.warning(f"No price history available for {selected}.")
    else:
        chart_html = make_lightweight_chart(selected, hist)
        st.components.v1.html(chart_html, height=590, scrolling=False)

    st.divider()

    # Price table
    rows = []
    for s in st.session_state.watchlist:
        q   = wq.get(s, {})
        qty = st.session_state.watchlist_qty.get(s, 0)
        price = q.get("price", 0)
        rows.append({
            "Ticker":    s,
            "Qty":       qty,
            "Price":     price,
            "Mkt Value": price * qty if qty else None,
            "Open":      q.get("open", 0),
            "High":      q.get("high", 0),
            "Low":       q.get("low", 0),
            "Chg":       q.get("change", 0),
            "% Chg":     q.get("pct_change", 0),
            "Volume":    q.get("volume", 0),
        })

    fmt = {
        "Price": "${:.2f}", "Open": "${:.2f}", "High": "${:.2f}", "Low": "${:.2f}",
        "Chg": "{:+.2f}", "% Chg": "{:+.2f}%", "Volume": "{:,.0f}",
        "Qty": "{:,.2f}", "Mkt Value": "${:,.2f}",
    }
    st.dataframe(
        pd.DataFrame(rows)
          .style.map(_clr, subset=["Chg", "% Chg"])
          .format(fmt, na_rep="—"),
        hide_index=True, width="stretch",
        height=min(500, 45 * (len(rows) + 1)),
    )

    with st.expander("Edit share quantities", expanded=False):
        changed = False
        eq_cols = st.columns(min(4, len(st.session_state.watchlist)))
        for i, sym in enumerate(st.session_state.watchlist):
            with eq_cols[i % len(eq_cols)]:
                new_q = st.number_input(
                    sym, min_value=0.0,
                    value=float(st.session_state.watchlist_qty.get(sym, 0)),
                    step=1.0, format="%.2f", key=f"wl_qty_edit_{sym}",
                )
                if new_q != st.session_state.watchlist_qty.get(sym, 0):
                    db.update_watchlist_qty(sym, st.session_state.active_watchlist, new_q)
                    st.session_state.watchlist_qty[sym] = new_q
                    changed = True
        if changed:
            st.rerun()

    st.divider()

    st.markdown("**Remove from Watchlist**")
    rm_c1, rm_c2 = st.columns([3, 1])
    with rm_c1:
        ticker_to_remove = st.selectbox("Remove ticker", st.session_state.watchlist,
                                         key="wl_remove_select", label_visibility="collapsed")
    with rm_c2:
        if st.button("Remove", width="stretch", key="wl_remove_btn"):
            db.remove_from_watchlist(ticker_to_remove, st.session_state.active_watchlist)
            st.session_state.watchlist.remove(ticker_to_remove)
            st.rerun()


# ─── TAB 5: SCREENER ─────────────────────────────────────────

def render_screener():
    st.markdown("## Stock Screener")
    st.caption("Filter stocks by fundamental criteria · Data via yFinance · Fundamentals cached 1h")

    with st.expander("Filters", expanded=True):
        f1, f2, f3, f4, f5 = st.columns(5)
        SECTOR_LIST = ["All","Technology","Healthcare","Financial Services",
                       "Consumer Cyclical","Consumer Defensive","Communication Services",
                       "Industrials","Energy","Real Estate","Utilities","Basic Materials"]
        sel_sector  = f1.selectbox("Sector", SECTOR_LIST, key="scr_sector")
        max_pe      = f2.number_input("Max P/E",           value=50.0,  min_value=0.0,    max_value=300.0, step=5.0,  key="scr_max_pe")
        min_roe     = f3.number_input("Min ROE (%)",        value=0.0,   min_value=-50.0,  max_value=100.0, step=5.0,  key="scr_min_roe")
        min_eps_grw = f4.number_input("Min EPS Growth (%)", value=0.0,   min_value=-100.0, max_value=500.0, step=5.0,  key="scr_min_eg")
        max_de      = f5.number_input("Max Debt/Equity",    value=5.0,   min_value=0.0,    max_value=50.0,  step=0.5,  key="scr_max_de")

        f6, f7, f8, f9, f10 = st.columns(5)
        min_div     = f6.number_input("Min Div Yield (%)",  value=0.0,   min_value=0.0,    max_value=20.0,  step=0.5,  key="scr_min_div")
        min_mktcap  = f7.number_input("Min Mkt Cap ($B)",   value=0.0,   min_value=0.0,    max_value=3000.0,step=10.0, key="scr_min_mc")
        min_net_mg  = f8.number_input("Min Net Margin (%)", value=0.0,   min_value=-100.0, max_value=100.0, step=2.0,  key="scr_min_nm")
        max_beta    = f9.number_input("Max Beta",            value=5.0,   min_value=0.0,    max_value=10.0,  step=0.5,  key="scr_max_beta")
        min_rev_grw = f10.number_input("Min Rev Growth (%)",value=0.0,   min_value=-100.0, max_value=500.0, step=5.0,  key="scr_min_rg")

    cx1, cx2 = st.columns([4, 1])
    custom_raw = cx1.text_input("Additional Tickers (comma-separated)",
                                 placeholder="e.g. ABNB, DUOL, CELH — added to the top of the universe",
                                 key="scr_custom")
    run_btn = cx2.button("▶  Run Screener", type="primary", width="stretch", key="scr_run")

    universe = list(_SCREENER_UNIVERSE)
    if custom_raw.strip():
        extras = [t.strip().upper() for t in custom_raw.split(",") if t.strip()]
        universe = list(dict.fromkeys(extras + universe))

    cached_df = db.get_screener_cache(max_price_age_s=300, max_fund_age_s=3600)

    if run_btn:
        _fetch_screener_data.clear()
        st.session_state.pop("scr_df", None)

    if "scr_df" in st.session_state and not run_btn:
        df = st.session_state["scr_df"]
    elif not cached_df.empty and not run_btn:
        col_map = {
            "Symbol": "Symbol", "Sector": "Sector", "Industry": "Industry",
            "Price": "Price", "Chg %": "Chg %", "Mkt Cap ($B)": "Mkt Cap ($B)",
            "P/E": "P/E", "Fwd P/E": "Fwd P/E",
            "ROE %": "ROE %", "EPS Growth %": "EPS Growth %",
            "Rev Growth %": "Rev Growth %", "Div Yield %": "Div Yield %",
            "P/B": "P/B", "Beta": "Beta",
        }
        df = cached_df.rename(columns=col_map)
        st.session_state["scr_df"] = df
    elif run_btn or "scr_df" not in st.session_state:
        with st.spinner(f"Fetching data for {len(universe)} tickers… (~20–30s)"):
            df = _fetch_screener_data(tuple(universe))
        if not df.empty:
            try:
                db.upsert_screener_rows(df.to_dict("records"))
            except Exception:
                pass
            st.session_state["scr_df"] = df
        else:
            st.warning("No data returned. Check your internet connection.")
            return
    else:
        st.info("Click **▶ Run Screener** to load data.")
        return

    if df.empty:
        st.info("Click **▶ Run Screener** to load data.")
        return

    # Apply filters
    flt = df.copy()
    if sel_sector != "All":
        flt = flt[flt["Sector"].str.contains(sel_sector, na=False, case=False)]
    if max_pe < 300:
        flt = flt[(flt["P/E"].isna()) | (flt["P/E"] <= max_pe)]
    if min_roe != 0:
        flt = flt[(flt["ROE %"].notna()) & (flt["ROE %"] >= min_roe)]
    if min_eps_grw != 0:
        flt = flt[(flt["EPS Growth %"].notna()) & (flt["EPS Growth %"] >= min_eps_grw)]
    if max_de < 50 and "Debt/Eq" in flt.columns:
        flt = flt[(flt["Debt/Eq"].isna()) | (flt["Debt/Eq"] <= max_de)]
    if min_div > 0:
        flt = flt[(flt["Div Yield %"].notna()) & (flt["Div Yield %"] >= min_div)]
    if min_mktcap > 0:
        flt = flt[(flt["Mkt Cap ($B)"].notna()) & (flt["Mkt Cap ($B)"] >= min_mktcap)]
    if min_net_mg != 0:
        flt = flt[(flt["Net Margin %"].notna()) & (flt["Net Margin %"] >= min_net_mg)]
    if max_beta < 10:
        flt = flt[(flt["Beta"].isna()) | (flt["Beta"] <= max_beta)]
    if min_rev_grw != 0:
        flt = flt[(flt["Rev Growth %"].notna()) & (flt["Rev Growth %"] >= min_rev_grw)]

    sb1, sb2, sb3, sb4 = st.columns(4)
    sb1.metric("Matches", len(flt))
    sb2.metric("Universe", len(df))
    pos_eg = flt[flt["EPS Growth %"].notna() & (flt["EPS Growth %"] > 0)]
    sb3.metric("Positive EPS Growth", len(pos_eg))
    div_payers = flt[flt["Div Yield %"].notna() & (flt["Div Yield %"] > 0)]
    sb4.metric("Dividend Payers", len(div_payers))

    st.divider()

    if flt.empty:
        st.info("No stocks match the current filters. Try relaxing your criteria.")
        return

    display_cols = [c for c in [
        "Symbol","Company","Sector","Price","Chg %","Mkt Cap ($B)",
        "P/E","Fwd P/E","EPS ($)","EPS Growth %","Rev Growth %",
        "Div Yield %","ROE %","Debt/Eq","Net Margin %","Beta",
    ] if c in flt.columns]

    disp = flt[display_cols].sort_values("Mkt Cap ($B)", ascending=False, na_position="last")

    def _color_pe(v):
        if v != v or v is None: return ""
        if v < 15:  return f"color: {COLORS['up']}"
        if v < 30:  return f"color: {COLORS['warn']}"
        return f"color: {COLORS['down']}"

    def _color_pct(v):
        if v != v or v is None: return ""
        if v > 0: return f"color: {COLORS['up']}"
        if v < 0: return f"color: {COLORS['down']}"
        return ""

    def _color_roe(v):
        if v != v or v is None: return ""
        if v >= 15: return f"color: {COLORS['up']}"
        if v >= 5:  return f"color: {COLORS['warn']}"
        return f"color: {COLORS['down']}"

    def _color_de(v):
        if v != v or v is None: return ""
        if v <= 1:  return f"color: {COLORS['up']}"
        if v <= 2:  return f"color: {COLORS['warn']}"
        return f"color: {COLORS['down']}"

    styled = disp.style
    if "P/E"          in disp.columns: styled = styled.map(_color_pe,  subset=["P/E"])
    if "Fwd P/E"      in disp.columns: styled = styled.map(_color_pe,  subset=["Fwd P/E"])
    if "Chg %"        in disp.columns: styled = styled.map(_color_pct, subset=["Chg %"])
    if "EPS Growth %"  in disp.columns: styled = styled.map(_color_pct, subset=["EPS Growth %"])
    if "Rev Growth %"  in disp.columns: styled = styled.map(_color_pct, subset=["Rev Growth %"])
    if "Net Margin %"  in disp.columns: styled = styled.map(_color_pct, subset=["Net Margin %"])
    if "ROE %"         in disp.columns: styled = styled.map(_color_roe, subset=["ROE %"])
    if "Debt/Eq"       in disp.columns: styled = styled.map(_color_de,  subset=["Debt/Eq"])

    fmt = {}
    for col, spec in [
        ("Price","${:.2f}"), ("Chg %","{:+.2f}%"), ("Mkt Cap ($B)","${:.1f}B"),
        ("P/E","{:.1f}"),    ("Fwd P/E","{:.1f}"), ("EPS ($)","${:.2f}"),
        ("EPS Growth %","{:+.1f}%"), ("Rev Growth %","{:+.1f}%"),
        ("Div Yield %","{:.2f}%"),   ("ROE %","{:.1f}%"),
        ("Debt/Eq","{:.2f}"),        ("Net Margin %","{:.1f}%"),
        ("P/B","{:.2f}"),            ("Beta","{:.2f}"),
    ]:
        if col in disp.columns:
            fmt[col] = spec

    styled = styled.format(fmt, na_rep="—")
    st.dataframe(styled, hide_index=True, width="stretch", height=520)
    st.caption(f"**{len(flt)} results** · Green P/E < 15 · Amber 15–30 · Red > 30 · "
               "ROE green ≥ 15% · Debt/Eq green ≤ 1×")


# ─── AUTH ────────────────────────────────────────────────────

# Add every Google email that should have access
ALLOWED_EMAILS = [
    "rajksamy@gmail.com",  # <-- replace with your Gmail
    "maximus03156@gmail.com",
    "vsr85048@gmail.com",

]

def _google_auth_gate():
    """Block the app until the user signs in with an allowed Google account.
    Returns immediately (no-op) if already authenticated."""

    if "auth_email" in st.session_state:
        return  # already signed in

    # ── Login screen ──────────────────────────────────────────
    st.markdown(
        """
        <div style='display:flex;flex-direction:column;align-items:center;
                    justify-content:center;height:55vh;gap:1.2rem;text-align:center'>
            <div style='font-size:3rem'>📈</div>
            <h1 style='color:#e2e8f0;margin:0'>MarketBoard</h1>
            <p style='color:#64748b;margin:0'>Sign in with your Google account to continue</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    import os
    def _secret(key, default=None):
        # Try st.secrets first (local secrets.toml), fall back to env var (Railway)
        try:
            return st.secrets[key]
        except Exception:
            return os.environ.get(key, default)

    client_id     = _secret("GOOGLE_CLIENT_ID")
    client_secret = _secret("GOOGLE_CLIENT_SECRET")
    redirect_uri  = _secret("REDIRECT_URI", "http://localhost:8501")

    if not client_id or not client_secret:
        st.error("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET. Add them as Railway environment variables.")
        st.stop()

    _, mid, _ = st.columns([2, 1, 2])
    with mid:
        oauth2 = OAuth2Component(
            client_id=client_id,
            client_secret=client_secret,
            authorize_endpoint="https://accounts.google.com/o/oauth2/v2/auth",
            token_endpoint="https://oauth2.googleapis.com/token",
            refresh_token_endpoint="https://oauth2.googleapis.com/token",
            revoke_token_endpoint="https://oauth2.googleapis.com/revoke",
        )
        result = oauth2.authorize_button(
            name="Continue with Google",
            icon="https://www.google.com/favicon.ico",
            redirect_uri=redirect_uri,
            scope="openid email profile",
            key="google_oauth",
            use_container_width=True,
        )

    if result and "token" in result:
        id_token = result["token"].get("id_token", "")
        try:
            payload = pyjwt.decode(id_token, options={"verify_signature": False})
            email   = payload.get("email", "")
            name    = payload.get("name", email)
        except Exception as e:
            st.error(f"Token decode error: {e}")
            st.stop()

        if email not in ALLOWED_EMAILS:
            st.error(f"Access denied — **{email}** is not on the allowed list.")
            st.stop()

        st.session_state["auth_email"] = email
        st.session_state["auth_name"]  = name
        st.rerun()

    st.stop()


# ─── MAIN ────────────────────────────────────────────────────

def main():
    _google_auth_gate()

    # ── Signed-in user info in sidebar ───────────────────────
    with st.sidebar:
        st.markdown(f"👤 **{st.session_state['auth_name']}**")
        st.caption(st.session_state["auth_email"])
        if st.button("Sign out", use_container_width=True):
            del st.session_state["auth_email"]
            del st.session_state["auth_name"]
            st.rerun()
        st.divider()

    render_sidebar()

    tabs = st.tabs([
        "🌍 Market Overview",
        "📊 Stock Research",
        "🎯 Value Investing",
        "👁️ Watchlist",
        "🔍 Screener",
    ])

    with tabs[0]: render_market_overview()
    with tabs[1]: render_stock_research()
    with tabs[2]: render_value_investing()
    with tabs[3]: render_watchlist()
    with tabs[4]: render_screener()


if __name__ == "__main__":
    main()
