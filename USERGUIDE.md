# Damodaran Value Scanner — User Guide

> A Streamlit web application implementing Professor Aswath Damodaran's (NYU Stern) equity valuation framework. Scan the S&P 500, NASDAQ 100, or any custom list of stocks for value creation signals, intrinsic value estimates, and value trap flags — all cached locally in SQLite.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
   - [Windows](#11-windows)
   - [Linux (Ubuntu / Mint)](#12-linux-ubuntu--mint)
2. [Installation](#2-installation)
3. [Running the App](#3-running-the-app)
4. [Application Overview](#4-application-overview)
5. [Methodology](#5-methodology)
   - [ROIC vs WACC — Value Creation](#51-roic-vs-wacc--value-creation)
   - [WACC Calculation](#52-wacc-calculation)
   - [ROIC Calculation](#53-roic-calculation)
   - [Two-Stage DCF Model](#54-two-stage-dcf-model)
   - [Margin of Safety](#55-margin-of-safety)
   - [Value Trap Detection](#56-value-trap-detection)
   - [Damodaran Score](#57-damodaran-score)
   - [Sector Benchmarks](#58-sector-benchmarks)
6. [Tab-by-Tab Guide](#6-tab-by-tab-guide)
7. [Caching System](#7-caching-system)
8. [Limitations & Disclaimer](#8-limitations--disclaimer)

---

## 1. Prerequisites

### 1.1 Windows

| Requirement | Minimum | Recommended | Notes |
|---|---|---|---|
| Python | 3.10 | 3.12+ | [python.org](https://www.python.org/downloads/) — tick **Add to PATH** during install |
| pip | bundled with Python | latest | Run `python -m pip install --upgrade pip` after install |
| Internet access | required | — | Fetches data from Yahoo Finance and Wikipedia |
| Disk space | ~200 MB | — | For Python packages + SQLite cache |

**Verify your install:**
```cmd
python --version
pip --version
```

If `python` is not found, try `py` instead (the Python Launcher for Windows).

---

### 1.2 Linux (Ubuntu / Mint)

| Requirement | Minimum | Notes |
|---|---|---|
| Python | 3.10 | Usually pre-installed on modern distros |
| pip | 22+ | May need separate install (see below) |
| build-essential | — | Required to compile `curl_cffi` |
| Internet access | required | — |

**Install system dependencies:**
```bash
sudo apt update
sudo apt install python3 python3-pip build-essential -y
```

**Verify:**
```bash
python3 --version
pip3 --version
```

---

## 2. Installation

### Clone or copy the project

Place these files in the same folder (e.g. `~/advantage` or `C:\Projects\advantage`):

```
damodaran_scanner.py   ← Main app (UI + analysis logic)
damodaran_db.py        ← SQLite persistence layer
requirements.txt       ← Python dependencies
```

### Install dependencies

**Windows:**
```cmd
cd C:\Projects\advantage
pip install -r requirements.txt
```

**Linux:**
```bash
cd ~/advantage
pip3 install -r requirements.txt
```

### Why `curl_cffi`?

Yahoo Finance blocks Python's default `urllib` with HTTP 429 (Too Many Requests). `curl_cffi` impersonates a real Chrome browser at the TLS handshake level, bypassing this restriction reliably. It is listed in `requirements.txt` and installed automatically.

---

## 3. Running the App

### Windows
```cmd
cd C:\Projects\advantage
python -m streamlit run damodaran_scanner.py --server.port 9501
```

### Linux
```bash
cd ~/advantage
python3 -m streamlit run damodaran_scanner.py --server.port 9501
```

Then open your browser at: **http://localhost:9501**

> **Tip:** Always use `python -m streamlit` rather than `streamlit` directly. The `streamlit` command may not be on your PATH, while `python -m streamlit` always works as long as the package is installed.

### Custom port
Replace `9501` with any free port, e.g. `--server.port 8080`.

### Run on a server (accessible from other machines)
```bash
python3 -m streamlit run damodaran_scanner.py --server.port 9501 --server.address 0.0.0.0
```
Then access via `http://<server-ip>:9501` from any machine on the network.

---

## 4. Application Overview

The app has five tabs:

| Tab | Purpose |
|---|---|
| **Scanner** | Bulk-scan S&P 500, NASDAQ 100, or custom stock lists |
| **Deep Dive** | Full analysis of a single ticker |
| **Benchmarks** | Damodaran's sector-level ROIC, WACC, and EV/EBITDA reference data |
| **Cache** | View, manage, and clear the local SQLite cache |
| **Methodology** | In-app summary of the valuation framework |

---

## 5. Methodology

The scanner implements three pillars from Professor Damodaran's valuation framework.

---

### 5.1 ROIC vs WACC — Value Creation

The central question in Damodaran's framework is not *"Is the stock cheap?"* but *"Does this company create value?"*

A company creates economic value **only when its Return on Invested Capital (ROIC) exceeds its Weighted Average Cost of Capital (WACC)**:

```
Spread = ROIC − WACC

Spread > 3%    →  Value Creator       (growth benefits shareholders)
0% < Spread ≤ 3%  →  Marginal Creator
-3% ≤ Spread < 0%  →  Marginal Destroyer
Spread < -3%   →  Value Destroyer      (growth destroys shareholder wealth)
```

The key insight: a company with a low P/E but ROIC < WACC is **not** a bargain — every dollar it reinvests erodes value. Growth only helps when ROIC > WACC.

---

### 5.2 WACC Calculation

WACC is the blended cost of all capital (equity + debt), weighted by their proportions:

```
WACC = (E/V) × Cost of Equity  +  (D/V) × After-tax Cost of Debt
```

Where:

**Cost of Equity** — via CAPM:
```
Cost of Equity = Risk-Free Rate + Beta × Equity Risk Premium
               = 4.3%  +  Beta  ×  4.61%
```

- **Risk-Free Rate (Rf):** 4.3% (10-year US Treasury yield, Jan 2026)
- **Equity Risk Premium (ERP):** 4.61% (Damodaran's Jan 2026 US ERP estimate)
- **Beta:** sourced from yFinance (trailing 5-year monthly vs S&P 500); defaults to 1.0 if unavailable

**After-tax Cost of Debt:**
```
After-tax Kd = (Interest Expense / Total Debt) × (1 − Effective Tax Rate)
```
Falls back to 4% pre-tax if debt is zero.

**Capital weights:**
```
E/V = Market Cap / (Market Cap + Total Debt)
D/V = Total Debt / (Market Cap + Total Debt)
```

WACC is floored at 4% to prevent unrealistic values for near-zero-debt companies.

---

### 5.3 ROIC Calculation

ROIC measures how efficiently a company deploys its total invested capital:

```
ROIC = NOPAT / Invested Capital

NOPAT          = EBIT × (1 − Effective Tax Rate)
Invested Capital = Equity + Total Debt − Cash & Equivalents
```

- **EBIT** is sourced from the income statement (`EBIT` or `Operating Income` row)
- **Invested Capital** uses the most recent balance sheet
- Cash is subtracted because it is not operationally deployed capital

If EBIT or balance sheet data is unavailable, ROIC is set to `None` and the signal becomes `No Data`.

---

### 5.4 Two-Stage DCF Model

The intrinsic value per share is estimated using a two-stage Free Cash Flow to Firm (FCFF) model, discounted at WACC:

**Stage 1 — Explicit forecast (Years 1–5):**
```
FCF grows at the company's trailing revenue growth rate
(capped between 2% and 25%)
```

**Stage 2 — Fading growth (Years 6–10):**
```
Growth fades linearly from Stage 1 rate to terminal rate
Fade rate = (Stage 1 growth + Terminal growth) / 2
```

**Terminal Value (Gordon Growth Model):**
```
TV  = FCF₁₀ × (1 + g) / (WACC − g)
g   = 2.5%  (long-run nominal GDP growth)
```

**Equity Value:**
```
Equity Value = PV(Stage 1) + PV(Stage 2) + PV(Terminal Value) + Cash − Debt
Intrinsic Value per Share = Equity Value / Shares Outstanding
```

The DCF requires **positive Free Cash Flow**. If FCF is unavailable or negative, `Intrinsic Value` is set to `None`. FCF is taken directly from the cash flow statement; if absent, it is derived as:
```
FCF = Operating Cash Flow − |Capital Expenditure|
```

---

### 5.5 Margin of Safety

Margin of Safety (MoS) is the gap between intrinsic value and current market price:

```
Margin of Safety % = (Intrinsic Value − Price) / Intrinsic Value × 100
```

| MoS | Interpretation |
|---|---|
| > 20% | Potentially undervalued |
| 0% – 20% | Near fair value |
| < 0% | Potentially overvalued |

A positive MoS does **not** mean a buy signal on its own — it must be combined with the ROIC/WACC spread and trap check.

---

### 5.6 Value Trap Detection

Five automated flags screen for common value traps — stocks that appear cheap but have fundamental problems:

| Flag | Condition | Why it matters |
|---|---|---|
| **Low P/E but ROIC < WACC** | P/E < 15 and ROIC < WACC | Cheap valuation driven by value destruction |
| **High leverage + thin margins** | D/E > 1.5× and Op Margin < 10% | Debt servicing will consume earnings |
| **Declining revenue** | Revenue growth < −5% and P/E < 12 | Secular decline masked by low headline multiple |
| **Negative FCF + low EV/EBITDA** | FCF < 0 and EV/EBITDA < 10× | Burning cash despite cheap-looking ratio |
| **Leveraged ROE trap** | ROE > 20% but ROIC < 8% | High ROE driven by debt, not operational excellence |

Each flag triggered is listed explicitly in the Deep Dive tab and deducted from the Damodaran Score.

---

### 5.7 Damodaran Score

A composite 0–100 score that summarises the overall quality and valuation attractiveness of a stock:

```
Base score:                          50

ROIC-WACC spread (±20 pts):
  +20 max if spread = +10%
  −20 max if spread = −10%

Margin of Safety (±15 pts):
  +15 max if MoS = +50%
  −15 max if MoS = −50%

FCF Yield bonus (+10 pts max):
  +2 pts per 1% of FCF yield (positive FCF only)

Value trap penalty:
  −10 pts per flag triggered

Growth bonus (+5 pts):
  Rev growth > 5% AND ROIC > WACC
```

Scores are clamped to [0, 100]. A score above 70 generally indicates a high-quality, attractively valued business. Below 40 suggests significant concerns.

---

### 5.8 Sector Benchmarks

The scanner uses Damodaran's January 2025/2026 published sector averages as reference points:

| Sector | WACC | ROIC | Avg EV/EBITDA |
|---|---|---|---|
| Technology | 10.50% | 25.00% | 22× |
| Healthcare | 9.50% | 15.00% | 18× |
| Consumer Defensive | 7.50% | 18.00% | 16× |
| Consumer Cyclical | 9.00% | 14.00% | 14× |
| Industrials | 8.80% | 13.00% | 13× |
| Communication Services | 9.20% | 12.00% | 12× |
| Utilities | 5.80% | 5.50% | 12× |
| Financial Services | 8.50% | 11.00% | 10× |
| Energy | 9.50% | 10.00% | 7× |
| Basic Materials | 9.20% | 9.00% | 9× |
| Real Estate | 7.00% | 6.00% | 18× |

These are displayed in the **Benchmarks** tab and used in the Deep Dive tab to contextualise a company's EV/EBITDA relative to its sector.

---

## 6. Tab-by-Tab Guide

### Scanner Tab

1. **Select Universe** from the sidebar:
   - *S&P 500* — all ~503 constituents (fetched from Wikipedia, cached 30 days)
   - *NASDAQ 100* — all 101 constituents
   - *Custom Sectors* — pick from 6 pre-built groups (Mega Cap Tech, Growth Tech, Financials, Healthcare, Consumer, Industrials & Energy)
   - *Custom Tickers* — enter any comma-separated tickers (e.g. `AAPL, MSFT, NVDA`)

2. **Set Filters** (sidebar):
   - *Min Score* — hide stocks below a Damodaran Score threshold
   - *Value creators only* — show only `Value Creator` and `Marginal Creator` signals
   - *Hide value traps* — exclude any stock with at least one trap flag

3. **Run or Load:**
   - **Load Cached** — instant load from SQLite (respects Cache validity hours)
   - **Run Fresh Scan** — fetches live data from Yahoo Finance (takes time for large universes)
   - **Refresh (Clear Cache)** — deletes cached data and forces a new scan next time

4. **Live Price Overlay** — toggle to fetch current prices and recalculate Margin of Safety without re-running the full scan.

5. **Charts produced:**
   - *ROIC vs WACC scatter* — visualises the value creation map; stocks above the diagonal line create value
   - *Score distribution histogram* — shows the spread of quality across the scanned universe
   - *Margin of Safety bar chart* — top 30 stocks by MoS, green = undervalued, red = overvalued

6. **Download CSV** — exports the full filtered results table.

---

### Deep Dive Tab

Enter any ticker to get a full single-stock analysis:

- Price, Market Cap, Damodaran Score
- Intrinsic Value and Margin of Safety
- ROIC, WACC, Spread, Value Signal
- Full quality metrics: ROE, Gross/Op/Net Margin, Revenue & Earnings Growth, FCF
- Relative valuation: P/E, Fwd P/E, EV/EBITDA, P/B, PEG vs sector average
- Capital structure: Beta, Cost of Equity, Debt/Equity, Debt/Capital
- Value trap flags (each listed explicitly)
- DCF assumptions expander (shows all inputs used in the model)

---

### Cache Tab

Displays all cached scan groups with:
- Number of stocks cached
- When the scan was last run and how long ago
- How long the scan took

Allows clearing cache per group or all at once. Also shows the SQLite database file size.

---

## 7. Caching System

The app uses a local SQLite database (`damodaran_scanner.db`, auto-created) to avoid re-fetching data on every page load.

| Data | Cache Duration | Reason |
|---|---|---|
| Scan results (fundamentals) | 24 hours | Financial statements change quarterly |
| Live price quotes | 60 seconds | Near real-time during a session |
| Index constituents (S&P/NASDAQ lists) | 30 days | Index membership changes infrequently |

The database path can be overridden with the environment variable:
```bash
export DAMODARAN_DB=/path/to/custom.db
```

---

## 8. Limitations & Disclaimer

- **Data quality:** All fundamental data is sourced from Yahoo Finance via yFinance. Data may be delayed, incomplete, or incorrectly classified for some tickers (especially non-US stocks, ADRs, and financial sector companies).
- **Financial companies:** ROIC/WACC methodology is less meaningful for banks and insurers, where leverage is operational rather than financial. Treat those signals with caution.
- **DCF sensitivity:** Intrinsic value estimates are highly sensitive to growth rate and WACC assumptions. Small changes in inputs produce large changes in output. The model is a starting point, not a precise prediction.
- **No look-ahead:** All data uses the most recent available financial statements. The model does not incorporate analyst forecasts.
- **Rate limits:** Yahoo Finance enforces request rate limits. If scans fail with network errors, wait a few minutes and retry. The `curl_cffi` Chrome impersonation handles most rate-limit responses, but sustained heavy scanning may still trigger blocks.

> **This tool is for educational and research purposes only. Nothing in this application constitutes financial advice. Always conduct your own due diligence before making investment decisions.**
