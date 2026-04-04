"""
damodaran_db.py — SQLite persistence layer for the Damodaran Value Scanner
===========================================================================
Provides a local cache so the scanner doesn't hammer yFinance on every run.

TABLES
------
  scan_results     — Full Damodaran metrics per ticker (JSON blob + key columns)
  live_quotes      — Latest price snapshots for overlay (lightweight, fast refresh)
  index_members    — Cached S&P 500 / NASDAQ-100 constituent lists
  scan_metadata    — Tracks when each scan was last run

DESIGN PRINCIPLES
-----------------
  • WAL journal mode for concurrent reads (Streamlit reruns) while writing.
  • All data has a `fetched_at` timestamp for staleness checks.
  • Scan results cached 24h by default; live quotes cached 60s.
  • Thread-safe via check_same_thread=False.
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = os.environ.get("DAMODARAN_DB", "damodaran_scanner.db")


def _conn() -> sqlite3.Connection:
    """Return a WAL-mode connection to the database."""
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=5000")
    c.row_factory = sqlite3.Row
    return c


def init_db():
    """Create all tables if they don't exist."""
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS scan_results (
            symbol        TEXT    NOT NULL,
            scan_group    TEXT    NOT NULL DEFAULT 'custom',
            data_json     TEXT    NOT NULL,
            damodaran_score REAL,
            roic          REAL,
            wacc          REAL,
            spread        REAL,
            intrinsic_val REAL,
            margin_safety REAL,
            price         REAL,
            sector        TEXT,
            value_signal  TEXT,
            trap_count    INTEGER DEFAULT 0,
            fetched_at    TEXT    NOT NULL,
            PRIMARY KEY (symbol, scan_group)
        );

        CREATE TABLE IF NOT EXISTS live_quotes (
            symbol      TEXT PRIMARY KEY,
            price       REAL,
            change      REAL,
            pct_change  REAL,
            volume      INTEGER,
            fetched_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS index_members (
            index_name  TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            fetched_at  TEXT NOT NULL,
            PRIMARY KEY (index_name, symbol)
        );

        CREATE TABLE IF NOT EXISTS scan_metadata (
            scan_group  TEXT PRIMARY KEY,
            last_run    TEXT NOT NULL,
            ticker_count INTEGER,
            duration_s  REAL
        );
    """)
    c.commit()
    c.close()


# ─── SCAN RESULTS ────────────────────────────────────────────

def save_scan_results(results: list[dict], scan_group: str = "custom"):
    """Persist a list of Damodaran metric dicts to the database.

    Each dict must contain at minimum: Symbol, and the full metric payload.
    Overwrites previous results for the same (symbol, scan_group).
    """
    c = _conn()
    now = datetime.utcnow().isoformat()

    for r in results:
        sym = r.get("Symbol", "")
        if not sym:
            continue

        # Store value traps as JSON-serialisable list
        data = r.copy()
        if "Value Traps" in data:
            data["Value Traps"] = list(data["Value Traps"])

        c.execute("""
            INSERT OR REPLACE INTO scan_results
                (symbol, scan_group, data_json, damodaran_score,
                 roic, wacc, spread, intrinsic_val, margin_safety,
                 price, sector, value_signal, trap_count, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sym, scan_group, json.dumps(data, default=str),
            r.get("Damodaran Score"),
            r.get("ROIC"),
            r.get("WACC"),
            r.get("ROIC-WACC Spread"),
            r.get("Intrinsic Value"),
            r.get("Margin of Safety %"),
            r.get("Price"),
            r.get("Sector"),
            r.get("Value Signal"),
            r.get("Trap Count", 0),
            now,
        ))

    c.commit()
    c.close()


def load_scan_results(scan_group: str = "custom",
                      max_age_hours: float = 24) -> list[dict]:
    """Load cached scan results if they exist and are fresh enough.

    Returns a list of metric dicts, or empty list if stale/missing.
    """
    c = _conn()
    cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat()

    rows = c.execute("""
        SELECT data_json FROM scan_results
        WHERE scan_group = ? AND fetched_at > ?
        ORDER BY damodaran_score DESC
    """, (scan_group, cutoff)).fetchall()
    c.close()

    results = []
    for row in rows:
        try:
            results.append(json.loads(row["data_json"]))
        except (json.JSONDecodeError, KeyError):
            continue
    return results


def get_scan_age(scan_group: str) -> Optional[datetime]:
    """Return the datetime of the last scan for a group, or None."""
    c = _conn()
    row = c.execute("""
        SELECT last_run FROM scan_metadata WHERE scan_group = ?
    """, (scan_group,)).fetchone()
    c.close()
    if row:
        try:
            return datetime.fromisoformat(row["last_run"])
        except ValueError:
            return None
    return None


def save_scan_metadata(scan_group: str, ticker_count: int, duration_s: float):
    """Record when a scan completed."""
    c = _conn()
    c.execute("""
        INSERT OR REPLACE INTO scan_metadata
            (scan_group, last_run, ticker_count, duration_s)
        VALUES (?, ?, ?, ?)
    """, (scan_group, datetime.utcnow().isoformat(), ticker_count, duration_s))
    c.commit()
    c.close()


def clear_scan_results(scan_group: str):
    """Delete all cached results for a scan group (force refresh)."""
    c = _conn()
    c.execute("DELETE FROM scan_results WHERE scan_group = ?", (scan_group,))
    c.execute("DELETE FROM scan_metadata WHERE scan_group = ?", (scan_group,))
    c.commit()
    c.close()


# ─── LIVE QUOTES ─────────────────────────────────────────────

def save_live_quotes(quotes: dict):
    """Persist live price quotes. quotes = {symbol: {price, change, pct_change, volume}}"""
    c = _conn()
    now = datetime.utcnow().isoformat()
    for sym, q in quotes.items():
        c.execute("""
            INSERT OR REPLACE INTO live_quotes
                (symbol, price, change, pct_change, volume, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            sym,
            q.get("price", 0),
            q.get("change", 0),
            q.get("pct_change", 0),
            q.get("volume", 0),
            now,
        ))
    c.commit()
    c.close()


def load_live_quotes(symbols: list, max_age_s: int = 90) -> dict:
    """Load cached live quotes that are fresh enough.

    Returns {symbol: {price, change, pct_change, volume}} for found symbols.
    """
    c = _conn()
    cutoff = (datetime.utcnow() - timedelta(seconds=max_age_s)).isoformat()
    placeholders = ",".join("?" * len(symbols))
    rows = c.execute(f"""
        SELECT symbol, price, change, pct_change, volume
        FROM live_quotes
        WHERE symbol IN ({placeholders}) AND fetched_at > ?
    """, (*symbols, cutoff)).fetchall()
    c.close()

    return {
        row["symbol"]: {
            "price": row["price"],
            "change": row["change"],
            "pct_change": row["pct_change"],
            "volume": row["volume"],
        }
        for row in rows
    }


# ─── INDEX MEMBERS ───────────────────────────────────────────

def save_index_members(index_name: str, members: list[dict]):
    """Save index constituent list.

    members = [{"symbol": "AAPL", "company": "Apple Inc.", "sector": "Technology"}, ...]
    """
    c = _conn()
    now = datetime.utcnow().isoformat()
    # Clear old members for this index
    c.execute("DELETE FROM index_members WHERE index_name = ?", (index_name,))
    for m in members:
        c.execute("""
            INSERT INTO index_members (index_name, symbol, company, sector, fetched_at)
            VALUES (?, ?, ?, ?, ?)
        """, (index_name, m["symbol"], m.get("company"), m.get("sector"), now))
    c.commit()
    c.close()


def load_index_members(index_name: str, max_age_days: int = 30) -> list[dict]:
    """Load cached index members if fresh enough."""
    c = _conn()
    cutoff = (datetime.utcnow() - timedelta(days=max_age_days)).isoformat()
    rows = c.execute("""
        SELECT symbol, company, sector FROM index_members
        WHERE index_name = ? AND fetched_at > ?
        ORDER BY symbol
    """, (index_name, cutoff)).fetchall()
    c.close()

    return [{"symbol": r["symbol"], "company": r["company"], "sector": r["sector"]}
            for r in rows]


def get_all_cached_groups() -> list[dict]:
    """Return metadata for all cached scan groups."""
    c = _conn()
    rows = c.execute("""
        SELECT scan_group, last_run, ticker_count, duration_s
        FROM scan_metadata ORDER BY last_run DESC
    """).fetchall()
    c.close()
    return [dict(r) for r in rows]


# ─── INIT ON IMPORT ──────────────────────────────────────────
init_db()
