"""
test_damodaran.py — Test suite for Damodaran Value Scanner
===========================================================
Tests cover:
  - damodaran_db.py  : SQLite persistence layer (no network)
  - damodaran_scanner.py : pure helper functions and compute_metrics (mocked yFinance)

Run:
    pytest test_damodaran.py -v
"""

import os
import json
import sqlite3
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
import pytest


# ─── DB LAYER TESTS ──────────────────────────────────────────────────────────

@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    """Redirect DB_PATH to a temp file for each test."""
    db_file = str(tmp_path / "test.db")
    monkeypatch.setenv("DAMODARAN_DB", db_file)

    import importlib
    import damodaran_db
    importlib.reload(damodaran_db)          # picks up new env var
    damodaran_db.init_db()                  # create tables in temp DB
    yield damodaran_db
    if os.path.exists(db_file):
        os.remove(db_file)


class TestInitDb:
    def test_tables_created(self, tmp_db):
        c = sqlite3.connect(tmp_db.DB_PATH)
        tables = {row[0] for row in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        c.close()
        assert {"scan_results", "live_quotes", "index_members", "scan_metadata"} <= tables

    def test_wal_mode(self, tmp_db):
        c = sqlite3.connect(tmp_db.DB_PATH)
        mode = c.execute("PRAGMA journal_mode").fetchone()[0]
        c.close()
        assert mode == "wal"


class TestScanResults:
    SAMPLE = [
        {
            "Symbol": "AAPL",
            "Company": "Apple Inc.",
            "Sector": "Technology",
            "ROIC": 28.5,
            "WACC": 9.8,
            "ROIC-WACC Spread": 18.7,
            "Value Signal": "Value Creator",
            "Intrinsic Value": 190.0,
            "Margin of Safety %": 15.0,
            "Price": 161.0,
            "Damodaran Score": 75.0,
            "Trap Count": 0,
            "Value Traps": [],
        },
        {
            "Symbol": "XYZ",
            "Company": "XYZ Corp",
            "Sector": "Energy",
            "ROIC": 5.0,
            "WACC": 9.0,
            "ROIC-WACC Spread": -4.0,
            "Value Signal": "Value Destroyer",
            "Intrinsic Value": None,
            "Margin of Safety %": None,
            "Price": 42.0,
            "Damodaran Score": 30.0,
            "Trap Count": 1,
            "Value Traps": ["High leverage + thin margins"],
        },
    ]

    def test_save_and_load_roundtrip(self, tmp_db):
        tmp_db.save_scan_results(self.SAMPLE, scan_group="test_group")
        loaded = tmp_db.load_scan_results("test_group", max_age_hours=24)
        assert len(loaded) == 2
        symbols = {r["Symbol"] for r in loaded}
        assert symbols == {"AAPL", "XYZ"}

    def test_load_returns_empty_when_stale(self, tmp_db):
        tmp_db.save_scan_results(self.SAMPLE, scan_group="stale_group")
        # Force staleness by requesting max_age_hours=0
        loaded = tmp_db.load_scan_results("stale_group", max_age_hours=0)
        assert loaded == []

    def test_load_returns_empty_for_missing_group(self, tmp_db):
        loaded = tmp_db.load_scan_results("nonexistent", max_age_hours=24)
        assert loaded == []

    def test_value_traps_serialised_as_list(self, tmp_db):
        tmp_db.save_scan_results(self.SAMPLE, scan_group="traps_test")
        loaded = tmp_db.load_scan_results("traps_test", max_age_hours=24)
        xyz = next(r for r in loaded if r["Symbol"] == "XYZ")
        assert isinstance(xyz["Value Traps"], list)
        assert xyz["Value Traps"] == ["High leverage + thin margins"]

    def test_upsert_overwrites_existing(self, tmp_db):
        tmp_db.save_scan_results(self.SAMPLE, scan_group="upsert_test")
        updated = [{**self.SAMPLE[0], "Damodaran Score": 99.0}]
        tmp_db.save_scan_results(updated, scan_group="upsert_test")
        loaded = tmp_db.load_scan_results("upsert_test", max_age_hours=24)
        aapl = next(r for r in loaded if r["Symbol"] == "AAPL")
        assert aapl["Damodaran Score"] == 99.0

    def test_clear_scan_results(self, tmp_db):
        tmp_db.save_scan_results(self.SAMPLE, scan_group="clear_test")
        tmp_db.clear_scan_results("clear_test")
        loaded = tmp_db.load_scan_results("clear_test", max_age_hours=24)
        assert loaded == []

    def test_results_ordered_by_score_desc(self, tmp_db):
        tmp_db.save_scan_results(self.SAMPLE, scan_group="order_test")
        loaded = tmp_db.load_scan_results("order_test", max_age_hours=24)
        scores = [r["Damodaran Score"] for r in loaded]
        assert scores == sorted(scores, reverse=True)

    def test_skip_entry_without_symbol(self, tmp_db):
        data = [{"Company": "No Symbol Corp", "Damodaran Score": 50}]
        tmp_db.save_scan_results(data, scan_group="nosym")
        loaded = tmp_db.load_scan_results("nosym", max_age_hours=24)
        assert loaded == []


class TestScanMetadata:
    def test_save_and_get_age(self, tmp_db):
        tmp_db.save_scan_metadata("meta_group", ticker_count=50, duration_s=12.5)
        age = tmp_db.get_scan_age("meta_group")
        assert isinstance(age, datetime)
        assert (datetime.utcnow() - age).total_seconds() < 5

    def test_get_scan_age_missing(self, tmp_db):
        age = tmp_db.get_scan_age("nonexistent_group")
        assert age is None

    def test_get_all_cached_groups(self, tmp_db):
        tmp_db.save_scan_metadata("g1", 10, 2.0)
        tmp_db.save_scan_metadata("g2", 20, 3.5)
        groups = tmp_db.get_all_cached_groups()
        names = {g["scan_group"] for g in groups}
        assert {"g1", "g2"} <= names


class TestLiveQuotes:
    QUOTES = {
        "AAPL": {"price": 175.50, "change": 1.20, "pct_change": 0.69, "volume": 55_000_000},
        "MSFT": {"price": 380.00, "change": -2.10, "pct_change": -0.55, "volume": 22_000_000},
    }

    def test_save_and_load_fresh(self, tmp_db):
        tmp_db.save_live_quotes(self.QUOTES)
        loaded = tmp_db.load_live_quotes(["AAPL", "MSFT"], max_age_s=60)
        assert "AAPL" in loaded
        assert loaded["AAPL"]["price"] == 175.50
        assert loaded["MSFT"]["pct_change"] == -0.55

    def test_load_returns_empty_when_stale(self, tmp_db):
        tmp_db.save_live_quotes(self.QUOTES)
        loaded = tmp_db.load_live_quotes(["AAPL"], max_age_s=0)
        assert loaded == {}

    def test_partial_symbols_returned(self, tmp_db):
        tmp_db.save_live_quotes(self.QUOTES)
        loaded = tmp_db.load_live_quotes(["AAPL", "TSLA"], max_age_s=60)
        assert "AAPL" in loaded
        assert "TSLA" not in loaded

    def test_empty_symbols_list(self, tmp_db):
        loaded = tmp_db.load_live_quotes([], max_age_s=60)
        assert loaded == {}


class TestIndexMembers:
    MEMBERS = [
        {"symbol": "AAPL", "company": "Apple Inc.", "sector": "Technology"},
        {"symbol": "MSFT", "company": "Microsoft Corp.", "sector": "Technology"},
        {"symbol": "XOM",  "company": "Exxon Mobil", "sector": "Energy"},
    ]

    def test_save_and_load_fresh(self, tmp_db):
        tmp_db.save_index_members("SP500", self.MEMBERS)
        loaded = tmp_db.load_index_members("SP500", max_age_days=30)
        assert len(loaded) == 3
        symbols = {m["symbol"] for m in loaded}
        assert symbols == {"AAPL", "MSFT", "XOM"}

    def test_load_returns_empty_when_stale(self, tmp_db):
        tmp_db.save_index_members("SP500", self.MEMBERS)
        loaded = tmp_db.load_index_members("SP500", max_age_days=0)
        assert loaded == []

    def test_save_replaces_previous_members(self, tmp_db):
        tmp_db.save_index_members("SP500", self.MEMBERS)
        new = [{"symbol": "NVDA", "company": "Nvidia", "sector": "Technology"}]
        tmp_db.save_index_members("SP500", new)
        loaded = tmp_db.load_index_members("SP500", max_age_days=30)
        assert len(loaded) == 1
        assert loaded[0]["symbol"] == "NVDA"

    def test_load_returns_empty_for_unknown_index(self, tmp_db):
        loaded = tmp_db.load_index_members("NASDAQ100", max_age_days=30)
        assert loaded == []


# ─── SCANNER HELPER TESTS ─────────────────────────────────────────────────────

class TestHelpers:
    """Tests for _sf and _gr — imported directly from scanner module."""

    @pytest.fixture(autouse=True)
    def import_scanner(self):
        # Prevent Streamlit from trying to run set_page_config at import time
        with patch("streamlit.set_page_config"), \
             patch("streamlit.markdown"):
            import damodaran_scanner as scanner
            self.scanner = scanner

    def test_sf_float(self):
        assert self.scanner._sf(3.14) == pytest.approx(3.14)

    def test_sf_string_number(self):
        assert self.scanner._sf("42.0") == pytest.approx(42.0)

    def test_sf_none_returns_default(self):
        assert self.scanner._sf(None, 99.0) == 99.0

    def test_sf_nan_returns_default(self):
        assert self.scanner._sf(float("nan"), -1.0) == -1.0

    def test_sf_non_numeric_returns_default(self):
        assert self.scanner._sf("abc", 5.0) == 5.0

    def test_gr_finds_first_matching_key(self):
        df = pd.DataFrame({"EBIT": [100.0], "Operating Income": [95.0]})
        df.index = ["EBIT"]  # make it look like a financial statement row index
        df2 = pd.DataFrame({"val": [100.0, 95.0]}, index=["EBIT", "Operating Income"])
        result = self.scanner._gr(df2, ["EBIT", "Operating Income"])
        assert result is not None
        assert float(result.iloc[0]) == 100.0

    def test_gr_falls_through_to_second_key(self):
        df = pd.DataFrame({"val": [95.0]}, index=["Operating Income"])
        result = self.scanner._gr(df, ["EBIT", "Operating Income"])
        assert result is not None
        assert float(result.iloc[0]) == 95.0

    def test_gr_returns_none_on_empty_df(self):
        assert self.scanner._gr(pd.DataFrame(), ["EBIT"]) is None

    def test_gr_returns_none_when_no_key_matches(self):
        df = pd.DataFrame({"val": [1.0]}, index=["Revenue"])
        assert self.scanner._gr(df, ["EBIT", "Operating Income"]) is None


# ─── COMPUTE METRICS TESTS ───────────────────────────────────────────────────

def _make_financials(ebit=5_000_000_000):
    """Return a minimal fake income_stmt DataFrame."""
    return pd.DataFrame({"2023": [ebit, 50_000_000_000]}, index=["EBIT", "Total Revenue"])


def _make_balance(equity=20e9, debt=5e9, cash=3e9):
    return pd.DataFrame(
        {"2023": [equity, debt, cash]},
        index=["Stockholders Equity", "Total Debt", "Cash And Cash Equivalents"],
    )


def _make_cashflow(fcf=2_000_000_000, opcf=2_500_000_000, capex=-500_000_000):
    return pd.DataFrame(
        {"2023": [fcf, opcf, capex]},
        index=["Free Cash Flow", "Operating Cash Flow", "Capital Expenditure"],
    )


def _make_info():
    return {
        "shortName": "Test Corp",
        "sector": "Technology",
        "industry": "Software",
        "currentPrice": 100.0,
        "marketCap": 50_000_000_000,
        "sharesOutstanding": 500_000_000,
        "beta": 1.2,
        "effectiveTaxRate": 0.21,
        "trailingPE": 20.0,
        "forwardPE": 18.0,
        "enterpriseToEbitda": 15.0,
        "priceToBook": 5.0,
        "pegRatio": 1.5,
        "returnOnEquity": 0.25,
        "grossMargins": 0.60,
        "operatingMargins": 0.20,
        "profitMargins": 0.15,
        "revenueGrowth": 0.10,
        "earningsGrowth": 0.12,
        "debtToEquity": 25.0,  # yfinance reports as percent (×100)
    }


@pytest.fixture()
def scanner_module():
    with patch("streamlit.set_page_config"), patch("streamlit.markdown"):
        import importlib
        import damodaran_scanner
        importlib.reload(damodaran_scanner)
        yield damodaran_scanner


class TestComputeMetrics:
    def _data(self):
        fi = MagicMock()
        fi.last_price = 100.0
        return {
            "info": _make_info(),
            "financials": _make_financials(),
            "balance": _make_balance(),
            "cashflow": _make_cashflow(),
            "fast_info": fi,
        }

    def test_basic_fields_present(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert m["Symbol"] == "TEST"
        assert m["Company"] == "Test Corp"
        assert m["Sector"] == "Technology"
        assert m["Price"] == 100.0

    def test_roic_computed(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert m["ROIC"] is not None
        assert isinstance(m["ROIC"], float)

    def test_wacc_positive_and_reasonable(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert m["WACC"] is not None
        assert 1.0 < m["WACC"] < 30.0   # expressed as percentage

    def test_spread_equals_roic_minus_wacc(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        if m["ROIC"] and m["WACC"]:
            expected = round(m["ROIC"] - m["WACC"], 2)
            assert m["ROIC-WACC Spread"] == pytest.approx(expected, abs=0.1)

    def test_value_signal_set(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert m["Value Signal"] in {"Value Creator", "Marginal Creator", "Marginal Destroyer", "Value Destroyer", "No Data"}

    def test_damodaran_score_in_range(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert 0 <= m["Damodaran Score"] <= 100

    def test_intrinsic_value_positive_when_fcf_positive(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert m["Intrinsic Value"] is not None
        assert m["Intrinsic Value"] > 0

    def test_value_traps_is_list(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert isinstance(m["Value Traps"], list)

    def test_trap_count_matches_traps_len(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert m["Trap Count"] == len(m["Value Traps"])

    def test_fcf_derived_from_opcf_capex_when_direct_missing(self, scanner_module):
        data = self._data()
        # Remove direct FCF row
        data["cashflow"] = pd.DataFrame(
            {"2023": [2_500_000_000, -500_000_000]},
            index=["Operating Cash Flow", "Capital Expenditure"],
        )
        m = scanner_module.compute_metrics("TEST", data)
        assert m["FCF ($M)"] == pytest.approx(2000.0, abs=1.0)

    def test_no_data_when_missing_financials(self, scanner_module):
        data = self._data()
        data["financials"] = None
        data["balance"] = None
        data["cashflow"] = None
        m = scanner_module.compute_metrics("TEST", data)
        assert m["ROIC"] is None
        assert m["Value Signal"] == "No Data"

    def test_mkt_cap_in_billions(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        assert m["Mkt Cap ($B)"] == pytest.approx(50.0, abs=0.1)

    def test_margin_of_safety_computed(self, scanner_module):
        m = scanner_module.compute_metrics("TEST", self._data())
        if m["Intrinsic Value"] and m["Price"]:
            expected_mos = round((m["Intrinsic Value"] - m["Price"]) / m["Intrinsic Value"] * 100, 1)
            assert m["Margin of Safety %"] == pytest.approx(expected_mos, abs=0.2)

    def test_value_creator_when_roic_far_above_wacc(self, scanner_module):
        data = self._data()
        # Push EBIT high to ensure very high ROIC
        data["financials"] = _make_financials(ebit=15_000_000_000)
        m = scanner_module.compute_metrics("TEST", data)
        if m["ROIC"] and m["WACC"]:
            if (m["ROIC"] - m["WACC"]) > 3:
                assert m["Value Signal"] == "Value Creator"

    def test_value_destroyer_when_roic_below_wacc(self, scanner_module):
        data = self._data()
        # Very low EBIT → very low ROIC
        data["financials"] = _make_financials(ebit=100_000)
        m = scanner_module.compute_metrics("TEST", data)
        if m["ROIC"] is not None and (m["ROIC"] - m["WACC"]) < -3:
            assert m["Value Signal"] == "Value Destroyer"

    def test_score_lower_with_traps(self, scanner_module):
        data_clean = self._data()
        m_clean = scanner_module.compute_metrics("TEST", data_clean)

        # Inject trap conditions: very high D/E, thin margins, negative FCF
        trap_info = _make_info()
        trap_info["debtToEquity"] = 250.0  # 2.5× D/E (×100 in yfinance format)
        trap_info["operatingMargins"] = 0.05  # 5% margins
        data_trap = {
            "info": trap_info,
            "financials": _make_financials(ebit=200_000_000),
            "balance": _make_balance(equity=5e9, debt=12e9, cash=1e9),
            "cashflow": _make_cashflow(fcf=-500_000_000, opcf=100_000_000, capex=-600_000_000),
            "fast_info": data_clean["fast_info"],
        }
        m_trap = scanner_module.compute_metrics("TRAP", data_trap)
        assert m_trap["Damodaran Score"] <= m_clean["Damodaran Score"]


# ─── APPLY LIVE OVERLAY TESTS ─────────────────────────────────────────────────

class TestApplyLiveOverlay:
    @pytest.fixture(autouse=True)
    def _mod(self, scanner_module):
        self.scanner = scanner_module

    def _df(self):
        return pd.DataFrame([
            {"Symbol": "AAPL", "Price": 150.0, "Intrinsic Value": 200.0, "Margin of Safety %": 25.0},
            {"Symbol": "MSFT", "Price": 380.0, "Intrinsic Value": None,  "Margin of Safety %": None},
        ])

    def test_price_updated(self):
        lq = {"AAPL": {"price": 160.0, "pct_change": 1.5}}
        result = self.scanner.apply_live_overlay(self._df(), lq)
        assert result.loc[result["Symbol"] == "AAPL", "Price"].values[0] == 160.0

    def test_mos_recalculated_with_new_price(self):
        lq = {"AAPL": {"price": 160.0, "pct_change": 1.5}}
        result = self.scanner.apply_live_overlay(self._df(), lq)
        expected_mos = round((200.0 - 160.0) / 200.0 * 100, 1)
        assert result.loc[result["Symbol"] == "AAPL", "Margin of Safety %"].values[0] == pytest.approx(expected_mos)

    def test_live_change_added(self):
        lq = {"AAPL": {"price": 160.0, "pct_change": 1.5}}
        result = self.scanner.apply_live_overlay(self._df(), lq)
        assert "Live Change %" in result.columns
        assert result.loc[result["Symbol"] == "AAPL", "Live Change %"].values[0] == 1.5

    def test_msft_price_unchanged_when_not_in_lq(self):
        lq = {"AAPL": {"price": 160.0, "pct_change": 1.5}}
        result = self.scanner.apply_live_overlay(self._df(), lq)
        assert result.loc[result["Symbol"] == "MSFT", "Price"].values[0] == 380.0

    def test_empty_lq_returns_df_unchanged(self):
        df = self._df()
        result = self.scanner.apply_live_overlay(df, {})
        pd.testing.assert_frame_equal(result, df)

    def test_empty_df_returns_empty(self):
        result = self.scanner.apply_live_overlay(pd.DataFrame(), {"AAPL": {"price": 100.0}})
        assert result.empty
