"""
Damodaran Value Scanner v2
===========================
Standalone Streamlit app implementing Aswath Damodaran's valuation framework.

NEW IN V2
---------
  - SQLite database caching (damodaran_db.py) — scan once, load instantly
  - S&P 500 and NASDAQ-100 full constituent scanning (fetched from Wikipedia)
  - Live Price Overlay toggle — lightweight yFinance quote refresh on cached data
  - Refresh Data button — force re-scan with fresh yFinance data
  - Scan groups: results are cached per-group (sp500, nasdaq100, custom, etc.)
  - Scan age display — shows when data was last fetched
  - Cache Manager tab — view/clear cached data, see DB size
  - CSV export

FILES
-----
  damodaran_scanner.py  — This file (UI + analysis logic)
  damodaran_db.py       — SQLite persistence layer
  damodaran_scanner.db  — SQLite database (auto-created)

RUN
---
    pip install streamlit yfinance plotly pandas numpy
    streamlit run damodaran_scanner.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from datetime import datetime, timedelta
import concurrent.futures
import time
import os
import warnings
import damodaran_db as db

import threading
try:
    from curl_cffi import requests as _curl_requests
    _curl_available = True
except ImportError:
    _curl_available = False

_thread_local = threading.local()

def _get_session():
    """Return a thread-local curl_cffi session, creating one if needed."""
    if not _curl_available:
        return None
    if not getattr(_thread_local, "session", None):
        _thread_local.session = _curl_requests.Session(impersonate="chrome")
    return _thread_local.session

warnings.filterwarnings("ignore")

st.set_page_config(page_title="Damodaran Value Scanner", page_icon="🎯", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    .stApp{background-color:#0e1117;color:#e2e8f0}
    [data-testid="metric-container"]{background-color:#1a1f2e;border:1px solid #2d3250;border-radius:10px;padding:16px}
    [data-testid="stSidebar"]{background-color:#161b27}
    div[data-testid="stExpander"]{background-color:#1a1f2e;border:1px solid #2d3250;border-radius:10px}
    .value-creator{background:rgba(34,197,94,.12);border-left:3px solid #22c55e;padding:10px 14px;border-radius:4px;margin:4px 0;color:#e2e8f0}
    .value-destroyer{background:rgba(239,68,68,.12);border-left:3px solid #ef4444;padding:10px 14px;border-radius:4px;margin:4px 0;color:#e2e8f0}
    .cache-info{background:rgba(99,102,241,.10);border-left:3px solid #6366f1;padding:8px 14px;border-radius:4px;margin:4px 0;color:#94a3b8;font-size:.85em}
    h1,h2,h3{color:#e2e8f0}
    .stTabs [data-baseweb="tab-list"]{background:#161b27;border-radius:10px;padding:4px;gap:4px}
    .stTabs [data-baseweb="tab"]{color:#94a3b8;font-weight:600;border-radius:8px;padding:8px 16px}
    .stTabs [aria-selected="true"]{background:#2d3250!important;color:#e2e8f0!important}
</style>
""", unsafe_allow_html=True)

C = {"up":"#22c55e","down":"#ef4444","warn":"#f59e0b","accent":"#6366f1","bg":"#0e1117","text":"#e2e8f0","muted":"#94a3b8"}

# Damodaran sector benchmarks (Jan 2025/2026)
S_WACC={"Technology":.1050,"Communication Services":.0920,"Consumer Cyclical":.0900,"Consumer Defensive":.0750,"Healthcare":.0950,"Financial Services":.0850,"Industrials":.0880,"Energy":.0950,"Basic Materials":.0920,"Real Estate":.0700,"Utilities":.0580}
S_ROIC={"Technology":.2500,"Communication Services":.1200,"Consumer Cyclical":.1400,"Consumer Defensive":.1800,"Healthcare":.1500,"Financial Services":.1100,"Industrials":.1300,"Energy":.1000,"Basic Materials":.0900,"Real Estate":.0600,"Utilities":.0550}
S_EV={"Technology":22.,"Communication Services":12.,"Consumer Cyclical":14.,"Consumer Defensive":16.,"Healthcare":18.,"Financial Services":10.,"Industrials":13.,"Energy":7.,"Basic Materials":9.,"Real Estate":18.,"Utilities":12.}
RF=0.043; ERP=0.0461

CUSTOM_GROUPS={"Mega Cap Tech":["AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA","AVGO","ORCL","ADBE","CRM","AMD","INTC","QCOM","TXN"],"Growth Tech":["PLTR","SNOW","CRWD","PANW","DDOG","NET","SHOP","WDAY","ZS","HUBS","TEAM","ARM","MDB","FTNT","NOW"],"Financials":["JPM","BAC","WFC","GS","MS","BLK","V","MA","AXP","C","SCHW","SPGI","MCO","ICE","COF"],"Healthcare":["UNH","JNJ","LLY","ABBV","MRK","PFE","TMO","ABT","DHR","BMY","AMGN","GILD","REGN","VRTX","ISRG"],"Consumer":["WMT","COST","HD","MCD","SBUX","NKE","TGT","LOW","TJX","BKNG","PG","KO","PEP","CL","PM"],"Industrials & Energy":["CAT","DE","HON","GE","RTX","LMT","BA","UPS","XOM","CVX","COP","SLB","EOG","LIN","APD"]}

def _sf(v,d=0.):
    try:
        f=float(v);return f if f==f else d
    except:return d

def _gr(df,keys):
    if df is None or df.empty:return None
    for k in keys:
        if k in df.index:return df.loc[k]
    return None

def _fetch_wiki_index(name):
    import requests
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
    try:
        if name=="S&P 500":
            url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            html=requests.get(url,headers=HEADERS,timeout=15).text
            df=pd.read_html(html)[0]
            sc="Symbol" if "Symbol" in df.columns else df.columns[0]
            nc="Security" if "Security" in df.columns else None
            sec="GICS Sector" if "GICS Sector" in df.columns else None
            return [{"symbol":str(r[sc]).strip().replace(".","-"),"company":str(r[nc]) if nc else str(r[sc]),"sector":str(r[sec]) if sec else "Unknown"} for _,r in df.iterrows()]
        elif name=="NASDAQ 100":
            url="https://en.wikipedia.org/wiki/Nasdaq-100"
            html=requests.get(url,headers=HEADERS,timeout=15).text
            tables=pd.read_html(html)
            df=None
            for t in tables:
                str_cols=[str(c) for c in t.columns]
                cl=[c.lower() for c in str_cols]
                if "ticker" in cl or "symbol" in cl:df=t;break
            if df is None:df=tables[4] if len(tables)>4 else tables[0]
            str_cols=[str(c) for c in df.columns]
            sc=next((df.columns[i] for i,c in enumerate(str_cols) if c.lower() in ("ticker","symbol")),df.columns[1])
            nc=next((df.columns[i] for i,c in enumerate(str_cols) if c.lower() in ("company","security","name")),df.columns[0])
            members=[]
            for _,r in df.iterrows():
                s=str(r[sc]).strip().replace(".","-")
                if not s or s=="nan" or len(s)>6:continue
                members.append({"symbol":s,"company":str(r[nc]),"sector":"Unknown"})
            return members
    except Exception as e:
        st.warning(f"Could not fetch {name} from Wikipedia: {e}")
    return []

def get_index_members(name):
    cached=db.load_index_members(name,max_age_days=30)
    if cached:return cached
    members=_fetch_wiki_index(name)
    if members:db.save_index_members(name,members)
    return members

def _yf_ticker(symbol):
    s = _get_session()
    return yf.Ticker(symbol, session=s) if s else yf.Ticker(symbol)

def fetch_company_data(ticker):
    try:
        t=_yf_ticker(ticker);info=t.info or {}
        inc=getattr(t,"income_stmt",None)
        if inc is None or (hasattr(inc,"empty") and inc.empty):inc=t.financials
        return {"info":info,"financials":inc,"balance":t.balance_sheet,"cashflow":t.cashflow,"fast_info":t.fast_info}
    except:return {}

def fetch_live_quotes_yf(tickers):
    if not tickers:return {}
    result={}
    try:
        dl_kwargs=dict(period="2d",interval="1d",group_by="ticker",auto_adjust=True,progress=False,threads=True)
        s=_get_session()
        if s:dl_kwargs["session"]=s
        raw=yf.download(tickers,**dl_kwargs)
        for tk in tickers:
            try:
                h=raw if len(tickers)==1 else (raw[tk] if tk in raw.columns.get_level_values(0) else None)
                if h is None:continue
                h=h.dropna(how="all")
                if h.empty:continue
                cur=float(h["Close"].iloc[-1]);prev=float(h["Close"].iloc[-2]) if len(h)>=2 else cur
                chg=cur-prev;pct=(chg/prev*100) if prev else 0
                result[tk]={"price":round(cur,2),"change":round(chg,2),"pct_change":round(pct,2),"volume":int(h["Volume"].iloc[-1]) if "Volume" in h else 0}
            except:continue
    except:pass
    return result

def compute_metrics(ticker,data):
    info=data.get("info",{});fin=data.get("financials");bal=data.get("balance");cf=data.get("cashflow");fi=data.get("fast_info")
    r={"Symbol":ticker,"Company":info.get("shortName",ticker),"Sector":info.get("sector","Unknown"),"Industry":info.get("industry","Unknown")}
    try:price=fi.last_price or info.get("currentPrice",0)
    except:price=info.get("currentPrice",0)
    r["Price"]=round(_sf(price),2)
    mc=_sf(info.get("marketCap",0));r["Mkt Cap ($B)"]=round(mc/1e9,1) if mc else None
    shares=_sf(info.get("sharesOutstanding",0))
    sector=r["Sector"];sw=S_WACC.get(sector,.09);sr=S_ROIC.get(sector,.12)
    r["Sector WACC"]=sw;r["Sector ROIC"]=sr
    ebit_row=_gr(fin,["EBIT","Operating Income"]);tax=_sf(info.get("effectiveTaxRate"),.21)
    if tax>1:tax/=100
    nopat=None
    if ebit_row is not None and len(ebit_row)>0:nopat=_sf(ebit_row.iloc[0])*(1-tax)
    eq=_gr(bal,["Stockholders Equity","Total Stockholders Equity","Stockholders' Equity","Common Stock Equity"])
    dt=_gr(bal,["Total Debt","Long Term Debt","Long Term Debt And Capital Lease Obligation"])
    ca=_gr(bal,["Cash And Cash Equivalents","Cash Cash Equivalents And Short Term Investments","Cash Financial"])
    ev=_sf(eq.iloc[0]) if eq is not None and len(eq)>0 else 0
    dv=_sf(dt.iloc[0]) if dt is not None and len(dt)>0 else 0
    cv=_sf(ca.iloc[0]) if ca is not None and len(ca)>0 else 0
    ic=ev+dv-cv
    roic=nopat/ic if nopat and ic>0 else None
    r["ROIC"]=round(roic*100,2) if roic else None
    beta=_sf(info.get("beta",1.0),1.0);ce=RF+beta*ERP
    ir=_gr(fin,["Interest Expense","Interest Expense Non Operating"])
    ie=abs(_sf(ir.iloc[0])) if ir is not None and len(ir)>0 else 0
    cd=(ie/dv) if dv>0 else .04;cda=cd*(1-tax)
    tc=mc+dv if mc else ev+dv;ew=(mc if mc else ev)/tc if tc>0 else .7;dw=1-ew
    wacc=max(ew*ce+dw*cda,.04)
    r["WACC"]=round(wacc*100,2);r["Cost of Equity"]=round(ce*100,2);r["Beta"]=round(beta,2)
    if roic is not None:
        sp=roic-wacc;r["ROIC-WACC Spread"]=round(sp*100,2)
        if sp>.03:r["Value Signal"]="Value Creator"
        elif sp>0:r["Value Signal"]="Marginal Creator"
        elif sp>-.03:r["Value Signal"]="Marginal Destroyer"
        else:r["Value Signal"]="Value Destroyer"
    else:r["ROIC-WACC Spread"]=None;r["Value Signal"]="No Data"
    r["P/E"]=round(_sf(info.get("trailingPE")),1) or None
    r["Fwd P/E"]=round(_sf(info.get("forwardPE")),1) or None
    r["EV/EBITDA"]=round(_sf(info.get("enterpriseToEbitda")),1) or None
    r["P/B"]=round(_sf(info.get("priceToBook")),2) or None
    r["PEG"]=round(_sf(info.get("pegRatio")),2) or None
    roe=_sf(info.get("returnOnEquity"));r["ROE %"]=round(roe*100,2) if roe else None
    r["Gross Margin %"]=round(_sf(info.get("grossMargins"))*100,1) if info.get("grossMargins") else None
    r["Op Margin %"]=round(_sf(info.get("operatingMargins"))*100,1) if info.get("operatingMargins") else None
    r["Net Margin %"]=round(_sf(info.get("profitMargins"))*100,1) if info.get("profitMargins") else None
    rg=_sf(info.get("revenueGrowth"));eg=_sf(info.get("earningsGrowth"))
    r["Rev Growth %"]=round(rg*100,1) if rg else None;r["Earn Growth %"]=round(eg*100,1) if eg else None
    r["Debt/Equity"]=round(_sf(info.get("debtToEquity"))/100,2) if info.get("debtToEquity") else None
    r["Debt/Capital"]=round(dw,2)
    fr=_gr(cf,["Free Cash Flow"]);opr=_gr(cf,["Operating Cash Flow","Cash Flow From Continuing Operating Activities"]);cxr=_gr(cf,["Capital Expenditure"])
    fcf=_sf(fr.iloc[0]) if fr is not None and len(fr)>0 else None
    if fcf is None and opr is not None and cxr is not None:fcf=_sf(opr.iloc[0])-abs(_sf(cxr.iloc[0]))
    r["FCF ($M)"]=round(fcf/1e6,0) if fcf else None
    r["FCF Yield %"]=round((fcf/mc)*100,2) if fcf and mc else None
    iv=None
    if fcf and fcf>0 and shares>0:
        gr=min(max(_sf(rg,.05),.02),.25);tg=.025;dr=wacc
        pv=0;pf=fcf
        for y in range(1,6):pf*=(1+gr);pv+=pf/(1+dr)**y
        fg=(gr+tg)/2
        for y in range(6,11):pf*=(1+fg);pv+=pf/(1+dr)**y
        tf=pf*(1+tg);tv=tf/(dr-tg);pvt=tv/(1+dr)**10
        eqv=pv+pvt+cv-dv;iv=max(eqv/shares,0)
    r["Intrinsic Value"]=round(iv,2) if iv else None
    r["Margin of Safety %"]=round((iv-price)/iv*100,1) if iv and price>0 else None
    traps=[]
    pe=r.get("P/E");eve=r.get("EV/EBITDA")
    if pe and pe>0 and pe<15 and roic is not None and roic<wacc:traps.append("Low P/E but ROIC < WACC")
    if r.get("Debt/Equity") and r["Debt/Equity"]>1.5 and r.get("Op Margin %") and r["Op Margin %"]<10:traps.append("High leverage + thin margins")
    if rg and rg<-.05 and pe and pe<12:traps.append("Declining revenue — secular decline")
    if fcf and fcf<0 and eve and eve<10:traps.append("Negative FCF + low EV/EBITDA")
    if roe and roic and roe>.20 and roic<.08:traps.append("Leveraged ROE trap")
    r["Value Traps"]=traps;r["Trap Count"]=len(traps)
    score=50
    if roic is not None:score+=min(max((roic-wacc)*100*2,-20),20)
    mos=r.get("Margin of Safety %")
    if mos is not None:score+=min(max(mos*.3,-15),15)
    if r.get("FCF Yield %") and r["FCF Yield %"]>0:score+=min(r["FCF Yield %"]*2,10)
    score-=len(traps)*10
    if rg and rg>.05 and roic and roic>wacc:score+=5
    r["Damodaran Score"]=round(max(0,min(100,score)),0)
    return r

def run_scan(tickers,scan_group,progress_bar=None):
    t0=time.time();total=len(tickers);results=[]
    def _proc(tk):
        try:
            d=fetch_company_data(tk)
            if not d:return None
            return compute_metrics(tk,d)
        except:return None
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures={pool.submit(_proc,tk):tk for tk in tickers}
        for i,(fut) in enumerate(concurrent.futures.as_completed(futures),1):
            m=fut.result()
            if m:results.append(m)
            if progress_bar:
                tk=futures[fut]
                progress_bar.progress(i/total,text=f"Scanning {tk}… ({i}/{total})")
    results=[r for r in results if r];dur=time.time()-t0
    if results:db.save_scan_results(results,scan_group);db.save_scan_metadata(scan_group,len(results),dur)
    df=pd.DataFrame(results)
    if "Damodaran Score" in df.columns:df=df.sort_values("Damodaran Score",ascending=False)
    return df.reset_index(drop=True)

def load_cached(scan_group,max_age_hours=24):
    cached=db.load_scan_results(scan_group,max_age_hours=max_age_hours)
    if cached:
        df=pd.DataFrame(cached)
        if "Damodaran Score" in df.columns:df=df.sort_values("Damodaran Score",ascending=False)
        return df.reset_index(drop=True),True
    return pd.DataFrame(),False

def apply_live_overlay(df,lq):
    if df.empty or not lq:return df
    df=df.copy()
    for idx,row in df.iterrows():
        s=row.get("Symbol")
        if s in lq:
            np_=lq[s]["price"]
            if np_ and np_>0:
                df.at[idx,"Price"]=np_;df.at[idx,"Live Change %"]=lq[s].get("pct_change",0)
                iv=row.get("Intrinsic Value")
                if iv and iv>0:df.at[idx,"Margin of Safety %"]=round((iv-np_)/iv*100,1)
    return df

def _build_roic_wacc(df):
    dp=df.dropna(subset=["ROIC","WACC"]).copy()
    if dp.empty:return go.Figure()
    cm={"Value Creator":C["up"],"Marginal Creator":"#84cc16","Marginal Destroyer":C["warn"],"Value Destroyer":C["down"],"No Data":C["muted"]}
    fig=go.Figure()
    for sig,col in cm.items():
        s=dp[dp["Value Signal"]==sig]
        if s.empty:continue
        fig.add_trace(go.Scatter(x=s["WACC"],y=s["ROIC"],mode="markers+text",marker=dict(size=10,color=col,line=dict(width=.5,color="#0e1117")),text=s["Symbol"],textposition="top center",textfont=dict(size=9,color=C["text"]),name=sig,hovertemplate="<b>%{text}</b><br>ROIC:%{y:.1f}%<br>WACC:%{x:.1f}%<extra></extra>"))
    mx=max(dp["ROIC"].max(),dp["WACC"].max(),20)+5
    fig.add_trace(go.Scatter(x=[0,mx],y=[0,mx],mode="lines",line=dict(dash="dash",color=C["muted"],width=1),showlegend=False,hoverinfo="skip"))
    fig.update_layout(title="ROIC vs WACC — value creation map",xaxis_title="WACC (%)",yaxis_title="ROIC (%)",plot_bgcolor="#0e1117",paper_bgcolor="#0e1117",font=dict(color=C["text"]),height=500,legend=dict(orientation="h",y=-.15))
    fig.update_xaxes(gridcolor="#1e2330",zeroline=False);fig.update_yaxes(gridcolor="#1e2330",zeroline=False)
    return fig

def _build_mos(df):
    dp=df.dropna(subset=["Margin of Safety %"]).sort_values("Margin of Safety %",ascending=True).tail(30)
    if dp.empty:return go.Figure()
    cols=[C["up"] if v>0 else C["down"] for v in dp["Margin of Safety %"]]
    fig=go.Figure(go.Bar(x=dp["Margin of Safety %"],y=dp["Symbol"],orientation="h",marker=dict(color=cols),text=[f"{v:+.0f}%" for v in dp["Margin of Safety %"]],textposition="outside",textfont=dict(size=10,color=C["text"])))
    fig.add_vline(x=0,line=dict(color=C["muted"],width=1,dash="dash"))
    fig.update_layout(title="Margin of safety",xaxis_title="MoS %",plot_bgcolor="#0e1117",paper_bgcolor="#0e1117",font=dict(color=C["text"]),height=max(400,len(dp)*26))
    fig.update_xaxes(gridcolor="#1e2330",zeroline=False);fig.update_yaxes(gridcolor="#1e2330")
    return fig

def _build_score(df):
    fig=go.Figure(go.Histogram(x=df["Damodaran Score"].dropna(),nbinsx=20,marker=dict(color=C["accent"],line=dict(color="#0e1117",width=1))))
    fig.update_layout(title="Score distribution",xaxis_title="Score",yaxis_title="Count",plot_bgcolor="#0e1117",paper_bgcolor="#0e1117",font=dict(color=C["text"]),height=350)
    fig.update_xaxes(gridcolor="#1e2330",zeroline=False);fig.update_yaxes(gridcolor="#1e2330",zeroline=False)
    return fig

def _cs(v):
    if v=="Value Creator":return "color:#22c55e"
    elif v=="Value Destroyer":return "color:#ef4444"
    elif "Destroyer" in str(v):return "color:#f59e0b"
    return ""

def _cm(v):
    try:
        v=float(v)
        if v>20:return "color:#22c55e"
        elif v>0:return "color:#84cc16"
        elif v>-20:return "color:#f59e0b"
        return "color:#ef4444"
    except:return ""

def show_cache_info(sg):
    age=db.get_scan_age(sg)
    if age:
        el=datetime.utcnow()-age
        if el.total_seconds()<3600:a=f"{int(el.total_seconds()/60)} min ago"
        elif el.total_seconds()<86400:a=f"{el.total_seconds()/3600:.1f}h ago"
        else:a=f"{el.days}d ago"
        st.markdown(f'<div class="cache-info">📦 Cached data from <b>{a}</b> ({age.strftime("%Y-%m-%d %H:%M")} UTC). Use 🔄 Refresh to re-scan.</div>',unsafe_allow_html=True)

def render_scanner():
    st.markdown("## 🔍 Damodaran Value Scanner")
    st.caption("Results cached in SQLite — scan once, load instantly. Toggle live prices for real-time MoS updates.")
    with st.sidebar:
        st.markdown("### Scan Target")
        target=st.selectbox("Universe",["S&P 500","NASDAQ 100","Custom Sectors","Custom Tickers"])
        custom_txt="";sel_sec=[]
        if target=="Custom Sectors":sel_sec=st.multiselect("Sectors",list(CUSTOM_GROUPS.keys()),default=list(CUSTOM_GROUPS.keys()))
        elif target=="Custom Tickers":custom_txt=st.text_area("Tickers (comma-separated)",placeholder="AAPL, MSFT, GOOGL",height=80)
        st.markdown("### Filters")
        min_s=st.slider("Min Score",0,100,0);vc_only=st.checkbox("Value creators only");no_traps=st.checkbox("Hide value traps")
        st.markdown("### Cache")
        cache_h=st.number_input("Cache validity (hours)",1,168,24,step=1)

    sg=target.lower().replace(" ","_").replace("&","and")
    if target=="S&P 500":
        with st.spinner("Loading S&P 500…"):members=get_index_members("S&P 500")
        tickers=[m["symbol"] for m in members];st.info(f"S&P 500: {len(tickers)} constituents")
    elif target=="NASDAQ 100":
        with st.spinner("Loading NASDAQ 100…"):members=get_index_members("NASDAQ 100")
        tickers=[m["symbol"] for m in members];st.info(f"NASDAQ 100: {len(tickers)} constituents")
    elif target=="Custom Sectors":
        tickers=[];
        for s in sel_sec:tickers.extend(CUSTOM_GROUPS[s])
        sg="custom_sectors";st.info(f"{len(tickers)} stocks across {len(sel_sec)} sectors")
    else:
        tickers=[t.strip().upper() for t in custom_txt.split(",") if t.strip()];sg="custom_tickers"
        if tickers:st.info(f"{len(tickers)} custom tickers")
    tickers=list(dict.fromkeys(tickers))
    if not tickers:st.warning("No tickers selected.");return

    show_cache_info(sg)
    b1,b2,b3=st.columns(3)
    with b1:load_btn=st.button("📦 Load Cached",width="stretch")
    with b2:scan_btn=st.button("🚀 Run Fresh Scan",type="primary",width="stretch")
    with b3:
        if st.button("🔄 Refresh (Clear Cache)",width="stretch"):
            db.clear_scan_results(sg);st.success("Cache cleared.");st.rerun()

    df=pd.DataFrame()
    if scan_btn:
        prog=st.progress(0,text="Starting…")
        df=run_scan(tickers,sg,progress_bar=prog);prog.empty()
        if not df.empty:st.success(f"✅ {len(df)} stocks scanned and cached.")
        st.session_state[f"df_{sg}"]=df
    elif load_btn or f"df_{sg}" not in st.session_state:
        df,cached=load_cached(sg,max_age_hours=cache_h)
        if not df.empty:st.session_state[f"df_{sg}"]=df
        elif load_btn:st.info("No cache found. Run a fresh scan.");return

    if f"df_{sg}" in st.session_state:df=st.session_state[f"df_{sg}"].copy()
    if df.empty:st.info("Press 'Load Cached' or 'Run Fresh Scan'.");return

    st.divider()
    lc1,lc2=st.columns([3,1])
    with lc1:live_on=st.toggle("⚡ Live Price Overlay",value=False,help="Fetch current prices, overlay on cached fundamentals")
    show_live=False
    if live_on:
        with lc2:
            if st.button("🔄 Refresh Prices",width="stretch"):st.session_state.pop(f"lq_{sg}",None)
        syms=df["Symbol"].tolist()
        cq=db.load_live_quotes(syms,max_age_s=60);miss=[s for s in syms if s not in cq]
        if miss:
            with st.spinner(f"Fetching prices for {len(miss)} stocks…"):
                for i in range(0,len(miss),50):
                    fq=fetch_live_quotes_yf(miss[i:i+50])
                    if fq:db.save_live_quotes(fq);cq.update(fq)
        if cq:df=apply_live_overlay(df,cq);show_live=True
            
    if min_s>0:df=df[df["Damodaran Score"]>=min_s]
    if vc_only:df=df[df["Value Signal"].isin(["Value Creator","Marginal Creator"])]
    if no_traps:df=df[df["Trap Count"]==0]
    if df.empty:st.warning("No stocks match filters.");return

    s1,s2,s3,s4=st.columns(4)
    s1.metric("Scanned",len(df))
    s2.metric("Value Creators",len(df[df["Value Signal"].isin(["Value Creator","Marginal Creator"])]))
    s3.metric("Undervalued",len(df[df.get("Margin of Safety %",pd.Series(dtype=float))>0]) if "Margin of Safety %" in df else 0)
    s4.metric("Value Traps",len(df[df["Trap Count"]>0]))

    st.divider()
    c1,c2=st.columns(2)
    with c1:st.plotly_chart(_build_roic_wacc(df),width="stretch")
    with c2:st.plotly_chart(_build_score(df),width="stretch")
    st.plotly_chart(_build_mos(df),width="stretch")
    st.divider()

    st.markdown("### Full results")
    dcols=["Symbol","Company","Sector","Damodaran Score","ROIC","WACC","ROIC-WACC Spread","Value Signal","Intrinsic Value","Price"]
    if show_live and "Live Change %" in df.columns:dcols.append("Live Change %")
    dcols+=["Margin of Safety %","P/E","EV/EBITDA","FCF Yield %","Rev Growth %","ROE %","Op Margin %","Debt/Equity","Trap Count"]
    dcols=[c for c in dcols if c in df.columns]
    styled=df[dcols].style
    _style_fn = styled.map if hasattr(styled, "map") else styled.applymap
    if "Value Signal" in dcols:styled=_style_fn(_cs,subset=["Value Signal"])
    if "Margin of Safety %" in dcols:styled=_style_fn(_cm,subset=["Margin of Safety %"])
    st.dataframe(styled,hide_index=True,width="stretch",height=min(800,40*(len(df)+1)))
    st.download_button("📥 Download CSV",df.to_csv(index=False),f"damodaran_{sg}.csv","text/csv",width="stretch")

def render_deep_dive():
    st.markdown("## 🎯 Deep Dive Analysis")
    ticker=st.text_input("Ticker","AAPL",placeholder="e.g. AAPL").strip().upper()
    if not ticker:return
    with st.spinner(f"Analysing {ticker}…"):data=fetch_company_data(ticker)
    if not data:st.error(f"No data for {ticker}");return
    m=compute_metrics(ticker,data);info=data["info"]
    st.markdown(f"## {m['Company']} ({ticker})")
    st.caption(f"{m['Sector']} · {m['Industry']}")
    c1,c2,c3,c4,c5=st.columns(5)
    c1.metric("Price",f"${m['Price']:,.2f}");c2.metric("Mkt Cap",f"${m.get('Mkt Cap ($B)',0):,.1f}B");c3.metric("Score",f"{m.get('Damodaran Score','—')}/100")
    iv=m.get("Intrinsic Value");mos=m.get("Margin of Safety %")
    c4.metric("Intrinsic Value",f"${iv:,.2f}" if iv else "N/A")
    c5.metric("Margin of Safety",f"{mos:+.1f}%" if mos else "N/A",delta=f"{mos:+.1f}%" if mos else None,delta_color="normal" if mos and mos>0 else "inverse")
    st.divider()
    st.markdown("### Value creation (ROIC vs WACC)")
    v1,v2,v3,v4=st.columns(4)
    v1.metric("ROIC",f"{m.get('ROIC','—')}%");v2.metric("WACC",f"{m.get('WACC','—')}%")
    sp=m.get("ROIC-WACC Spread");v3.metric("Spread",f"{sp:+.2f}%" if sp else "—");v4.metric("Signal",m.get("Value Signal","—"))
    sig=m.get("Value Signal","")
    if "Creator" in sig:st.markdown(f'<div class="value-creator">✅ <b>{sig}</b> — ROIC exceeds cost of capital.</div>',unsafe_allow_html=True)
    elif "Destroyer" in sig:st.markdown(f'<div class="value-destroyer">⚠️ <b>{sig}</b> — ROIC below cost of capital.</div>',unsafe_allow_html=True)
    st.divider()
    st.markdown("### Quality & profitability")
    q1,q2,q3,q4=st.columns(4)
    q1.metric("ROE",f"{m.get('ROE %','—')}%");q2.metric("Gross Margin",f"{m.get('Gross Margin %','—')}%");q3.metric("Op Margin",f"{m.get('Op Margin %','—')}%");q4.metric("Net Margin",f"{m.get('Net Margin %','—')}%")
    q5,q6,q7,q8=st.columns(4)
    q5.metric("Rev Growth",f"{m.get('Rev Growth %','—')}%");q6.metric("Earn Growth",f"{m.get('Earn Growth %','—')}%");q7.metric("FCF",f"${m.get('FCF ($M)','—')}M");q8.metric("FCF Yield",f"{m.get('FCF Yield %','—')}%")
    st.divider()
    st.markdown("### Relative valuation")
    p1,p2,p3,p4,p5=st.columns(5)
    p1.metric("P/E",m.get("P/E","—"));p2.metric("Fwd P/E",m.get("Fwd P/E","—"));p3.metric("EV/EBITDA",m.get("EV/EBITDA","—"));p4.metric("P/B",m.get("P/B","—"));p5.metric("PEG",m.get("PEG","—"))
    sector=m["Sector"]
    if sector in S_EV and m.get("EV/EBITDA"):
        d=m["EV/EBITDA"]-S_EV[sector];st.caption(f"Sector avg: {S_EV[sector]:.1f}× — {abs(d):.1f}× {'above' if d>0 else 'below'} average")
    st.divider()
    st.markdown("### Capital structure")
    r1,r2,r3,r4=st.columns(4)
    r1.metric("Beta",m.get("Beta","—"));r2.metric("Cost of Equity",f"{m.get('Cost of Equity','—')}%");r3.metric("Debt/Equity",m.get("Debt/Equity","—"));r4.metric("Debt/Capital",m.get("Debt/Capital","—"))
    st.divider()
    st.markdown("### Value trap detection")
    traps=m.get("Value Traps",[])
    if traps:
        for t in traps:st.markdown(f'<div class="value-destroyer">🚩 {t}</div>',unsafe_allow_html=True)
    else:st.markdown('<div class="value-creator">✅ No value trap flags</div>',unsafe_allow_html=True)
    if m.get("Intrinsic Value"):
        with st.expander("📐 DCF assumptions"):
            st.markdown(f"|Param|Value|\n|---|---|\n|Base FCF|${m.get('FCF ($M)',0):,.0f}M|\n|Growth (yr 1-5)|{_sf(info.get('revenueGrowth',.05))*100:.1f}%|\n|Terminal growth|2.5%|\n|WACC|{m.get('WACC',9):.2f}%|\n|Risk-free|{RF*100:.1f}%|\n|ERP|{ERP*100:.2f}%|\n|Beta|{m.get('Beta',1.0)}|")

def render_benchmarks():
    st.markdown("## 📊 Sector Benchmarks")
    rows=[]
    for s in S_WACC:
        w=S_WACC[s];r=S_ROIC[s];sp=r-w
        rows.append({"Sector":s,"WACC (%)":round(w*100,2),"ROIC (%)":round(r*100,2),"Spread (%)":round(sp*100,2),"Avg EV/EBITDA":S_EV[s],"Creation":"✅" if sp>0 else "❌"})
    df=pd.DataFrame(rows).sort_values("Spread (%)",ascending=False)
    st.dataframe(df,hide_index=True,width="stretch")
    fig=go.Figure()
    fig.add_trace(go.Bar(x=df["Sector"],y=df["ROIC (%)"],name="ROIC",marker_color=C["up"]))
    fig.add_trace(go.Bar(x=df["Sector"],y=df["WACC (%)"],name="WACC",marker_color=C["down"]))
    fig.update_layout(title="ROIC vs WACC by sector",barmode="group",plot_bgcolor="#0e1117",paper_bgcolor="#0e1117",font=dict(color=C["text"]),height=450,legend=dict(orientation="h",y=-.2))
    fig.update_xaxes(gridcolor="#1e2330",tickangle=45);fig.update_yaxes(gridcolor="#1e2330",title_text="%")
    st.plotly_chart(fig,width="stretch")

def render_cache():
    st.markdown("## 💾 Cache Manager")
    groups=db.get_all_cached_groups()
    if not groups:st.info("No cached scans.");return
    rows=[]
    for g in groups:
        try:
            lr=datetime.fromisoformat(g["last_run"]);el=datetime.utcnow()-lr
            a=f"{int(el.total_seconds()/60)}m" if el.total_seconds()<3600 else (f"{el.total_seconds()/3600:.1f}h" if el.total_seconds()<86400 else f"{el.days}d")
        except:a="?";lr=None
        rows.append({"Group":g["scan_group"],"Stocks":g["ticker_count"],"Last Run":lr.strftime("%Y-%m-%d %H:%M") if lr else "—","Age":a,"Duration":f"{g['duration_s']:.1f}s" if g.get("duration_s") else "—"})
    st.dataframe(pd.DataFrame(rows),hide_index=True,width="stretch")
    st.divider()
    cg=st.selectbox("Clear cache for:",[g["scan_group"] for g in groups])
    c1,c2=st.columns(2)
    with c1:
        if st.button(f"🗑️ Clear '{cg}'",width="stretch"):db.clear_scan_results(cg);st.success("Cleared.");st.rerun()
    with c2:
        if st.button("🗑️ Clear ALL",width="stretch"):
            for g in groups:db.clear_scan_results(g["scan_group"])
            st.success("All cleared.");st.rerun()
    if os.path.exists(db.DB_PATH):st.caption(f"DB: {db.DB_PATH} ({os.path.getsize(db.DB_PATH)/1024/1024:.2f} MB)")

def render_methodology():
    st.markdown("""## 📖 Methodology
Based on Prof. Aswath Damodaran's valuation frameworks (NYU Stern).

### Three pillars

**Pillar 1: Value creation (ROIC vs WACC)** — A company creates value only when ROIC exceeds WACC. The spread determines whether growth benefits shareholders.

**Pillar 2: Intrinsic value (DCF)** — Two-stage FCFF model: 5 years at current growth, 5 years fading to terminal, discounted at company-specific WACC.

**Pillar 3: Value trap detection** — Five automated flags: low P/E + ROIC < WACC, high leverage + thin margins, declining revenue, negative FCF, leveraged ROE.

### Architecture
- **Full scan** → yFinance → compute metrics → SQLite cache
- **Load cached** → instant from SQLite
- **Live overlay** → lightweight price fetch → overlay on cached fundamentals
- **Refresh** → clear cache → re-scan

### Data sources
- Company data: yFinance
- Sector benchmarks: Damodaran's annual datasets (Jan 2025/2026)
- Index lists: Wikipedia (cached 30 days)

---
*Educational purposes only. Not financial advice.*""")

def main():
    st.markdown("# 🎯 Damodaran Value Scanner")
    st.caption("ROIC vs WACC · DCF intrinsic value · value trap detection — cached in SQLite")
    tabs=st.tabs(["🔍 Scanner","🎯 Deep Dive","📊 Benchmarks","💾 Cache","📖 Methodology"])
    with tabs[0]:render_scanner()
    with tabs[1]:render_deep_dive()
    with tabs[2]:render_benchmarks()
    with tabs[3]:render_cache()
    with tabs[4]:render_methodology()

if __name__=="__main__":main()
