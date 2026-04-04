A Streamlit web application implementing Professor Aswath Damodaran's (NYU Stern) equity valuation
framework. Scan the S&P 500, NASDAQ 100, or any custom list of stocks for value creation signals, intrinsic
value estimates, and value trap flags -- all cached locally in SQLite.
Please use it from Streamlit Community Cloud here https://vgooqrzqsfawixppxhfgnf.streamlit.app/
Please go ahead and tweak and contribute to the larger community.


Limitations & Disclaimer- 
Data quality: All fundamental data is sourced from Yahoo Finance via yFinance. Data may be delayed,
incomplete, or incorrectly classified for some tickers (especially non-US stocks, ADRs, and financial
sector companies).- Financial companies: ROIC/WACC methodology is less meaningful for banks and insurers, where
leverage is operational rather than financial. Treat those signals with caution.- DCF sensitivity: Intrinsic value estimates are highly sensitive to growth rate and WACC assumptions.
Small changes in inputs produce large changes in output. The model is a starting point, not a precise
prediction.- No look-ahead: All data uses the most recent available financial statements. The model does not
incorporate analyst forecasts.- Rate limits: Yahoo Finance enforces request rate limits. If scans fail with network errors, wait a few
minutes and retry. The curl_cffi Chrome impersonation handles most rate-limit responses, but
sustained heavy scanning may still trigger blocks.
This tool is for educational and research purposes only. Nothing in this application constitutes financial advice.
Always conduct your own due diligence before making investment decisions
