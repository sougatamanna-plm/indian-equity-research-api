from fastapi import FastAPI, HTTPException, Query
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import time
import csv
import io
import requests
from urllib.parse import quote
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from nsedata import nse

NSE_BASE = "https://www.nseindia.com"
_nse_session = None
_NSE_SESSION_LOCK = Lock()

# ---------------------------------------------------------
# Master-universe cache
# ---------------------------------------------------------
# The master universe is built from the NSE daily equity bhavcopy.
# Index memberships are fetched only when explicitly requested, because
# NSE's per-symbol getIndexList endpoint can require hundreds/thousands
# of requests for a full market universe.
_MASTER_CACHE = {}
_MASTER_CACHE_LOCK = Lock()

_MEMBERSHIP_CACHE = {}
_MEMBERSHIP_CACHE_LOCK = Lock()

_INDEX_LIST_CACHE = None
_INDEX_LIST_CACHE_LOCK = Lock()

_INDEX_CONSTITUENTS_CACHE = {}
_INDEX_CONSTITUENTS_CACHE_LOCK = Lock()

MASTER_MEMBERSHIP_WORKERS = 6

# Membership enrichment is OPTIONAL. The broad universe must not call
# getIndexList for every stock by default.
MASTER_MEMBERSHIP_MAX_SYMBOLS = 600

# Prevent an accidental request from trying to fetch hundreds of live
# index constituent lists at once.
INDEX_UNION_MAX_INDICES = 25
INDEX_CONSTITUENT_WORKERS = 4


app = FastAPI(
    title="Indian Equity Research API",
    description="Free NSE/BSE research data gateway",
    version="0.6.5",
    docs_url=None,
)


# ---------------------------------------------------------
# Custom low-glare Swagger UI
# ---------------------------------------------------------
# FastAPI's normal /docs page is disabled above so we can inject a
# dark/charcoal stylesheet while keeping the standard Swagger UI,
# OpenAPI schema, Try-it-out functionality, and all API routes intact.
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    response = get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_ui_parameters={
            "syntaxHighlight": {
                "theme": "obsidian"
            }
        },
    )

    html = response.body.decode("utf-8")

    dark_css = """
<style>
    :root {
        color-scheme: dark;
    }

    html, body {
        background: #1f2023 !important;
        color: #e6e8eb !important;
    }

    body {
        margin: 0 !important;
    }

    .swagger-ui {
        color: #e8eaed !important;
    }

    .swagger-ui .topbar {
        background: #17191c !important;
        border-bottom: 1px solid #3a3d42 !important;
    }

    .swagger-ui .info {
        margin: 30px 0 !important;
    }

    .swagger-ui .info .title,
    .swagger-ui .info p,
    .swagger-ui .info li,
    .swagger-ui .info table,
    .swagger-ui .info h1,
    .swagger-ui .info h2,
    .swagger-ui .info h3,
    .swagger-ui .info h4 {
        color: #e8eaed !important;
    }

    .swagger-ui .scheme-container,
    .swagger-ui .opblock-tag-section,
    .swagger-ui section.models {
        background: #1f2023 !important;
        box-shadow: none !important;
    }

    .swagger-ui .opblock-tag {
        color: #e8eaed !important;
        border-bottom-color: #3c4043 !important;
    }

    .swagger-ui .opblock {
        background: #292c31 !important;
        border-color: #464a51 !important;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.25) !important;
    }

    /* Keep expanded Try-it-out / Parameters / Responses areas dark. */
    .swagger-ui .opblock-body,
    .swagger-ui .opblock-body pre,
    .swagger-ui .parameters-container,
    .swagger-ui .responses-wrapper,
    .swagger-ui .responses-inner,
    .swagger-ui .response,
    .swagger-ui .response-col_description,
    .swagger-ui .response-col_links,
    .swagger-ui .response-control-media-type,
    .swagger-ui .response-control-media-type__accept-message,
    .swagger-ui .execute-wrapper {
        background: #24272b !important;
        color: #e6e8eb !important;
    }

    .swagger-ui .opblock-section-header {
        background: #30343a !important;
        border-color: #45464a !important;
        box-shadow: none !important;
        color: #e8eaed !important;
    }

    .swagger-ui .opblock-section-header h4,
    .swagger-ui .opblock-section-header label,
    .swagger-ui .opblock-section-header span {
        color: #e8eaed !important;
    }

    /* Give the response area its own slightly darker surface so it is
       visually distinct from the expanded operation/parameter area. */
    .swagger-ui .responses-wrapper,
    .swagger-ui .responses-inner {
        background: #1f2226 !important;
        border-top: 1px solid #3b4047 !important;
    }

    .swagger-ui .response-col_description {
        background: #23262b !important;
    }

    .swagger-ui .responses-table,
    .swagger-ui .responses-table tbody tr,
    .swagger-ui .responses-table tbody tr td,
    .swagger-ui .responses-table thead tr th,
    .swagger-ui .responses-table thead tr td {
        background: #24272b !important;
        color: #e6e8eb !important;
        border-color: #45464a !important;
    }

    .swagger-ui .parameter__name,
    .swagger-ui .parameter__type,
    .swagger-ui .parameter__deprecated,
    .swagger-ui .parameter__in,
    .swagger-ui .parameter__extension,
    .swagger-ui .parameter__empty_value_toggle {
        color: #d7d9dc !important;
    }

    .swagger-ui .opblock .opblock-summary {
        border-color: #45464a !important;
    }

    .swagger-ui .opblock .opblock-summary-description,
    .swagger-ui .opblock .opblock-summary-path,
    .swagger-ui .opblock .opblock-summary-path__deprecated,
    .swagger-ui .opblock-description-wrapper p,
    .swagger-ui .opblock-external-docs-wrapper,
    .swagger-ui .opblock-title_normal {
        color: #d7d9dc !important;
    }

    .swagger-ui label,
    .swagger-ui .parameter__name,
    .swagger-ui .parameter__type,
    .swagger-ui .response-col_status,
    .swagger-ui .response-col_links,
    .swagger-ui table thead tr th,
    .swagger-ui table thead tr td {
        color: #e8eaed !important;
    }

    .swagger-ui .model-title,
    .swagger-ui .model,
    .swagger-ui section.models h4 {
        color: #e8eaed !important;
    }

    .swagger-ui .model-box,
    .swagger-ui .model-container {
        background: #25282d !important;
    }

    .swagger-ui input[type=text],
    .swagger-ui textarea,
    .swagger-ui select {
        background: #2b2f35 !important;
        color: #eef0f2 !important;
        border: 1px solid #5f6368 !important;
    }

    .swagger-ui input::placeholder,
    .swagger-ui textarea::placeholder {
        color: #9aa0a6 !important;
    }

    .swagger-ui .highlight-code,
    .swagger-ui .microlight,
    .swagger-ui pre {
        background: #15171a !important;
        color: #e6e8eb !important;
    }

    .swagger-ui .response-col_description__inner p,
    .swagger-ui .renderedMarkdown p,
    .swagger-ui .renderedMarkdown li {
        color: #d7d9dc !important;
    }

    .swagger-ui a {
        color: #8ab4f8 !important;
    }

    .swagger-ui a:hover {
        color: #aecbfa !important;
    }

    .swagger-ui .btn,
    .swagger-ui .try-out__btn,
    .swagger-ui .btn.cancel {
        background: #2b2f35 !important;
        color: #e6e8eb !important;
        border: 1px solid #5f6368 !important;
        box-shadow: none !important;
    }

    .swagger-ui .execute-wrapper .btn {
        background: #3a4a5e !important;
        color: #f0f3f7 !important;
        border: 1px solid #65788f !important;
        box-shadow: none !important;
    }

    .swagger-ui .try-out__btn:hover,
    .swagger-ui .btn.cancel:hover,
    .swagger-ui .btn:hover {
        background: #41464d !important;
        color: #ffffff !important;
        border-color: #80868b !important;
    }

    .swagger-ui .execute-wrapper .btn:hover {
        background: #465b73 !important;
        color: #ffffff !important;
        border-color: #8a9bb0 !important;
    }

    .swagger-ui .loading-container .loading::after {
        border-color: #8ab4f8 transparent #8ab4f8 transparent !important;
    }

    .swagger-ui .responses-inner h4,
    .swagger-ui .responses-inner h5 {
        color: #e8eaed !important;
    }

    .swagger-ui .tab li {
        color: #bdc1c6 !important;
    }

    .swagger-ui .tab li.active {
        color: #8ab4f8 !important;
    }

    .swagger-ui .servers > label,
    .swagger-ui .servers-title {
        color: #e8eaed !important;
    }

    .swagger-ui .servers select {
        background: #2b2f35 !important;
        color: #e6e8eb !important;
        border-color: #5f6368 !important;
    }
</style>
"""

    html = html.replace("</head>", dark_css + "\n</head>", 1)
    response.body = html.encode("utf-8")
    response.headers["content-length"] = str(len(response.body))
    return response


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


def dataframe_to_records(df):
    """Convert a pandas DataFrame to JSON-safe records."""
    if df is None:
        return []

    if hasattr(df, "to_json"):
        return json.loads(
            df.to_json(
                orient="records",
                date_format="iso"
            )
        )

    return df

# ---------------------------------------------------------
# NSE live API session
# ---------------------------------------------------------
def get_nse_session(force_refresh=False):
    """
    Create and warm an NSE browser-like session.

    NSE's live web endpoints use bot protection and may require
    cookies obtained from normal website navigation before API calls.
    """
    global _nse_session

    if _nse_session is not None and not force_refresh:
        return _nse_session

    with _NSE_SESSION_LOCK:
        if _nse_session is not None and not force_refresh:
            return _nse_session

        session = requests.Session()

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
        })

        session.get(
            NSE_BASE + "/",
            headers={
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,image/avif,"
                    "image/webp,*/*;q=0.8"
                ),
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            },
            timeout=20,
        )

        time.sleep(0.8)

        session.get(
            NSE_BASE + "/market-data/live-equity-market",
            headers={
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,image/avif,"
                    "image/webp,*/*;q=0.8"
                ),
                "Referer": NSE_BASE + "/",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
            },
            timeout=20,
        )

        time.sleep(0.8)

        _nse_session = session
        return _nse_session


def nse_api_get(path, params=None, retries=2):
    """
    Request an NSE live JSON endpoint using the warmed session.

    Automatically refreshes the session after common NSE anti-bot/session
    failures (401/403/429 and selected 5xx responses).
    """
    global _nse_session

    last_error = None

    for attempt in range(retries + 1):
        session = get_nse_session(
            force_refresh=(attempt > 0)
        )

        try:
            response = session.get(
                NSE_BASE + path,
                params=params,
                headers={
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate",
                    "Referer": NSE_BASE + "/market-data/live-equity-market",
                    "X-Requested-With": "XMLHttpRequest",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/146.0.0.0 Safari/537.36"
                    ),
                },
                timeout=30,
            )

            if response.status_code in {401, 403, 429} or response.status_code >= 500:
                last_error = (
                    f"HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )

                if attempt < retries:
                    with _NSE_SESSION_LOCK:
                        _nse_session = None

                    time.sleep(1.0 + attempt)
                    continue

            response.raise_for_status()
            return response.json()

        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)

            if attempt < retries:
                with _NSE_SESSION_LOCK:
                    _nse_session = None

                time.sleep(1.0 + attempt)
                continue

            raise

    raise RuntimeError(last_error or "NSE API request failed")



# ---------------------------------------------------------
# NSE equity quote / company data
# ---------------------------------------------------------
_QUOTE_CACHE = {}
_QUOTE_CACHE_LOCK = Lock()


def _get_equity_quote_cached(symbol, section=None):
    """Fetch NSE's current equity quote endpoint with caching."""
    symbol_name = _normalize_symbol(symbol)
    if not symbol_name:
        raise ValueError("A valid NSE equity symbol is required.")

    section_name = (
        str(section).strip()
        if section is not None and str(section).strip()
        else None
    )
    cache_key = (symbol_name, section_name)

    with _QUOTE_CACHE_LOCK:
        if cache_key in _QUOTE_CACHE:
            return _QUOTE_CACHE[cache_key]

    params = {"symbol": symbol_name}
    if section_name:
        params["section"] = section_name

    errors = []

    try:
        payload = nse_api_get("/api/quote-equity", params=params)
        result = {
            "source": "NSE",
            "symbol": symbol_name,
            "endpoint": "/api/quote-equity",
            "section": section_name,
            "data": payload,
        }
        with _QUOTE_CACHE_LOCK:
            _QUOTE_CACHE[cache_key] = result
        return result
    except Exception as e:
        errors.append(f"quote-equity: {str(e)}")

    # NSE may return 403 for /api/quote-equity, especially for
    # section=trade_info. The NextApi getSymbolData endpoint is a
    # richer fallback and already contains tradeInfo, priceInfo,
    # securityInfo, metaData and indexList in the equityResponse.
    # Therefore use it for BOTH normal quote and trade_info requests.
    try:
        payload = nse_api_get(
            "/api/NextApi/apiClient/GetQuoteApi",
            params={
                "functionName": "getSymbolData",
                "marketType": "N",
                "series": "EQ",
                "symbol": symbol_name,
            }
        )

        result = {
            "source": "NSE",
            "symbol": symbol_name,
            "endpoint": "/api/NextApi/apiClient/GetQuoteApi",
            "functionName": "getSymbolData",
            "section": section_name,
            "fallback": True,
            "data": payload,
            "errors": errors,
        }

        if section_name == "trade_info":
            equity_response = payload.get("equityResponse", []) if isinstance(payload, dict) else []
            if equity_response and isinstance(equity_response[0], dict):
                row = equity_response[0]
                result["trade_info"] = row.get("tradeInfo", {})
                result["security_info"] = row.get("securityInfo", {})
                result["price_info"] = row.get("priceInfo", {})
                result["meta_data"] = row.get("metaData", {})

        with _QUOTE_CACHE_LOCK:
            _QUOTE_CACHE[cache_key] = result
        return result
    except Exception as e:
        errors.append(f"getSymbolData fallback: {str(e)}")

    raise RuntimeError(
        f"NSE equity quote unavailable for '{symbol_name}'. "
        + " | ".join(errors)
    )


@app.get("/nse/quote-equity")
def nse_quote_equity(
    symbol: str = Query(
        ...,
        description="NSE equity symbol, e.g. RELIANCE, TCS, NETWEB"
    ),
    section: str | None = Query(
        None,
        description="Optional NSE quote section. Use 'trade_info' for liquidity and delivery data."
    )
):
    """Return current NSE quote data for one equity."""
    symbol_name = symbol.strip().upper()
    if not symbol_name:
        raise HTTPException(status_code=400, detail="A valid NSE stock symbol is required.")
    try:
        return _get_equity_quote_cached(symbol_name, section)
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE equity quote unavailable for symbol '{symbol_name}'. Underlying error: {str(e)}"
        )


@app.get("/nse/equity-meta-info")
def nse_equity_meta_info(
    symbol: str = Query(
        ...,
        description="NSE equity symbol, e.g. RELIANCE, TCS, NETWEB"
    )
):
    """Return NSE company/entity metadata for one equity."""
    symbol_name = symbol.strip().upper()
    if not symbol_name:
        raise HTTPException(status_code=400, detail="A valid NSE stock symbol is required.")
    try:
        payload = nse_api_get("/api/equity-meta-info", params={"symbol": symbol_name})
        return {
            "source": "NSE",
            "symbol": symbol_name,
            "endpoint": "/api/equity-meta-info",
            "data": payload,
        }
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE equity metadata unavailable for symbol '{symbol_name}'. Underlying error: {str(e)}"
        )


# ---------------------------------------------------------
# Master NSE equity universe helpers
# ---------------------------------------------------------
def _normalize_symbol(value):
    """Normalize an NSE symbol to a clean uppercase string."""
    if value is None:
        return None

    value = str(value).strip().upper()

    if not value or value in {"NAN", "NONE", "NULL"}:
        return None

    return value


def _extract_equity_symbols(df):
    """
    Extract a de-duplicated equity symbol universe from the NSE
    securities bhavcopy while preserving the source order.
    """
    if df is None:
        return []

    if hasattr(df, "columns"):
        # nse-archives column names can vary by release/version.
        candidates = [
            "symbol",
            "Symbol",
            "SYMBOL",
            "Symbol ",
            "SYMBOL "
        ]

        column = next(
            (c for c in candidates if c in df.columns),
            None
        )

        if column is not None:
            values = df[column].tolist()
        else:
            # Last-resort case-insensitive lookup.
            column = next(
                (
                    c for c in df.columns
                    if str(c).strip().lower() == "symbol"
                ),
                None
            )
            values = df[column].tolist() if column else []
    else:
        values = []

    symbols = []
    seen = set()

    for value in values:
        symbol = _normalize_symbol(value)

        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)

    return symbols


def _get_stock_index_membership_cached(symbol):
    """
    Fetch one stock's index membership through NSE's current
    getIndexList API, with an in-process cache.
    """
    symbol_name = _normalize_symbol(symbol)

    if not symbol_name:
        return {
            "symbol": symbol_name,
            "count": 0,
            "indices": [],
            "error": "Invalid symbol"
        }

    with _MEMBERSHIP_CACHE_LOCK:
        if symbol_name in _MEMBERSHIP_CACHE:
            return _MEMBERSHIP_CACHE[symbol_name]

    try:
        payload = nse_api_get(
            "/api/NextApi/apiClient/GetQuoteApi",
            params={
                "functionName": "getIndexList",
                "symbol": symbol_name
            }
        )

        if isinstance(payload, dict):
            data = payload.get("data", [])

            if not data:
                for key in ("indexList", "indices", "equityResponse"):
                    if isinstance(payload.get(key), list):
                        data = payload.get(key)
                        break
        else:
            data = payload

        if data is None:
            data = []

        if not isinstance(data, list):
            data = [data]

        result = {
            "symbol": symbol_name,
            "count": len(data),
            "indices": data
        }

        with _MEMBERSHIP_CACHE_LOCK:
            _MEMBERSHIP_CACHE[symbol_name] = result

        return result

    except Exception as e:
        return {
            "symbol": symbol_name,
            "count": 0,
            "indices": [],
            "error": str(e)
        }


def _build_master_universe(
    date,
    include_membership=False,
    max_symbols=None,
    membership_offset=0
):
    """
    Build the broad NSE equity universe from the daily securities
    bhavcopy.

    DEFAULT:
        include_membership=False
        -> returns the broad universe without any per-symbol live API calls.

    OPTIONAL:
        include_membership=True
        -> enriches only the requested slice of the universe with
           getIndexList data.

    IMPORTANT:
        This endpoint does NOT require membership enrichment for broad
        discovery. Use /nse/index-union when the goal is thematic/index
        discovery.
    """
    date_info = resolve_effective_eod_date(date)
    effective_date = date_info["effective_date"]

    cache_key = (
        effective_date,
        bool(include_membership),
        int(max_symbols) if max_symbols is not None else None,
        int(membership_offset)
    )

    with _MASTER_CACHE_LOCK:
        if cache_key in _MASTER_CACHE:
            return _MASTER_CACHE[cache_key]

    df = nse.get(
        "capital_market",
        "equities_sme",
        "sec_bhavdata_full",
        effective_date
    )

    all_symbols = _extract_equity_symbols(df)
    total_count = len(all_symbols)

    # max_symbols + membership_offset are the public pagination controls for
    # the master universe. The full universe is still built internally so the
    # total count remains stable and downstream discovery can request the full
    # universe with max_symbols=None. When membership enrichment is enabled
    # without an explicit max_symbols, retain the historical live-call safety cap.
    offset = max(0, int(membership_offset))
    if max_symbols is None:
        if include_membership:
            limit = min(MASTER_MEMBERSHIP_MAX_SYMBOLS, max(0, total_count - offset))
        else:
            limit = max(0, total_count - offset)
    else:
        limit = max(0, min(int(max_symbols), max(0, total_count - offset)))

    symbols = all_symbols[offset:offset + limit]

    result = {
        "source": "NSE",
        "requested_date": date_info["requested_date"],
        "effective_date": effective_date,
        "date": effective_date,
        "date_adjusted": date_info["date_adjusted"],
        "adjustment_reason": date_info["adjustment_reason"],
        "calendar_source": date_info["calendar_source"],
        "calendar_error": date_info["calendar_error"],
        "dataset": "sec_bhavdata_full",
        "count": total_count,
        "total_count": total_count,
        "returned_count": len(symbols),
        "offset": offset,
        "limit": limit,
        "symbols": symbols,
        "membership_included": bool(include_membership),
        "membership_strategy": (
            "disabled_by_default"
            if not include_membership
            else "targeted_slice"
        )
    }

    if include_membership:
        # symbols is already the requested public page. Do not apply the offset
        # a second time when selecting symbols for membership enrichment.
        selected = symbols
        membership = {}

        with ThreadPoolExecutor(
            max_workers=MASTER_MEMBERSHIP_WORKERS
        ) as executor:
            futures = {
                executor.submit(
                    _get_stock_index_membership_cached,
                    symbol
                ): symbol
                for symbol in selected
            }

            for future in as_completed(futures):
                symbol = futures[future]

                try:
                    membership[symbol] = future.result()
                except Exception as e:
                    membership[symbol] = {
                        "symbol": symbol,
                        "count": 0,
                        "indices": [],
                        "error": str(e)
                    }

        result["membership_offset"] = offset
        result["membership_limit"] = limit
        result["membership_count"] = len(membership)

        result["stocks"] = [
            {
                "symbol": symbol,
                "indices": membership.get(
                    symbol,
                    {
                        "symbol": symbol,
                        "count": 0,
                        "indices": []
                    }
                ).get("indices", []),
                "index_count": membership.get(
                    symbol,
                    {
                        "symbol": symbol,
                        "count": 0,
                        "indices": []
                    }
                ).get("count", 0),
                **(
                    {
                        "membership_error": membership[symbol]["error"]
                    }
                    if membership.get(symbol, {}).get("error")
                    else {}
                )
            }
            for symbol in selected
        ]

    with _MASTER_CACHE_LOCK:
        _MASTER_CACHE[cache_key] = result

    return result



@app.get("/nse/master-universe")
def nse_master_universe(
    date: str | None = Query(
        None,
        description="Optional trading date in YYYY-MM-DD format; defaults to the latest completed NSE trading day"
    ),
    include_membership: bool = Query(
        False,
        description=(
            "Optional stock-to-index enrichment. Keep false for broad "
            "universe discovery. When true, the returned max_symbols page "
            "starting at membership_offset is enriched."
        )
    ),
    max_symbols: int | None = Query(
        None,
        ge=1,
        le=2000,
        description=(
            "Maximum number of symbols returned from the master universe. "
            "When include_membership=true, the returned slice is also enriched. "
            "Omit for the full universe (or the membership safety cap when enrichment is enabled)."
        )
    ),
    membership_offset: int = Query(
        0,
        ge=0,
        description=(
            "Starting position in the master symbol list for pagination. "
            "When include_membership=true, this is also the membership enrichment offset."
        )
    )
):
    """
    Return the broad NSE equity discovery universe for a trading date.

    Broad mode:
        include_membership=false
        -> max_symbols and membership_offset paginate the returned symbol list.

    Targeted enrichment mode:
        include_membership=true&max_symbols=5&membership_offset=20
        -> enriches exactly the returned page.

    The membership flag is intentionally retained for diagnostics and
    targeted enrichment; it is NOT required for broad discovery.
    """
    try:
        return _build_master_universe(
            date=date,
            include_membership=include_membership,
            max_symbols=max_symbols,
            membership_offset=membership_offset
        )

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE master-universe data unavailable for {date}. "
                f"Underlying error: {str(e)}"
            )
        )


def _classify_index(index_record):
    """
    Best-effort classification using NSE's own indexType when available,
    with a conservative name-based fallback.
    """
    raw_type = str(index_record.get("indexType") or "").strip()

    if raw_type:
        lowered = raw_type.lower()
        if "broad" in lowered:
            return "broad_market"
        if "sector" in lowered:
            return "sectoral"
        if "thematic" in lowered:
            return "thematic"
        if "strategy" in lowered:
            return "strategy"
        if "fixed" in lowered or "bond" in lowered:
            return "fixed_income"
        if "hybrid" in lowered:
            return "hybrid"

    name = str(
        index_record.get("indexSymbol")
        or index_record.get("index")
        or ""
    ).upper()

    if any(
        token in name
        for token in (
            "NIFTY 50", "NIFTY 100", "NIFTY 200", "NIFTY 500",
            "NEXT 50", "NEXT 100", "MIDCAP", "SMALLCAP", "MICROCAP",
            "TOTAL MARKET", "LARGEMIDCAP", "MIDSMALLCAP"
        )
    ):
        return "broad_market"

    sector_tokens = (
        "BANK", "IT", "FMCG", "PHARMA", "HEALTHCARE", "AUTO",
        "CEMENT", "CHEMICAL", "FINANCIAL", "METAL", "MEDIA",
        "REALTY", "POWER", "TELECOMMUNICATION", "INSURANCE",
        "CAPITAL GOODS", "CONSTRUCTION", "CONSUMER DURABLES"
    )

    if any(token in name for token in sector_tokens):
        return "sectoral"

    strategy_tokens = (
        "EQUAL WEIGHT", "QUALITY", "VALUE", "LOW VOLATILITY",
        "ALPHA", "MOMENTUM", "DIVIDEND", "SHARIAH"
    )

    if any(token in name for token in strategy_tokens):
        return "strategy"

    fixed_tokens = (
        "G-SEC", "GSEC", "GOVT", "SDL", "CORPORATE BOND",
        "BOND", "TREASURY"
    )

    if any(token in name for token in fixed_tokens):
        return "fixed_income"

    return "other"


def _get_index_catalogue_cached():
    global _INDEX_LIST_CACHE

    with _INDEX_LIST_CACHE_LOCK:
        if _INDEX_LIST_CACHE is not None:
            return _INDEX_LIST_CACHE

    payload = nse_api_get("/api/allIndices")
    data = payload.get("data", []) if isinstance(payload, dict) else []

    compact = []

    for item in data:
        record = {
            "indexSymbol": item.get("indexSymbol"),
            "index": item.get("index"),
            "key": item.get("key"),
            "indexType": item.get("indexType"),
            "last": item.get("last"),
            "variation": item.get("variation"),
            "percentChange": item.get("percentChange"),
        }
        record["category"] = _classify_index(record)
        compact.append(record)

    result = {
        "source": "NSE",
        "endpoint": "/api/allIndices",
        "count": len(data),
        "indices": compact,
        "raw_available": True,
    }

    with _INDEX_LIST_CACHE_LOCK:
        _INDEX_LIST_CACHE = result

    return result


@app.get("/nse/index-list")
def nse_index_list():
    """
    Return the currently discoverable NSE index universe.

    This is the index catalogue/discovery layer. It does NOT fetch
    constituents for every index.
    """
    try:
        return _get_index_catalogue_cached()

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE index catalogue error: {str(e)}"
        )


def _normalize_index_name(value):
    if value is None:
        return None

    value = str(value).strip().upper()

    if not value or value in {"NAN", "NONE", "NULL"}:
        return None

    return value


def _extract_index_constituents(payload):
    """
    Normalize the standard NSE equity-stockIndices response.
    """
    if isinstance(payload, dict):
        data = payload.get("data", [])
    else:
        data = payload

    if data is None:
        return []

    if not isinstance(data, list):
        data = [data]

    return data


def _get_index_constituents_cached(index_name):
    index_name = _normalize_index_name(index_name)

    if not index_name:
        return {
            "index": index_name,
            "count": 0,
            "data": [],
            "error": "Invalid index name"
        }

    with _INDEX_CONSTITUENTS_CACHE_LOCK:
        if index_name in _INDEX_CONSTITUENTS_CACHE:
            return _INDEX_CONSTITUENTS_CACHE[index_name]

    errors = []

    try:
        payload = nse_api_get(
            "/api/equity-stockIndices",
            params={"index": index_name}
        )

        data = _extract_index_constituents(payload)

        result = {
            "source": "NSE",
            "index": index_name,
            "endpoint": "/api/equity-stockIndices",
            "count": len(data),
            "data": data,
        }

        with _INDEX_CONSTITUENTS_CACHE_LOCK:
            _INDEX_CONSTITUENTS_CACHE[index_name] = result

        return result

    except Exception as e:
        errors.append(f"standard endpoint: {str(e)}")

    try:
        payload = nse_api_get(
            "/api/NextApi/apiClient/GetQuoteApi",
            params={
                "functionName": "getEquityStockIndices",
                "index": index_name,
            }
        )

        data = _extract_index_constituents(payload)

        result = {
            "source": "NSE",
            "index": index_name,
            "endpoint": "/api/NextApi/apiClient/GetQuoteApi",
            "functionName": "getEquityStockIndices",
            "count": len(data),
            "data": data,
            "raw": payload,
        }

        with _INDEX_CONSTITUENTS_CACHE_LOCK:
            _INDEX_CONSTITUENTS_CACHE[index_name] = result

        return result

    except Exception as e:
        errors.append(f"NextApi endpoint: {str(e)}")

    return {
        "source": "NSE",
        "index": index_name,
        "count": 0,
        "data": [],
        "errors": errors,
    }


@app.get("/nse/index-constituents")
def nse_index_constituents(
    index: str = Query(
        ...,
        description=(
            "NSE index name, e.g. NIFTY 50, NIFTY 500, "
            "NIFTY MIDCAP 150, NIFTY BANK"
        )
    )
):
    """
    Return constituent-level data for one NSE equity index.

    Primary endpoint:
        /api/equity-stockIndices?index=...

    NSE's allIndices endpoint is used only for index discovery.
    """
    index_name = _normalize_index_name(index)

    if not index_name:
        raise HTTPException(
            status_code=400,
            detail="A valid NSE index name is required."
        )

    result = _get_index_constituents_cached(index_name)

    if result.get("count", 0) == 0 and result.get("errors"):
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE constituent data unavailable for index '{index}'. "
                + " | ".join(result["errors"])
            )
        )

    return {
        "source": "NSE",
        "index_requested": index,
        "index_normalized": index_name,
        "endpoint": result.get("endpoint"),
        "functionName": result.get("functionName"),
        "count": result.get("count", 0),
        "data": result.get("data", []),
        **(
            {"raw": result["raw"]}
            if "raw" in result
            else {}
        ),
    }


@app.get("/nse/index-union")
def nse_index_union(
    indices: str = Query(
        ...,
        description=(
            "Comma-separated NSE indices. Example: "
            "NIFTY 500,NIFTY MIDCAP 150,NIFTY INDIA DIGITAL"
        )
    ),
    max_indices: int = Query(
        INDEX_UNION_MAX_INDICES,
        ge=1,
        le=INDEX_UNION_MAX_INDICES,
        description="Maximum number of index constituent lists to fetch."
    )
):
    """
    Build a deduplicated stock universe from multiple NSE indices.

    This is intentionally separate from /nse/master-universe:
      - master-universe = broad security discovery
      - index-union = index/thematic discovery
      - stock-index-membership = reverse stock -> index lookup

    It does NOT call getIndexList for every stock.
    """
    requested = [
        _normalize_index_name(item)
        for item in indices.split(",")
    ]
    requested = [item for item in requested if item]

    unique_indices = []
    seen = set()

    for item in requested:
        if item not in seen:
            seen.add(item)
            unique_indices.append(item)

    if not unique_indices:
        raise HTTPException(
            status_code=400,
            detail="At least one valid index name is required."
        )

    if len(unique_indices) > max_indices:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Requested {len(unique_indices)} indices. "
                f"Maximum allowed is {max_indices}."
            )
        )

    results = {}

    with ThreadPoolExecutor(
        max_workers=min(INDEX_CONSTITUENT_WORKERS, len(unique_indices))
    ) as executor:
        futures = {
            executor.submit(
                _get_index_constituents_cached,
                index_name
            ): index_name
            for index_name in unique_indices
        }

        for future in as_completed(futures):
            index_name = futures[future]

            try:
                results[index_name] = future.result()
            except Exception as e:
                results[index_name] = {
                    "source": "NSE",
                    "index": index_name,
                    "count": 0,
                    "data": [],
                    "errors": [str(e)],
                }

    union = []
    seen_symbols = set()
    membership_map = {}

    for index_name in unique_indices:
        result = results.get(index_name, {})
        data = result.get("data", [])

        for row in data:
            if not isinstance(row, dict):
                continue

            symbol = _normalize_symbol(row.get("symbol"))

            if not symbol or symbol == index_name:
                continue

            if symbol not in seen_symbols:
                seen_symbols.add(symbol)
                union.append(symbol)

            membership_map.setdefault(symbol, []).append(index_name)

    return {
        "source": "NSE",
        "indices_requested": unique_indices,
        "index_count": len(unique_indices),
        "successful_indices": [
            index_name
            for index_name in unique_indices
            if results.get(index_name, {}).get("count", 0) > 0
        ],
        "failed_indices": {
            index_name: results.get(index_name, {}).get("errors", [])
            for index_name in unique_indices
            if results.get(index_name, {}).get("errors")
        },
        "stock_count": len(union),
        "symbols": union,
        "index_memberships": membership_map,
    }


@app.get("/nse/master-discovery")
def nse_master_discovery(
    date: str | None = Query(
        None,
        description="Optional trading date in YYYY-MM-DD format; defaults to the latest completed NSE trading day"
    ),
    indices: str = Query(
        "NIFTY 500,NIFTY MIDCAP 150,NIFTY SMALLCAP 250,NIFTY MICROCAP 250",
        description=(
            "Comma-separated discovery indices to union with the broad "
            "NSE master universe."
        )
    ),
    include_membership: bool = Query(
        False,
        description=(
            "Optional targeted reverse membership enrichment. Disabled "
            "by default."
        )
    ),
    membership_limit: int = Query(
        0,
        ge=0,
        le=200,
        description=(
            "If include_membership=true, enrich this many symbols from "
            "the final discovery union. 0 means do not enrich."
        )
    )
):
    """
    Main discovery endpoint for the next screening stage.

    It combines:
      1. NSE daily equity universe
      2. Selected broad/thematic/sector index constituent universes

    It does NOT fetch getIndexList for every stock.
    """
    master = _build_master_universe(
        date=date,
        include_membership=False,
        max_symbols=None
    )

    requested_indices = [
        _normalize_index_name(item)
        for item in indices.split(",")
        if _normalize_index_name(item)
    ]

    unique_indices = []
    seen = set()

    for index_name in requested_indices:
        if index_name not in seen:
            seen.add(index_name)
            unique_indices.append(index_name)

    if len(unique_indices) > INDEX_UNION_MAX_INDICES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Requested {len(unique_indices)} indices. "
                f"Maximum allowed is {INDEX_UNION_MAX_INDICES}."
            )
        )

    index_union = []
    index_memberships = {}
    index_errors = {}
    index_results = {}

    if unique_indices:
        with ThreadPoolExecutor(
            max_workers=min(INDEX_CONSTITUENT_WORKERS, len(unique_indices))
        ) as executor:
            futures = {
                executor.submit(
                    _get_index_constituents_cached,
                    index_name
                ): index_name
                for index_name in unique_indices
            }

            for future in as_completed(futures):
                index_name = futures[future]

                try:
                    index_results[index_name] = future.result()
                except Exception as e:
                    index_results[index_name] = {
                        "count": 0,
                        "data": [],
                        "errors": [str(e)]
                    }

        seen_symbols = set(master["symbols"])
        discovery_symbols = list(master["symbols"])

        for index_name in unique_indices:
            result = index_results.get(index_name, {})
            data = result.get("data", [])

            if result.get("errors"):
                index_errors[index_name] = result["errors"]

            for row in data:
                if not isinstance(row, dict):
                    continue

                symbol = _normalize_symbol(row.get("symbol"))

                if not symbol or symbol == index_name:
                    continue

                index_memberships.setdefault(symbol, []).append(index_name)

                if symbol not in seen_symbols:
                    seen_symbols.add(symbol)
                    index_union.append(symbol)

        discovery_symbols.extend(index_union)
    else:
        discovery_symbols = list(master["symbols"])

    membership = {}

    if include_membership and membership_limit > 0:
        selected = discovery_symbols[:membership_limit]

        with ThreadPoolExecutor(
            max_workers=MASTER_MEMBERSHIP_WORKERS
        ) as executor:
            futures = {
                executor.submit(
                    _get_stock_index_membership_cached,
                    symbol
                ): symbol
                for symbol in selected
            }

            for future in as_completed(futures):
                symbol = futures[future]

                try:
                    membership[symbol] = future.result()
                except Exception as e:
                    membership[symbol] = {
                        "symbol": symbol,
                        "count": 0,
                        "indices": [],
                        "error": str(e)
                    }

    return {
        "source": "NSE",
        "requested_date": master.get("requested_date"),
        "effective_date": master.get("effective_date"),
        "date": master.get("effective_date"),
        "date_adjusted": master.get("date_adjusted"),
        "adjustment_reason": master.get("adjustment_reason"),
        "calendar_source": master.get("calendar_source"),
        "calendar_error": master.get("calendar_error"),
        "master_count": master["count"],
        "index_count": len(unique_indices),
        "indices_requested": unique_indices,
        "index_errors": index_errors,
        "discovery_count": len(discovery_symbols),
        "symbols": discovery_symbols,
        "index_memberships": index_memberships,
        "membership_included": bool(
            include_membership and membership_limit > 0
        ),
        "membership_limit": (
            membership_limit if include_membership else 0
        ),
        "membership": membership,
    }


@app.get("/nse/stock-index-membership")
def nse_stock_index_membership(
    symbol: str = Query(
        ...,
        description="NSE equity symbol, e.g. RELIANCE, TCS, NETWEB"
    )
):
    """
    Return all NSE indices to which a stock belongs.

    Uses NSE's current NextApi getIndexList function.
    """

    symbol_name = symbol.strip().upper()

    if not symbol_name:
        raise HTTPException(
            status_code=400,
            detail="A valid NSE stock symbol is required."
        )

    try:
        payload = nse_api_get(
            "/api/NextApi/apiClient/GetQuoteApi",
            params={
                "functionName": "getIndexList",
                "symbol": symbol_name
            }
        )

        if isinstance(payload, dict):
            data = payload.get("data", [])

            # Some NSE responses may use a different top-level
            # structure. Preserve the complete response.
            if not data:
                for key in ("indexList", "indices", "equityResponse"):
                    if isinstance(payload.get(key), list):
                        data = payload.get(key)
                        break
        else:
            data = payload

        if data is None:
            data = []

        if not isinstance(data, list):
            data = [data]

        return {
            "source": "NSE",
            "symbol": symbol_name,
            "endpoint": "/api/NextApi/apiClient/GetQuoteApi",
            "functionName": "getIndexList",
            "count": len(data),
            "indices": data,
            "raw": payload
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE index-membership data unavailable "
                f"for symbol '{symbol_name}'. "
                f"Underlying error: {str(e)}"
            )
        )



# ---------------------------------------------------------
# Historical price / corporate filing caches
# ---------------------------------------------------------
_HISTORICAL_CACHE = {}
_HISTORICAL_CACHE_LOCK = Lock()
_CORP_ACTIONS_CACHE = {}
_CORP_ACTIONS_CACHE_LOCK = Lock()
_CORP_ANNOUNCEMENTS_CACHE = {}
_CORP_ANNOUNCEMENTS_CACHE_LOCK = Lock()
_BOARD_MEETINGS_CACHE = {}
_BOARD_MEETINGS_CACHE_LOCK = Lock()
_FINANCIAL_RESULTS_CACHE = {}
_FINANCIAL_RESULTS_CACHE_LOCK = Lock()

_NSE_TRADING_HOLIDAYS_CACHE = None
_NSE_TRADING_HOLIDAYS_CACHE_LOCK = Lock()
_NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")


# ---------------------------------------------------------
# Generic helpers for NSE filing / historical endpoints
# ---------------------------------------------------------
def _parse_ddmmyyyy(value, field_name):
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Invalid {field_name}. Use DD-MM-YYYY, YYYY-MM-DD, or DD-Mon-YYYY."
    )


def _format_nse_date(value):
    return value.strftime("%d-%m-%Y") if value else None


def _unwrap_records(payload):
    """Normalize common NSE list/dict response shapes without losing raw data."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in (
            "data", "records", "result", "results", "corporateActions",
            "announcements", "boardMeetings", "financialResults",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                for nested_key in ("data", "records", "result", "results"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return nested
        # Some NSE responses are keyed by category/segment.
        collected = []
        for value in payload.values():
            if isinstance(value, list):
                collected.extend(value)
        if collected:
            return collected
    return []


def _validate_date_range(from_date, to_date, max_days=None):
    start = _parse_ddmmyyyy(from_date, "from_date")
    end = _parse_ddmmyyyy(to_date, "to_date")
    if start and end and start > end:
        raise ValueError("from_date cannot be later than to_date")
    if max_days and start and end and (end - start).days > max_days:
        raise ValueError(
            f"Requested date range exceeds the {max_days}-day API window. "
            "Use multiple non-overlapping requests."
        )
    return start, end


# ---------------------------------------------------------
# Historical equity price data
# ---------------------------------------------------------
def _clean_text(value):
    if value is None:
        return ""
    return str(value).replace("\ufeff", "").strip()


def _to_float(value):
    if value is None:
        return None
    raw = _clean_text(value).replace(",", "")
    if raw in {"", "-", "--", "NA", "N/A", "NULL", "NONE"}:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _parse_row_date(value):
    """Best-effort parser for NSE historical/bhavcopy date fields."""
    raw = _clean_text(value)
    if not raw:
        return None
    raw = raw.split("T", 1)[0].split(" ", 1)[0].strip()
    for fmt in (
        "%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%Y-%m-%d",
        "%Y/%m/%d", "%d/%m/%Y", "%d%m%Y",
    ):
        try:
            return datetime.strptime(raw.upper(), fmt).date()
        except ValueError:
            continue
    return None


def _historical_row_date_candidates(row):
    """
    Return all parseable date fields carried by a historical row.

    DATE1 is the exchange bhavcopy trade date. CH_TIMESTAMP is retained by
    NSE's historical JSON shape. If both exist they must agree.
    """
    if not isinstance(row, dict):
        return {}

    keys = (
        "DATE1", "date", "Date", "DATE", "TRADE_DATE", "TRADING_DATE",
        "CH_TIMESTAMP", "mTIMESTAMP", "TIMESTAMP",
    )
    candidates = {}
    for key in keys:
        if key in row:
            parsed = _parse_row_date(row.get(key))
            if parsed:
                candidates[key] = parsed
    return candidates


def _historical_row_date(row):
    candidates = _historical_row_date_candidates(row)
    if not candidates:
        return None

    # Prefer DATE1/trade-date fields when present; otherwise use the NSE
    # timestamp field. This prevents a shifted timestamp from silently
    # changing the observation's trading date.
    for key in ("DATE1", "date", "Date", "DATE", "TRADE_DATE", "TRADING_DATE"):
        if key in candidates:
            return candidates[key]
    return next(iter(candidates.values()))


def _historical_row_date_consistent(row):
    candidates = _historical_row_date_candidates(row)
    return len(set(candidates.values())) <= 1


def _historical_row_close(row):
    if not isinstance(row, dict):
        return None
    for key in (
        "CH_CLOSING_PRICE", "CLOSE_PRICE", "CLOSE", "close",
        "closingPrice", "lastPrice", "LAST_PRICE",
    ):
        value = _to_float(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _historical_row_symbol(row):
    if not isinstance(row, dict):
        return None
    for key in ("CH_SYMBOL", "SYMBOL", "symbol", "Symbol"):
        value = _normalize_symbol(row.get(key))
        if value:
            return value
    return None


def _historical_row_num(row, *keys):
    if not isinstance(row, dict):
        return None
    for key in keys:
        value = _to_float(row.get(key))
        if value is not None:
            return value
    return None


def _historical_row_quality_issues(row):
    """
    Validate internal OHLCV/delivery relationships without requiring every
    optional field to exist in every NSE response format.
    """
    issues = []

    open_price = _historical_row_num(
        row, "CH_OPENING_PRICE", "OPEN_PRICE", "OPEN", "open"
    )
    high = _historical_row_num(
        row, "CH_TRADE_HIGH_PRICE", "HIGH_PRICE", "HIGH", "high"
    )
    low = _historical_row_num(
        row, "CH_TRADE_LOW_PRICE", "LOW_PRICE", "LOW", "low"
    )
    close = _historical_row_close(row)
    volume = _historical_row_num(
        row, "CH_TOT_TRADED_QTY", "TTL_TRD_QNTY", "VOLUME", "volume"
    )
    delivery_qty = _historical_row_num(
        row, "CH_DELIV_QTY", "DELIV_QTY", "deliveryQuantity"
    )
    delivery_pct = _historical_row_num(
        row, "CH_DELIV_PER", "DELIV_PER", "deliveryPercent", "DELIVERY_PERCENT"
    )
    turnover = _historical_row_num(
        row, "CH_TOT_TRADED_VAL", "TURNOVER", "TURNOVER_LACS", "VALUE", "value"
    )

    for label, value in (
        ("open", open_price), ("high", high), ("low", low), ("close", close)
    ):
        if value is not None and value <= 0:
            issues.append(f"{label}_not_positive")

    if high is not None and low is not None and high < low:
        issues.append("high_below_low")

    if close is not None and high is not None and close > high + 1e-9:
        issues.append("close_above_high")

    if close is not None and low is not None and close < low - 1e-9:
        issues.append("close_below_low")

    if volume is not None and volume < 0:
        issues.append("negative_volume")

    if delivery_qty is not None and delivery_qty < 0:
        issues.append("negative_delivery_quantity")

    if volume is not None and delivery_qty is not None and delivery_qty > volume + 1e-9:
        issues.append("delivery_quantity_above_volume")

    if delivery_pct is not None and not 0 <= delivery_pct <= 100:
        issues.append("delivery_percent_out_of_range")

    if turnover is not None and turnover < 0:
        issues.append("negative_turnover")

    return issues


def _is_valid_historical_row(row, expected_symbol=None, start=None, end=None):
    """
    Reject HTML/error fragments and malformed records.

    A historical observation must contain a recognizable trade date and
    positive close. If multiple date fields exist they must agree. If the
    source supplies a symbol, it must match the requested symbol. The date
    must fall inside the requested window and OHLCV relationships must be
    internally coherent.
    """
    if not isinstance(row, dict) or not row:
        return False

    joined = " ".join(
        _clean_text(v).lower()
        for v in row.values()
        if isinstance(v, (str, int, float))
    )[:4000]

    if any(
        marker in joined
        for marker in (
            "service temporarily unavailable", "<html", "<!doctype",
            "access denied", "captcha", "cloudflare",
            "bad gateway", "internal server error",
        )
    ):
        return False

    row_date = _historical_row_date(row)
    close = _historical_row_close(row)

    if row_date is None or close is None:
        return False

    if not _historical_row_date_consistent(row):
        return False

    if start and row_date < start.date():
        return False
    if end and row_date > end.date():
        return False

    row_symbol = _historical_row_symbol(row)
    if expected_symbol and row_symbol and row_symbol != expected_symbol:
        return False

    if _historical_row_quality_issues(row):
        return False

    return True


def _validate_historical_rows(rows, expected_symbol=None, start=None, end=None):
    """
    Row-level validation only.

    IMPORTANT: do not deduplicate here. Dataset-level validation needs to see
    duplicate dates so that a broken source cannot hide them by overwriting
    one observation with another.
    """
    valid = []
    invalid = 0

    if not isinstance(rows, list):
        return [], 0

    for row in rows:
        if _is_valid_historical_row(
            row, expected_symbol=expected_symbol, start=start, end=end
        ):
            valid.append(row)
        else:
            invalid += 1

    valid.sort(key=lambda row: _historical_row_date(row).isoformat())
    return valid, invalid


def _get_nse_trading_holiday_dates():
    """
    Fetch NSE's official trading-holiday calendar.

    NSE exposes the trading holiday master through /api/holiday-master.
    CM is preferred for cash equities; FO is retained as a compatibility
    fallback because NSE's holiday response is segment-oriented.
    """
    global _NSE_TRADING_HOLIDAYS_CACHE

    with _NSE_TRADING_HOLIDAYS_CACHE_LOCK:
        if _NSE_TRADING_HOLIDAYS_CACHE is not None:
            return _NSE_TRADING_HOLIDAYS_CACHE

    try:
        payload = nse_api_get("/api/holiday-master", params={"type": "trading"})
        holiday_dates = set()

        if isinstance(payload, dict):
            segments = []
            for segment in ("CM", "FO"):
                value = payload.get(segment)
                if isinstance(value, list):
                    segments.extend(value)

            # Be tolerant of future NSE response-shape changes.
            if not segments:
                for value in payload.values():
                    if isinstance(value, list):
                        segments.extend(value)

            for record in segments:
                if not isinstance(record, dict):
                    continue
                for key in (
                    "tradingDate", "trading_date", "date", "DATE",
                    "holidayDate", "holiday_date",
                ):
                    parsed = _parse_row_date(record.get(key))
                    if parsed:
                        holiday_dates.add(parsed)
                        break

        result = {
            "dates": holiday_dates,
            "source": "NSE_holiday_master",
            "error": None,
        }
    except Exception as exc:
        # Do not make the historical endpoint unusable merely because the
        # holiday API is temporarily blocked. The caller will fall back to
        # weekday logic and explicitly report that limitation.
        result = {
            "dates": set(),
            "source": "weekday_fallback",
            "error": str(exc),
        }

    with _NSE_TRADING_HOLIDAYS_CACHE_LOCK:
        _NSE_TRADING_HOLIDAYS_CACHE = result

    return result

def _is_nse_trading_day(day, holidays):
    return day.weekday() < 5 and day not in holidays


def resolve_effective_eod_date(requested_date=None):
    """
    Resolve an EOD-data request to the latest completed NSE trading day.

    If no date is supplied, the resolver uses today's IST calendar date and
    then rolls back to the latest completed NSE trading day.

    Rules:
      - A past trading day is preserved.
      - A weekend/holiday rolls backward to the previous trading day.
      - Today is never treated as completed EOD; it rolls backward.
      - A future date also resolves to the latest completed trading day.

    Returns both requested and effective dates so callers never lose
    provenance.
    """
    today_ist = datetime.now(_NSE_TIMEZONE).date()
    raw = "" if requested_date is None else str(requested_date).strip()
    defaulted = not raw

    if defaulted:
        requested = today_ist
    else:
        try:
            requested = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Invalid date. Use YYYY-MM-DD format.")

    holiday_info = _get_nse_trading_holiday_dates()
    holidays = holiday_info.get("dates", set())

    candidate = requested
    reasons = []

    if defaulted:
        reasons.append("date not supplied; using current IST date")

    if candidate >= today_ist:
        candidate = today_ist - timedelta(days=1)
        reasons.append("current/future date has no completed EOD")

    while not _is_nse_trading_day(candidate, holidays):
        if candidate.weekday() >= 5:
            reasons.append("weekend")
        elif candidate in holidays:
            reasons.append("NSE trading holiday")
        candidate -= timedelta(days=1)

    adjusted = candidate != requested

    if not adjusted and not reasons:
        reason = "requested date is a completed NSE trading day"
    elif reasons:
        reason = "; ".join(dict.fromkeys(reasons))
    else:
        reason = "rolled back to latest completed NSE trading day"

    return {
        "requested_date": requested.isoformat(),
        "effective_date": candidate.isoformat(),
        "date_adjusted": adjusted,
        "adjustment_reason": reason,
        "calendar_source": holiday_info.get("source"),
        "calendar_error": holiday_info.get("error"),
        "today_ist": today_ist.isoformat(),
    }


@app.get("/nse/trading-date")
def nse_trading_date(
    date: str | None = Query(
        None,
        description="Optional requested EOD date in YYYY-MM-DD format; defaults to today and resolves to the latest completed NSE trading day"
    )
):
    """Diagnostic endpoint for the central NSE effective-EOD date resolver."""
    try:
        return resolve_effective_eod_date(date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))



def _historical_expected_trading_dates(start, end):
    """
    Return weekday dates excluding NSE's official trading holidays.

    If the holiday master cannot be retrieved, weekdays are used as a
    conservative fallback and the caller reports the holiday-data error.
    """
    if start is None or end is None:
        return set(), {"source": "unavailable", "error": "missing date range"}

    holiday_info = _get_nse_trading_holiday_dates()
    holidays = holiday_info.get("dates", set())

    current = start.date()
    finish = end.date()
    dates = set()

    while current <= finish:
        if current.weekday() < 5 and current not in holidays:
            dates.add(current)
        current += timedelta(days=1)

    return dates, {
        "source": holiday_info.get("source"),
        "error": holiday_info.get("error"),
        "holiday_count_in_range": sum(
            1
            for day in holidays
            if start.date() <= day <= finish
        ),
    }


def _historical_integrity_report(
    rows, expected_symbol=None, start=None, end=None
):
    """
    Dataset-level validation that cannot be performed safely on a single row.

    Detects:
      - duplicate trade dates
      - inconsistent DATE1/CH_TIMESTAMP dates
      - weekend/holiday observations
      - unexpected out-of-range dates
      - missing expected trading dates
      - OHLCV relationship problems
    """
    if not isinstance(rows, list):
        rows = []

    date_counts = {}
    invalid_row_count = 0
    inconsistent_date_count = 0
    quality_issue_count = 0
    quality_issue_examples = []

    for row in rows:
        if not _is_valid_historical_row(
            row, expected_symbol=expected_symbol, start=start, end=end
        ):
            invalid_row_count += 1

        row_date = _historical_row_date(row)
        if row_date:
            key = row_date.isoformat()
            date_counts[key] = date_counts.get(key, 0) + 1

        if not _historical_row_date_consistent(row):
            inconsistent_date_count += 1

        issues = _historical_row_quality_issues(row)
        if issues:
            quality_issue_count += 1
            if len(quality_issue_examples) < 20:
                quality_issue_examples.append({
                    "date": row_date.isoformat() if row_date else None,
                    "issues": issues,
                })

    observed_dates = set()
    for key in date_counts:
        try:
            observed_dates.add(datetime.strptime(key, "%Y-%m-%d").date())
        except ValueError:
            pass

    duplicate_dates = sorted(
        key for key, count in date_counts.items() if count > 1
    )

    expected_dates, expected_meta = _historical_expected_trading_dates(start, end)

    unexpected_dates = sorted(
        day.isoformat()
        for day in observed_dates
        if day not in expected_dates
    )
    missing_dates = sorted(
        day.isoformat()
        for day in expected_dates
        if day not in observed_dates
    )

    weekend_dates = sorted(
        day.isoformat()
        for day in observed_dates
        if day.weekday() >= 5
    )

    holiday_dates = sorted(
        day.isoformat()
        for day in observed_dates
        if day in _get_nse_trading_holiday_dates().get("dates", set())
    )

    integrity_ok = (
        len(duplicate_dates) == 0
        and inconsistent_date_count == 0
        and invalid_row_count == 0
        and quality_issue_count == 0
        and len(weekend_dates) == 0
        and len(holiday_dates) == 0
        and len(unexpected_dates) == 0
    )

    expected_count = len(expected_dates)

    return {
        "date_integrity": "VALID" if integrity_ok else "INVALID",
        "raw_row_count": len(rows),
        "unique_date_count": len(observed_dates),
        "duplicate_date_count": len(duplicate_dates),
        "duplicate_dates": duplicate_dates[:50],
        "inconsistent_date_count": inconsistent_date_count,
        "invalid_row_count": invalid_row_count,
        "quality_issue_count": quality_issue_count,
        "quality_issue_examples": quality_issue_examples,
        "weekend_date_count": len(weekend_dates),
        "weekend_dates": weekend_dates[:50],
        "holiday_date_count": len(holiday_dates),
        "holiday_dates": holiday_dates[:50],
        "expected_trading_days": expected_count,
        "missing_expected_dates_count": len(missing_dates),
        "missing_expected_dates": missing_dates[:50],
        "unexpected_date_count": len(unexpected_dates),
        "unexpected_dates": unexpected_dates[:50],
        "coverage_pct": (
            (len(observed_dates) / expected_count) * 100.0
            if expected_count else None
        ),
        "holiday_calendar_source": expected_meta.get("source"),
        "holiday_calendar_error": expected_meta.get("error"),
        "holiday_count_in_range": expected_meta.get("holiday_count_in_range", 0),
    }


def _historical_expected_weekdays(start, end):
    if start is None or end is None:
        return None
    current = start.date()
    finish = end.date()
    count = 0
    while current <= finish:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count


def _get_historical_equity_chunk(params):
    """
    Primary historical route with strict transport validation.

    NSE's legacy ``/api/historical/cm/equity`` route can return an HTML
    404/error page. Feeding that page to ``csv.DictReader`` creates fake
    dictionaries, which caused the misleading "100 rows / 0 valid market
    rows" diagnostic. Only genuine JSON or recognizable market CSV is now
    accepted here.
    """
    path = "/api/historical/cm/equity"
    last_errors = []

    for attempt in range(3):
        session = get_nse_session(force_refresh=(attempt > 0))
        headers = {
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": f"{NSE_BASE}/get-quotes/equity?symbol={params.get('symbol')}",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }

        try:
            response = session.get(
                NSE_BASE + path, params=params, headers=headers, timeout=40
            )
            text_body = response.text.lstrip("\ufeff \r\n\t")

            if response.status_code in {401, 403, 429} or response.status_code >= 500:
                last_errors.append(
                    f"HTTP {response.status_code}: {text_body[:300]}"
                )
                with _NSE_SESSION_LOCK:
                    global _nse_session
                    _nse_session = None
                time.sleep(1.0 + attempt)
                continue

            # IMPORTANT: a 404/HTML page is not CSV. Do not let
            # csv.DictReader turn an NSE error page into fake rows.
            if response.status_code == 404:
                last_errors.append(
                    "HTTP 404: NSE historical equity endpoint is unavailable "
                    f"for this request; body={text_body[:300]!r}"
                )
                break

            lower_body = text_body[:2000].lower()
            content_type = (response.headers.get("content-type") or "").lower()
            if (
                "text/html" in content_type
                or lower_body.startswith("<html")
                or lower_body.startswith("<!doctype")
                or "resource not found" in lower_body
                or "service temporarily unavailable" in lower_body
                or "access denied" in lower_body
                or "captcha" in lower_body
            ):
                last_errors.append(
                    "NSE historical endpoint returned an HTML/error response: "
                    f"status={response.status_code}, content-type={content_type}, "
                    f"body={text_body[:300]!r}"
                )
                break

            response.raise_for_status()

            if text_body.startswith("{") or text_body.startswith("["):
                try:
                    payload = response.json()
                    rows = _unwrap_records(payload)
                    if rows:
                        return {"rows": rows, "transport": "json"}
                    if isinstance(payload, dict) and "data" in payload:
                        return {"rows": [], "transport": "json"}
                except ValueError as exc:
                    last_errors.append(f"JSON parse failed: {exc}")

            csv_params = dict(params)
            csv_params["csv"] = "true"
            csv_headers = dict(headers)
            csv_headers["Accept"] = "text/csv,text/plain,*/*"

            csv_response = session.get(
                NSE_BASE + path,
                params=csv_params,
                headers=csv_headers,
                timeout=40,
            )
            csv_text = csv_response.content.decode(
                "utf-8-sig", errors="replace"
            )

            if csv_response.ok and csv_text.strip():
                csv_probe = csv_text.lstrip().lower()[:2000]
                csv_content_type = (
                    csv_response.headers.get("content-type") or ""
                ).lower()
                csv_is_html = (
                    "text/html" in csv_content_type
                    or csv_probe.startswith("<html")
                    or csv_probe.startswith("<!doctype")
                    or "resource not found" in csv_probe
                    or "service temporarily unavailable" in csv_probe
                    or "access denied" in csv_probe
                    or "captcha" in csv_probe
                )
                lines = csv_text.lstrip().splitlines()
                first_line = lines[0] if lines else ""
                header_upper = first_line.upper()
                looks_like_market_csv = any(
                    token in header_upper
                    for token in (
                        "DATE", "SYMBOL", "SERIES", "CLOSE",
                        "CH_CLOSING_PRICE", "OPEN", "HIGH", "LOW",
                    )
                )

                if not csv_is_html and looks_like_market_csv:
                    reader = csv.DictReader(io.StringIO(csv_text))
                    rows = [dict(row) for row in reader]
                    if rows:
                        return {"rows": rows, "transport": "csv"}

            last_errors.append(
                f"Non-usable historical response: "
                f"status={response.status_code}, "
                f"content-type={response.headers.get('content-type')}, "
                f"body={text_body[:200]!r}"
            )
        except Exception as exc:
            last_errors.append(str(exc))
            with _NSE_SESSION_LOCK:
                _nse_session = None
            time.sleep(1.0 + attempt)

    raise RuntimeError(" | ".join(last_errors))


def _get_nse_full_bhavcopy_for_date(session, trading_date, symbol):
    """
    Free NSE Full Bhavcopy + Security Deliverable fallback.

    Current NSE reports expose:
        sec_bhavdata_full_DDMMYYYY.csv
    """
    date_token = trading_date.strftime("%d%m%Y")
    url = (
        "https://nsearchives.nseindia.com/products/content/"
        f"sec_bhavdata_full_{date_token}.csv"
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,text/plain,*/*",
        "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": NSE_BASE + "/all-reports",
        "Connection": "keep-alive",
    }

    try:
        response = session.get(url, headers=headers, timeout=40)

        if response.status_code == 404:
            return {"status": "not_available", "date": trading_date.isoformat()}

        response.raise_for_status()
        content = response.content.decode("utf-8-sig", errors="replace")

        if not content.strip():
            return {"status": "empty", "date": trading_date.isoformat()}

        probe = content.lstrip().lower()[:1000]
        if (
            probe.startswith("<html")
            or probe.startswith("<!doctype")
            or "service temporarily unavailable" in probe
            or "access denied" in probe
            or "captcha" in probe
        ):
            return {
                "status": "invalid_response",
                "date": trading_date.isoformat(),
            }

        # NSE's legacy/full-bhavcopy CSV can contain whitespace/BOM in
        # column names (for example ``SYMBOL `` / `` CLOSE_PRICE``).
        # Normalize the header before looking up fields; otherwise a real
        # NETWEB/RELIANCE row can be incorrectly classified as invalid_row.
        reader = csv.DictReader(io.StringIO(content))
        target = _normalize_symbol(symbol)
        wanted = None

        for raw_row in reader:
            if not isinstance(raw_row, dict):
                continue

            row = {}
            for key, value in raw_row.items():
                normalized_key = _clean_text(key).upper()
                normalized_key = normalized_key.replace("\ufeff", "")
                if normalized_key:
                    row[normalized_key] = _clean_text(value)

            if _historical_row_symbol(row) == target:
                wanted = row
                break

        if wanted is None:
            return {
                "status": "symbol_not_found",
                "date": trading_date.isoformat(),
            }

        close = _to_float(
            wanted.get("CLOSE_PRICE")
            or wanted.get("CH_CLOSING_PRICE")
            or wanted.get("CLOSE")
        )
        if close is None or close <= 0:
            return {
                "status": "invalid_row",
                "date": trading_date.isoformat(),
                "error": "Target symbol row found but no positive CLOSE_PRICE/CLOSE field was available after header normalization.",
                "columns": sorted(row.keys())[:80],
            }

        turnover_lacs = _to_float(wanted.get("TURNOVER_LACS"))

        normalized = dict(wanted)
        normalized.update({
            "CH_SYMBOL": target,
            "CH_TIMESTAMP": trading_date.strftime("%d-%b-%Y"),
            "CH_OPENING_PRICE": (
                wanted.get("OPEN_PRICE")
                or wanted.get("CH_OPENING_PRICE")
            ),
            "CH_TRADE_HIGH_PRICE": (
                wanted.get("HIGH_PRICE")
                or wanted.get("CH_TRADE_HIGH_PRICE")
            ),
            "CH_TRADE_LOW_PRICE": (
                wanted.get("LOW_PRICE")
                or wanted.get("CH_TRADE_LOW_PRICE")
            ),
            "CH_CLOSING_PRICE": (
                wanted.get("CLOSE_PRICE")
                or wanted.get("CH_CLOSING_PRICE")
            ),
            "CH_TOT_TRADED_QTY": (
                wanted.get("TTL_TRD_QNTY")
                or wanted.get("CH_TOT_TRADED_QTY")
            ),
            "CH_TOT_TRADED_VAL": (
                turnover_lacs * 100000
                if turnover_lacs is not None
                else wanted.get("CH_TOT_TRADED_VAL")
            ),
            "CH_TOTAL_TRADES": (
                wanted.get("NO_OF_TRADES")
                or wanted.get("CH_TOTAL_TRADES")
            ),
            "CH_DELIV_QTY": (
                wanted.get("DELIV_QTY")
                or wanted.get("CH_DELIV_QTY")
            ),
            "CH_DELIV_PER": (
                wanted.get("DELIV_PER")
                or wanted.get("CH_DELIV_PER")
            ),
        })

        return {
            "status": "valid",
            "date": trading_date.isoformat(),
            "row": normalized,
        }

    except requests.RequestException as exc:
        return {
            "status": "request_error",
            "date": trading_date.isoformat(),
            "error": str(exc),
        }
    except Exception as exc:
        return {
            "status": "parse_error",
            "date": trading_date.isoformat(),
            "error": str(exc),
        }


def _get_historical_equity_bhavcopy_fallback(
    symbol, start, end, series="EQ"
):
    """
    Build validated history from NSE's free daily Full Bhavcopy +
    Security Deliverable archive.

    Weekends and official NSE trading holidays are skipped. Dataset-level
    validation is retained in the response so a successful HTTP request
    cannot be mistaken for a clean market-data dataset.
    """
    target = _normalize_symbol(symbol)
    if not target:
        raise ValueError("A valid NSE equity symbol is required.")

    session = get_nse_session()
    rows = []
    errors = []
    unavailable_dates = []
    invalid_dates = []

    expected_dates, holiday_meta = _historical_expected_trading_dates(start, end)

    current = start.date()
    finish = end.date()

    while current <= finish:
        if current in expected_dates:
            result = _get_nse_full_bhavcopy_for_date(
                session=session,
                trading_date=current,
                symbol=target,
            )
            status = result.get("status")

            if status == "valid":
                row = result.get("row")
                if _is_valid_historical_row(
                    row,
                    expected_symbol=target,
                    start=start,
                    end=end,
                ):
                    rows.append(row)
                else:
                    invalid_dates.append(current.isoformat())
            elif status in {
                "not_available", "empty", "symbol_not_found"
            }:
                unavailable_dates.append(current.isoformat())
            else:
                errors.append({
                    "date": current.isoformat(),
                    "status": status,
                    "error": result.get("error"),
                    **(
                        {"columns": result.get("columns")}
                        if result.get("columns") else {}
                    ),
                })

            # Conservative archive rate to reduce NSE throttling risk.
            time.sleep(0.20)

        current += timedelta(days=1)

    # Do not silently overwrite duplicates before the integrity report.
    integrity = _historical_integrity_report(
        rows,
        expected_symbol=target,
        start=start,
        end=end,
    )

    dedup = {}
    for row in rows:
        row_date = _historical_row_date(row)
        if row_date:
            dedup[row_date.isoformat()] = row

    rows = list(dedup.values())
    rows.sort(key=lambda row: _historical_row_date(row).isoformat())

    expected_trading_days = len(expected_dates)
    observed = len(rows)

    return {
        "source": "NSE_Bhavcopy_Fallback",
        "symbol": target,
        "series": str(series or "EQ").strip().upper(),
        "from_date": _format_nse_date(start),
        "to_date": _format_nse_date(end),
        "count": observed,
        "data": rows,
        "fallback_used": True,
        "data_quality": (
            "VALID"
            if observed and integrity.get("date_integrity") == "VALID"
            else ("UNAVAILABLE" if not observed else "INVALID")
        ),
        "expected_weekdays": _historical_expected_weekdays(start, end),
        "expected_trading_days": expected_trading_days,
        "valid_observations": observed,
        "unavailable_dates_count": len(unavailable_dates),
        "invalid_dates_count": len(invalid_dates),
        "fallback_error_count": len(errors),
        "unavailable_dates": unavailable_dates[:50],
        "invalid_dates": invalid_dates[:50],
        "fallback_errors": errors[:50],
        "holiday_calendar_source": holiday_meta.get("source"),
        "holiday_calendar_error": holiday_meta.get("error"),
        "holiday_count_in_range": holiday_meta.get("holiday_count_in_range", 0),
        "coverage_pct": (
            (observed / expected_trading_days) * 100.0
            if expected_trading_days else None
        ),
        "integrity": integrity,
    }


def _get_historical_equity(symbol, from_date, to_date, series="EQ"):
    symbol_name = _normalize_symbol(symbol)
    if not symbol_name:
        raise ValueError("A valid NSE equity symbol is required.")

    start = _parse_ddmmyyyy(from_date, "from_date")
    requested_end = _parse_ddmmyyyy(to_date, "to_date")

    # Resolve every historical EOD request through the central NSE trading-day
    # resolver. This prevents today/future/weekend/holiday dates from entering
    # the historical range and being incorrectly counted as expected EOD rows.
    requested_end_date = (
        requested_end.date()
        if requested_end is not None
        else datetime.now(_NSE_TIMEZONE).date()
    )
    end_info = resolve_effective_eod_date(requested_end_date.isoformat())
    end = datetime.strptime(
        end_info["effective_date"], "%Y-%m-%d"
    )

    if start is None:
        start = end - timedelta(days=30)
    if start > end:
        raise ValueError(
            "from_date cannot be later than the effective NSE EOD date "
            f"({end_info['effective_date']})"
        )

    series_name = str(series or "EQ").strip().upper()
    cache_key = (
        symbol_name,
        _format_nse_date(start),
        _format_nse_date(end),
        series_name,
    )

    with _HISTORICAL_CACHE_LOCK:
        cached = _HISTORICAL_CACHE.get(cache_key)
    if cached is not None:
        result = dict(cached)
        result.update({
            "requested_to_date": end_info["requested_date"],
            "effective_to_date": end_info["effective_date"],
            "date_adjusted": end_info["date_adjusted"],
            "adjustment_reason": end_info["adjustment_reason"],
            "calendar_source": end_info.get("calendar_source"),
            "calendar_error": end_info.get("calendar_error"),
            "today_ist": end_info.get("today_ist"),
        })
        return result

    chunks = []
    primary_errors = []
    invalid_primary_rows = 0
    primary_transport = []

    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=360), end)
        params = {
            "symbol": symbol_name,
            "series": json.dumps([series_name], separators=(",", ":")),
            "from": _format_nse_date(cursor),
            "to": _format_nse_date(chunk_end),
        }

        try:
            response = _get_historical_equity_chunk(params)
            raw_rows = response.get("rows", [])
            if response.get("transport"):
                primary_transport.append(response["transport"])

            valid_rows, invalid_count = _validate_historical_rows(
                raw_rows,
                expected_symbol=symbol_name,
                start=cursor,
                end=chunk_end,
            )
            invalid_primary_rows += invalid_count

            if raw_rows and not valid_rows:
                primary_errors.append(
                    f"{_format_nse_date(cursor)} to "
                    f"{_format_nse_date(chunk_end)}: "
                    f"received {len(raw_rows)} rows but 0 valid market rows"
                )
                chunks = []
                break

            chunks.extend(valid_rows)
        except Exception as exc:
            primary_errors.append(
                f"{_format_nse_date(cursor)} to "
                f"{_format_nse_date(chunk_end)}: {str(exc)}"
            )
            chunks = []
            break

        cursor = chunk_end + timedelta(days=1)

    # Do not run the dataset-integrity validator on an unavailable primary
    # source and then label an empty dataset as date_integrity=VALID.
    # An empty dataset can be internally consistent but still means that the
    # primary source supplied no market observations.
    if primary_errors and not chunks:
        primary_integrity = {
            "date_integrity": "UNAVAILABLE",
            "status": "UNAVAILABLE",
            "raw_row_count": 0,
            "unique_date_count": 0,
            "duplicate_date_count": 0,
            "duplicate_dates": [],
            "inconsistent_date_count": 0,
            "invalid_row_count": invalid_primary_rows,
            "quality_issue_count": 0,
            "quality_issue_examples": [],
            "weekend_date_count": 0,
            "weekend_dates": [],
            "holiday_date_count": 0,
            "holiday_dates": [],
            "expected_trading_days": None,
            "missing_expected_dates_count": 0,
            "missing_expected_dates": [],
            "unexpected_date_count": 0,
            "unexpected_dates": [],
            "coverage_pct": None,
            "holiday_calendar_source": None,
            "holiday_calendar_error": None,
            "holiday_count_in_range": 0,
        }
    else:
        primary_integrity = _historical_integrity_report(
            chunks,
            expected_symbol=symbol_name,
            start=start,
            end=end,
        )

    dedup = {}
    for row in chunks:
        row_date = _historical_row_date(row)
        if row_date:
            dedup[row_date.isoformat()] = row

    primary_rows = list(dedup.values())
    primary_rows.sort(
        key=lambda row: _historical_row_date(row).isoformat()
    )

    expected_weekdays = _historical_expected_weekdays(start, end)
    expected_trading_days = primary_integrity.get("expected_trading_days")
    primary_valid = bool(primary_rows)
    primary_complete_enough = (
        primary_valid
        and expected_trading_days is not None
        and len(primary_rows) >= max(1, int(expected_trading_days * 0.80))
    )

    primary_integrity_ok = (
        primary_integrity.get("date_integrity") == "VALID"
    )

    if (
        primary_valid
        and primary_complete_enough
        and not primary_errors
        and primary_integrity_ok
    ):
        result = {
            "source": "NSE",
            "symbol": symbol_name,
            "series": series_name,
            "from_date": _format_nse_date(start),
            "to_date": _format_nse_date(end),
            "count": len(primary_rows),
            "endpoint": "/api/historical/cm/equity",
            "data": primary_rows,
            "fallback_used": False,
            "data_quality": "VALID",
            "expected_weekdays": expected_weekdays,
            "expected_trading_days": expected_trading_days,
            "valid_observations": len(primary_rows),
            "invalid_primary_rows": invalid_primary_rows,
            "fallback_error_count": 0,
            "primary_errors": [],
            "primary_transport": sorted(set(primary_transport)),
            "coverage_pct": primary_integrity.get("coverage_pct"),
            "integrity": primary_integrity,
        }
        with _HISTORICAL_CACHE_LOCK:
            _HISTORICAL_CACHE[cache_key] = dict(result)

        result.update({
            "requested_to_date": end_info["requested_date"],
            "effective_to_date": end_info["effective_date"],
            "date_adjusted": end_info["date_adjusted"],
            "adjustment_reason": end_info["adjustment_reason"],
            "calendar_source": end_info.get("calendar_source"),
            "calendar_error": end_info.get("calendar_error"),
            "today_ist": end_info.get("today_ist"),
        })
        return result

    fallback = _get_historical_equity_bhavcopy_fallback(
        symbol=symbol_name,
        start=start,
        end=end,
        series=series_name,
    )
    fallback["primary_errors"] = primary_errors[:50]
    fallback["invalid_primary_rows"] = invalid_primary_rows
    fallback["primary_transport"] = sorted(set(primary_transport))
    fallback["primary_integrity"] = primary_integrity
    fallback["primary_source_status"] = (
        "UNAVAILABLE"
        if primary_errors and not primary_rows
        else ("PARTIAL" if primary_errors else "VALID")
    )

    observed = fallback.get("count", 0)
    fallback_integrity = fallback.get("integrity", {})

    if observed:
        fallback["data_quality"] = (
            "VALID"
            if fallback_integrity.get("date_integrity") == "VALID"
            else "INVALID"
        )
    else:
        fallback["data_quality"] = "UNAVAILABLE"

    fallback["endpoint"] = (
        "https://nsearchives.nseindia.com/products/content/"
        "sec_bhavdata_full_DDMMYYYY.csv"
    )

    with _HISTORICAL_CACHE_LOCK:
        _HISTORICAL_CACHE[cache_key] = dict(fallback)

    fallback.update({
        "requested_to_date": end_info["requested_date"],
        "effective_to_date": end_info["effective_date"],
        "date_adjusted": end_info["date_adjusted"],
        "adjustment_reason": end_info["adjustment_reason"],
        "calendar_source": end_info.get("calendar_source"),
        "calendar_error": end_info.get("calendar_error"),
        "today_ist": end_info.get("today_ist"),
    })

    return fallback


@app.get("/nse/historical-equity")
def nse_historical_equity(
    symbol: str = Query(
        ..., description="NSE equity symbol, e.g. NETWEB, DIXON, KAYNES"
    ),
    from_date: str | None = Query(
        None, description="Start date: DD-MM-YYYY or YYYY-MM-DD"
    ),
    to_date: str | None = Query(
        None, description="End date: DD-MM-YYYY or YYYY-MM-DD"
    ),
    series: str = Query("EQ", description="NSE equity series, normally EQ"),
):
    """
    Return validated daily historical OHLCV/turnover/delivery data.

    Primary: NSE historical equity endpoint.
    Fallback: NSE free daily Full Bhavcopy + Security Deliverable archive.
    HTML/error fragments are never accepted as market observations.
    """
    try:
        return _get_historical_equity(
            symbol, from_date, to_date, series
        )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE historical equity data unavailable for "
                f"'{symbol}'. Underlying error: {str(e)}"
            ),
        )


@app.get("/nse/historical-validation")
def nse_historical_validation(
    symbol: str = Query(..., description="NSE equity symbol"),
    from_date: str | None = Query(None, description="Optional start date"),
    to_date: str | None = Query(None, description="Optional end date"),
    series: str = Query("EQ", description="NSE equity series"),
):
    """
    Return only the validation/diagnostic layer for a historical request.

    This endpoint is intended for hand-validation of source integrity before
    the data is used by screening or investment logic.
    """
    try:
        result = _get_historical_equity(
            symbol, from_date, to_date, series
        )
        return {
            "source": result.get("source"),
            "symbol": result.get("symbol"),
            "series": result.get("series"),
            "from_date": result.get("from_date"),
            "to_date": result.get("to_date"),
            "requested_to_date": result.get("requested_to_date"),
            "effective_to_date": result.get("effective_to_date"),
            "date_adjusted": result.get("date_adjusted"),
            "adjustment_reason": result.get("adjustment_reason"),
            "calendar_source": result.get("calendar_source"),
            "calendar_error": result.get("calendar_error"),
            "today_ist": result.get("today_ist"),
            "count": result.get("count"),
            "data_quality": result.get("data_quality"),
            "fallback_used": result.get("fallback_used"),
            "expected_weekdays": result.get("expected_weekdays"),
            "expected_trading_days": result.get("expected_trading_days"),
            "valid_observations": result.get("valid_observations"),
            "coverage_pct": result.get("coverage_pct"),
            "holiday_calendar_source": result.get("holiday_calendar_source"),
            "holiday_calendar_error": result.get("holiday_calendar_error"),
            "holiday_count_in_range": result.get("holiday_count_in_range"),
            "integrity": result.get("integrity"),
            "primary_integrity": result.get("primary_integrity"),
            "primary_integrity_status": (
                (result.get("primary_integrity") or {}).get("status")
                or (result.get("primary_integrity") or {}).get("date_integrity")
            ),
            "primary_errors": result.get("primary_errors", []),
            "primary_source_status": result.get("primary_source_status"),
            "invalid_primary_rows": result.get("invalid_primary_rows", 0),
            "fallback_errors": result.get("fallback_errors", []),
            "unavailable_dates_count": result.get("unavailable_dates_count", 0),
            "invalid_dates_count": result.get("invalid_dates_count", 0),
        }
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE historical validation unavailable for "
                f"'{symbol}'. Underlying error: {str(e)}"
            ),
        )


@app.get("/nse/historical-summary")
def nse_historical_summary(
    symbol: str = Query(..., description="NSE equity symbol"),
    from_date: str | None = Query(None, description="Optional start date"),
    to_date: str | None = Query(None, description="Optional end date"),
    series: str = Query("EQ", description="NSE equity series"),
):
    """Return validated historical data plus research-ready statistics."""
    try:
        result = _get_historical_equity(
            symbol, from_date, to_date, series
        )
        rows = result.get("data", [])

        def num(row, *keys):
            for key in keys:
                value = row.get(key) if isinstance(row, dict) else None
                parsed = _to_float(value)
                if parsed is not None:
                    return parsed
            return None

        closes = []
        intraday_highs = []
        intraday_lows = []
        volumes = []
        values = []
        trades = []
        deliveries = []

        for row in rows:
            close = num(
                row, "CH_CLOSING_PRICE", "CLOSE_PRICE", "close", "CLOSE"
            )
            if close is not None and close > 0:
                closes.append(close)

            high = num(
                row, "CH_TRADE_HIGH_PRICE", "HIGH_PRICE", "HIGH", "high"
            )
            if high is not None and high > 0:
                intraday_highs.append(high)

            low = num(
                row, "CH_TRADE_LOW_PRICE", "LOW_PRICE", "LOW", "low"
            )
            if low is not None and low > 0:
                intraday_lows.append(low)

            volume = num(
                row, "CH_TOT_TRADED_QTY", "TTL_TRD_QNTY",
                "volume", "VOLUME"
            )
            if volume is not None:
                volumes.append(volume)

            value = num(
                row, "CH_TOT_TRADED_VAL", "TURNOVER",
                "TURNOVER_LACS", "value", "VALUE"
            )
            if value is not None:
                if (
                    "TURNOVER_LACS" in row
                    and "CH_TOT_TRADED_VAL" not in row
                    and "TURNOVER" not in row
                ):
                    value *= 100000
                values.append(value)

            trade_count = num(
                row, "CH_TOTAL_TRADES", "NO_OF_TRADES",
                "No of trades", "NO OF TRADES"
            )
            if trade_count is not None:
                trades.append(trade_count)

            delivery_pct = num(
                row, "CH_DELIV_PER", "DELIV_PER",
                "deliveryPercent", "DELIVERY_PERCENT"
            )
            if delivery_pct is not None and 0 <= delivery_pct <= 100:
                deliveries.append(delivery_pct)

        returns = []
        for i in range(1, len(closes)):
            if closes[i - 1]:
                returns.append(
                    (closes[i] / closes[i - 1] - 1.0) * 100.0
                )

        period_return = None
        if len(closes) >= 2 and closes[0]:
            period_return = (
                (closes[-1] / closes[0] - 1.0) * 100.0
            )

        annualized_vol = None
        if len(returns) >= 2:
            mean = sum(returns) / len(returns)
            variance = (
                sum((x - mean) ** 2 for x in returns)
                / (len(returns) - 1)
            )
            annualized_vol = (variance ** 0.5) * (252 ** 0.5)

        running_high = None
        max_drawdown = 0.0
        for close in closes:
            running_high = (
                close if running_high is None
                else max(running_high, close)
            )
            if running_high:
                max_drawdown = min(
                    max_drawdown,
                    (close / running_high - 1.0) * 100.0,
                )

        return {
            **result,
            "summary": {
                "period_return_pct": period_return,
                "annualized_volatility_pct": annualized_vol,
                "max_drawdown_pct": max_drawdown,
                "period_high_close": max(closes) if closes else None,
                "period_low_close": min(closes) if closes else None,
                "period_intraday_high": (
                    max(intraday_highs) if intraday_highs else None
                ),
                "period_intraday_low": (
                    min(intraday_lows) if intraday_lows else None
                ),
                # Backward-compatible aliases: these now explicitly mean
                # closing-price extremes, not intraday extremes.
                "period_high": max(closes) if closes else None,
                "period_low": min(closes) if closes else None,
                "avg_daily_volume": (
                    sum(volumes) / len(volumes) if volumes else None
                ),
                "avg_daily_traded_value": (
                    sum(values) / len(values) if values else None
                ),
                "avg_daily_trades": (
                    sum(trades) / len(trades) if trades else None
                ),
                "avg_delivery_pct": (
                    sum(deliveries) / len(deliveries)
                    if deliveries else None
                ),
                "valid_close_observations": len(closes),
                "valid_volume_observations": len(volumes),
                "valid_delivery_observations": len(deliveries),
            },
        }
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE historical summary unavailable for "
                f"'{symbol}'. Underlying error: {str(e)}"
            ),
        )


# ---------------------------------------------------------
# Corporate actions / announcements / board meetings / results
# ---------------------------------------------------------
def _filing_params(symbol=None, from_date=None, to_date=None, index="equities"):
    params = {"index": index}
    if symbol:
        params["symbol"] = _normalize_symbol(symbol)
    if from_date:
        start = _parse_ddmmyyyy(from_date, "from_date")
        params["from_date"] = _format_nse_date(start)
    if to_date:
        end = _parse_ddmmyyyy(to_date, "to_date")
        params["to_date"] = _format_nse_date(end)
    if from_date and to_date:
        start = _parse_ddmmyyyy(from_date, "from_date")
        end = _parse_ddmmyyyy(to_date, "to_date")
        if start > end:
            raise ValueError("from_date cannot be later than to_date")
    return params


def _get_corporate_actions(symbol=None, from_date=None, to_date=None, index="equities"):
    key = (str(symbol or "").upper(), from_date, to_date, index)
    with _CORP_ACTIONS_CACHE_LOCK:
        if key in _CORP_ACTIONS_CACHE:
            return _CORP_ACTIONS_CACHE[key]

    params = _filing_params(symbol, from_date, to_date, index)
    errors = []
    for path in ("/api/corporates-corporateActions", "/api/corporate-actions"):
        try:
            payload = nse_api_get(path, params=params)
            result = {
                "source": "NSE",
                "symbol": _normalize_symbol(symbol) if symbol else None,
                "endpoint": path,
                "count": len(_unwrap_records(payload)),
                "data": _unwrap_records(payload),
                "raw": payload,
            }
            with _CORP_ACTIONS_CACHE_LOCK:
                _CORP_ACTIONS_CACHE[key] = result
            return result
        except Exception as e:
            errors.append(f"{path}: {str(e)}")
    raise RuntimeError("Corporate actions unavailable | " + " | ".join(errors))


@app.get("/nse/corporate-actions")
def nse_corporate_actions(
    symbol: str | None = Query(None, description="Optional NSE symbol filter"),
    from_date: str | None = Query(None, description="Optional start date"),
    to_date: str | None = Query(None, description="Optional end date"),
    index: str = Query("equities", description="NSE segment: equities or sme"),
):
    """Return NSE corporate actions such as dividends, splits, bonus and rights."""
    try:
        return _get_corporate_actions(symbol, from_date, to_date, index)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NSE corporate actions unavailable. Underlying error: {str(e)}")


def _get_corporate_announcements(symbol=None, from_date=None, to_date=None, index="equities", page=1, size=100):
    key = (str(symbol or "").upper(), from_date, to_date, index, int(page), int(size))
    with _CORP_ANNOUNCEMENTS_CACHE_LOCK:
        if key in _CORP_ANNOUNCEMENTS_CACHE:
            return _CORP_ANNOUNCEMENTS_CACHE[key]

    params = _filing_params(symbol, from_date, to_date, index)
    params.update({"page": int(page), "size": int(size)})
    payload = nse_api_get("/api/corporate-announcements", params=params)
    rows = _unwrap_records(payload)
    result = {
        "source": "NSE",
        "symbol": _normalize_symbol(symbol) if symbol else None,
        "endpoint": "/api/corporate-announcements",
        "page": int(page),
        "size": int(size),
        "count": len(rows),
        "data": rows,
        "raw": payload,
    }
    with _CORP_ANNOUNCEMENTS_CACHE_LOCK:
        _CORP_ANNOUNCEMENTS_CACHE[key] = result
    return result


@app.get("/nse/corporate-announcements")
def nse_corporate_announcements(
    symbol: str | None = Query(None, description="Optional NSE symbol filter"),
    from_date: str | None = Query(None, description="Optional start date"),
    to_date: str | None = Query(None, description="Optional end date"),
    index: str = Query("equities", description="NSE segment"),
    page: int = Query(1, ge=1, description="NSE filing page"),
    size: int = Query(100, ge=1, le=200, description="Rows per page"),
):
    """Return NSE corporate announcements/disclosures, including attachment metadata."""
    try:
        return _get_corporate_announcements(symbol, from_date, to_date, index, page, size)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NSE corporate announcements unavailable. Underlying error: {str(e)}")


@app.get("/nse/board-meetings")
def nse_board_meetings(
    symbol: str | None = Query(None, description="Optional NSE symbol filter"),
    from_date: str | None = Query(None, description="Optional start date"),
    to_date: str | None = Query(None, description="Optional end date"),
    index: str = Query("equities", description="NSE segment"),
):
    """Return NSE board-meeting filings."""
    try:
        key = (str(symbol or "").upper(), from_date, to_date, index)
        with _BOARD_MEETINGS_CACHE_LOCK:
            if key in _BOARD_MEETINGS_CACHE:
                return _BOARD_MEETINGS_CACHE[key]
        params = _filing_params(symbol, from_date, to_date, index)
        payload = nse_api_get("/api/event-calendar", params=params)
        rows = _unwrap_records(payload)
        result = {
            "source": "NSE",
            "symbol": _normalize_symbol(symbol) if symbol else None,
            "endpoint": "/api/event-calendar",
            "count": len(rows),
            "data": rows,
            "raw": payload,
        }
        with _BOARD_MEETINGS_CACHE_LOCK:
            _BOARD_MEETINGS_CACHE[key] = result
        return result
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NSE board-meeting data unavailable. Underlying error: {str(e)}")


@app.get("/nse/financial-results-filings")
def nse_financial_results_filings(
    symbol: str | None = Query(None, description="Optional NSE symbol filter"),
    from_date: str | None = Query(None, description="Optional broadcast start date"),
    to_date: str | None = Query(None, description="Optional broadcast end date"),
    period: str = Query("quarterly", description="quarterly, annual or half-yearly"),
    index: str = Query("equities", description="NSE segment"),
):
    """Return NSE financial-result filing metadata; numeric P&L is not assumed to be present."""
    try:
        if period not in {"quarterly", "annual", "half-yearly"}:
            raise ValueError("period must be quarterly, annual or half-yearly")
        key = (str(symbol or "").upper(), from_date, to_date, period, index)
        with _FINANCIAL_RESULTS_CACHE_LOCK:
            if key in _FINANCIAL_RESULTS_CACHE:
                return _FINANCIAL_RESULTS_CACHE[key]
        params = _filing_params(symbol, from_date, to_date, index)
        params["period"] = period
        payload = nse_api_get("/api/corporates-financial-results", params=params)
        rows = _unwrap_records(payload)
        result = {
            "source": "NSE",
            "symbol": _normalize_symbol(symbol) if symbol else None,
            "endpoint": "/api/corporates-financial-results",
            "period": period,
            "count": len(rows),
            "data": rows,
            "raw": payload,
        }
        with _FINANCIAL_RESULTS_CACHE_LOCK:
            _FINANCIAL_RESULTS_CACHE[key] = result
        return result
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NSE financial-results filings unavailable. Underlying error: {str(e)}")


# ---------------------------------------------------------
# Index constituent cross-validation
# ---------------------------------------------------------
# Nifty Indices publishes constituent downloads for its equity indices. The
# exchange API remains the primary source; these URLs are an independent
# validation route for the most important broad-market indices.
NIFTY_CONSTITUENT_CSVS = {
    "NIFTY 50": "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "NIFTY 100": "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "NIFTY 200": "https://www.niftyindices.com/IndexConstituent/ind_nifty200list.csv",
    "NIFTY 500": "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "NIFTY MIDCAP 150": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "NIFTY SMALLCAP 250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
    "NIFTY MICROCAP 250": "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250list.csv",
}


def _get_niftyindices_constituents(index_name):
    index_name = _normalize_index_name(index_name)
    url = NIFTY_CONSTITUENT_CSVS.get(index_name)
    if not url:
        return {
            "source": "NSE Indices",
            "index": index_name,
            "count": 0,
            "data": [],
            "error": "No independent CSV mapping configured for this index",
        }
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/146 Safari/537.36",
            "Accept": "text/csv,text/plain,*/*",
        },
        timeout=30,
    )
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.content.decode("utf-8-sig", errors="replace")))
    rows = [dict(row) for row in reader]
    return {
        "source": "NSE Indices",
        "index": index_name,
        "endpoint": url,
        "count": len(rows),
        "data": rows,
    }


@app.get("/nse/index-constituents-validated")
def nse_index_constituents_validated(
    index: str = Query(..., description="Index, e.g. NIFTY 500 or NIFTY 50"),
):
    """Cross-check an index constituent list using NSE and, for mapped indices, NSE Indices CSV."""
    index_name = _normalize_index_name(index)
    if not index_name:
        raise HTTPException(status_code=400, detail="A valid NSE index name is required.")

    exchange_result = _get_index_constituents_cached(index_name)
    independent = _get_niftyindices_constituents(index_name)

    exchange_symbols = set()
    for row in exchange_result.get("data", []):
        if isinstance(row, dict):
            symbol = _normalize_symbol(row.get("symbol") or row.get("Symbol"))
            if symbol:
                exchange_symbols.add(symbol)

    independent_symbols = set()
    for row in independent.get("data", []):
        if isinstance(row, dict):
            symbol = _normalize_symbol(
                row.get("Symbol") or row.get("SYMBOL") or row.get("symbol")
            )
            if symbol:
                independent_symbols.add(symbol)

    if not exchange_symbols and independent_symbols:
        validation_status = "independent_source_only"
    elif exchange_symbols and independent_symbols and exchange_symbols == independent_symbols:
        validation_status = "validated"
    else:
        validation_status = "partial_or_mismatch"

    # For the research pipeline, an independently published Nifty Indices
    # constituent list is still a valid discovery universe even when NSE's
    # live constituent API is unavailable. The response makes that distinction
    # explicit instead of returning an unusable empty universe.
    effective_symbols = sorted(independent_symbols or exchange_symbols)

    return {
        "index": index_name,
        "exchange_source": exchange_result,
        "independent_source": independent,
        "exchange_symbol_count": len(exchange_symbols),
        "independent_symbol_count": len(independent_symbols),
        "common_symbol_count": len(exchange_symbols & independent_symbols),
        "only_exchange": sorted(exchange_symbols - independent_symbols),
        "only_independent": sorted(independent_symbols - exchange_symbols),
        "validation_status": validation_status,
        "effective_symbol_count": len(effective_symbols),
        "effective_symbols": effective_symbols,
        "effective_source": (
            "NSE+NSE_Indices" if exchange_symbols and independent_symbols
            else "NSE_Indices" if independent_symbols
            else "NSE" if exchange_symbols
            else "none"
        ),
    }

@app.get("/")
def root():
    return {
        "service": "Indian Equity Research API",
        "status": "online",
        "version": "0.6.4",
        "data_layers": ["NSE", "equity-quote", "equity-meta-info", "master-universe", "index-catalogue", "index-constituents", "index-union", "master-discovery", "index-membership", "historical-equity", "historical-summary", "corporate-actions", "corporate-announcements", "board-meetings", "financial-results-filings", "index-constituents-validated"],
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Indian Equity Research API",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/nse/datasets")
def nse_datasets():
    """
    Return the currently available NSE datasets exposed
    by the nse-archives package.
    """
    try:
        df = nse.list_datasets()
        return {
            "source": "NSE",
            "count": len(df),
            "datasets": dataframe_to_records(df)
        }
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE dataset catalogue error: {str(e)}"
        )


@app.get("/nse/equities")
def nse_equities(
    date: str | None = Query(
        None,
        description="Optional trading date in YYYY-MM-DD format; defaults to the latest completed NSE trading day"
    )
):
    """
    NSE full securities bhavcopy with delivery information.
    """
    try:
        date_info = resolve_effective_eod_date(date)
        effective_date = date_info["effective_date"]

        df = nse.get(
            "capital_market",
            "equities_sme",
            "sec_bhavdata_full",
            effective_date
        )

        return {
            "source": "NSE",
            "dataset": "sec_bhavdata_full",
            "requested_date": date_info["requested_date"],
            "effective_date": effective_date,
            "date": effective_date,
            "date_adjusted": date_info["date_adjusted"],
            "adjustment_reason": date_info["adjustment_reason"],
            "calendar_source": date_info["calendar_source"],
            "calendar_error": date_info["calendar_error"],
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE equity data unavailable for requested date {date}. "
                f"Underlying error: {str(e)}"
            )
        )


@app.get("/nse/indices")
def nse_indices(
    date: str | None = Query(
        None,
        description="Optional trading date in YYYY-MM-DD format; defaults to the latest completed NSE trading day"
    )
):
    """
    NSE all-index daily closing data.
    """
    try:
        date_info = resolve_effective_eod_date(date)
        effective_date = date_info["effective_date"]

        df = nse.get(
            "capital_market",
            "indices",
            "ind_close_all",
            effective_date
        )

        return {
            "source": "NSE",
            "dataset": "ind_close_all",
            "requested_date": date_info["requested_date"],
            "effective_date": effective_date,
            "date": effective_date,
            "date_adjusted": date_info["date_adjusted"],
            "adjustment_reason": date_info["adjustment_reason"],
            "calendar_source": date_info["calendar_source"],
            "calendar_error": date_info["calendar_error"],
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE index data unavailable for requested date {date}. "
                f"Underlying error: {str(e)}"
            )
        )


@app.get("/nse/bulk-deals")
def nse_bulk_deals():
    """
    NSE daily bulk-deal data.
    """
    try:
        df = nse.get(
            "capital_market",
            "equities_sme",
            "bulk_deals",
            None
        )

        return {
            "source": "NSE",
            "dataset": "bulk_deals",
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE bulk-deal data error: {str(e)}"
        )


@app.get("/nse/block-deals")
def nse_block_deals():
    """
    NSE daily block-deal data.
    """
    try:
        df = nse.get(
            "capital_market",
            "equities_sme",
            "block_deals",
            None
        )

        return {
            "source": "NSE",
            "dataset": "block_deals",
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE block-deal data error: {str(e)}"
        )


@app.get("/nse/mutual-fund")
def nse_mutual_fund(
    date: str | None = Query(
        None,
        description="Optional trading date in YYYY-MM-DD format; defaults to the latest completed NSE trading day"
    )
):
    """
    Dynamically discover the current NSE mutual-fund dataset
    instead of assuming a hard-coded dataset key.
    """
    try:
        datasets = nse.list_datasets()

        mf = datasets[
            (datasets["category"] == "capital_market") &
            (datasets["subcategory"] == "mutual_fund")
        ]

        if mf.empty:
            raise HTTPException(
                status_code=404,
                detail="No NSE mutual-fund dataset is currently listed."
            )

        dataset_row = mf.iloc[0]

        dataset_key = dataset_row["dataset"]
        dataset_name = dataset_row["name"]
        df_supported = dataset_row["df_supported"]

        if not bool(df_supported):
            return {
                "source": "NSE",
                "subcategory": "mutual_fund",
                "dataset": dataset_key,
                "name": dataset_name,
                "df_supported": False,
                "message": (
                    "The current NSE mutual-fund dataset is listed, "
                    "but this package does not expose it through nse.get(). "
                    "A download-based implementation may be required."
                )
            }

        date_info = resolve_effective_eod_date(date)
        effective_date = date_info["effective_date"]

        df = nse.get(
            "capital_market",
            "mutual_fund",
            dataset_key,
            effective_date
        )

        return {
            "source": "NSE",
            "subcategory": "mutual_fund",
            "dataset": dataset_key,
            "name": dataset_name,
            "requested_date": date_info["requested_date"],
            "effective_date": effective_date,
            "date": effective_date,
            "date_adjusted": date_info["date_adjusted"],
            "adjustment_reason": date_info["adjustment_reason"],
            "calendar_source": date_info["calendar_source"],
            "calendar_error": date_info["calendar_error"],
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except HTTPException:
        raise

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        requested_label = date if date else "latest completed NSE trading day"
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE mutual-fund data unavailable for {requested_label}. "
                f"Underlying error: {str(e)}"
            )
        )
