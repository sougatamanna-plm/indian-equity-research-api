from fastapi import FastAPI, HTTPException, Query
from datetime import datetime
import json
import time
import requests
from urllib.parse import quote
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from nsedata import nse

NSE_BASE = "https://www.nseindia.com"
_nse_session = None

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

MASTER_MEMBERSHIP_WORKERS = 6
MASTER_MEMBERSHIP_MAX_SYMBOLS = 600


app = FastAPI(
    title="Indian Equity Research API",
    description="Free NSE/BSE research data gateway",
    version="0.3.0"
)


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
def get_nse_session():
    """
    Create and warm an NSE browser-like session.

    NSE's live web endpoints use bot protection and may require
    cookies obtained from normal website navigation before API calls.
    """

    global _nse_session

    if _nse_session is not None:
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

    # Step 1: establish the main NSE browser session.
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

    time.sleep(1)

    # Step 2: visit the live equity page.
    # Current NSE implementations use this page to establish
    # additional session state/cookies before API requests.
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

    time.sleep(1)

    _nse_session = session

    return _nse_session


def nse_api_get(path, params=None):
    """
    Request an NSE live JSON endpoint using the warmed session.
    """

    session = get_nse_session()

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

    response.raise_for_status()

    return response.json()



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


def _build_master_universe(date, include_membership=False, max_symbols=None):
    """
    Build the broad NSE equity universe from the daily securities
    bhavcopy.

    By default this returns the full symbol universe without making a
    request for every stock's index membership. Set include_membership
    to true to enrich the first N symbols with getIndexList data.
    """
    cache_key = (
        date,
        bool(include_membership),
        int(max_symbols) if max_symbols is not None else None
    )

    with _MASTER_CACHE_LOCK:
        if cache_key in _MASTER_CACHE:
            return _MASTER_CACHE[cache_key]

    df = nse.get(
        "capital_market",
        "equities_sme",
        "sec_bhavdata_full",
        date
    )

    symbols = _extract_equity_symbols(df)

    result = {
        "source": "NSE",
        "date": date,
        "dataset": "sec_bhavdata_full",
        "count": len(symbols),
        "symbols": symbols,
        "membership_included": bool(include_membership)
    }

    if include_membership:
        limit = max_symbols if max_symbols is not None else MASTER_MEMBERSHIP_MAX_SYMBOLS
        limit = max(1, min(int(limit), len(symbols)))

        selected = symbols[:limit]
        membership = {}

        # A small worker pool avoids the extremely high request volume
        # that would occur if all symbols were requested at once.
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

        # Preserve universe order rather than completion order.
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
    date: str = Query(
        ...,
        description="Trading date in YYYY-MM-DD format"
    ),
    include_membership: bool = Query(
        False,
        description=(
            "Also fetch stock-to-index membership. "
            "This is slower because it uses NSE's per-symbol getIndexList API."
        )
    ),
    max_symbols: int = Query(
        MASTER_MEMBERSHIP_MAX_SYMBOLS,
        ge=1,
        le=2000,
        description=(
            "Maximum number of symbols to enrich when "
            "include_membership=true."
        )
    )
):
    """
    Return the broad NSE equity discovery universe for a trading date.

    Default mode is fast and returns the de-duplicated equity symbol
    universe from the NSE daily securities bhavcopy.

    Optional membership mode enriches up to max_symbols with each
    stock's current NSE index memberships.
    """
    try:
        return _build_master_universe(
            date=date,
            include_membership=include_membership,
            max_symbols=max_symbols
        )

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE master-universe data unavailable for {date}. "
                f"Underlying error: {str(e)}"
            )
        )


@app.get("/nse/index-list")
def nse_index_list():
    """
    Return the currently discoverable NSE index universe
    from NSE's live all-indices endpoint.
    """
    try:
        payload = nse_api_get("/api/allIndices")

        data = payload.get("data", [])

        # Keep the complete raw records while also providing
        # a compact discovery list.
        compact = []

        for item in data:
            compact.append({
                "indexSymbol": item.get("indexSymbol"),
                "index": item.get("index"),
                "key": item.get("key"),
                "indexType": item.get("indexType"),
                "last": item.get("last"),
                "variation": item.get("variation"),
                "percentChange": item.get("percentChange"),
            })

        return {
            "source": "NSE",
            "endpoint": "/api/allIndices",
            "count": len(data),
            "indices": compact,
            "raw_available": True,
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE index catalogue error: {str(e)}"
        )


@app.get("/nse/index-constituents")
def nse_index_constituents(
    index: str = Query(
        ...,
        description=(
            "NSE index name, e.g. NIFTY 50, "
            "NIFTY 500, NIFTY MIDCAP 150, NIFTY BANK"
        )
    )
):
    """
    Return constituent-level data for a selected NSE index.

    Try the standard NSE equity-stockIndices endpoint first.
    If NSE returns an endpoint-level failure, try the newer NextApi
    infrastructure used by current NSE client implementations.
    """

    index_name = index.strip().upper()
    errors = []

    # Method 1: standard NSE index constituent endpoint.
    try:
        payload = nse_api_get(
            "/api/equity-stockIndices",
            params={"index": index_name}
        )

        data = payload.get("data", []) if isinstance(payload, dict) else []

        return {
            "source": "NSE",
            "index_requested": index,
            "index_normalized": index_name,
            "endpoint": "/api/equity-stockIndices",
            "count": len(data),
            "data": data,
        }

    except Exception as e:
        errors.append(f"standard endpoint: {str(e)}")

    # Method 2: current NSE NextApi infrastructure.
    try:
        payload = nse_api_get(
            "/api/NextApi/apiClient/GetQuoteApi",
            params={
                "functionName": "getEquityStockIndices",
                "index": index_name,
            }
        )

        if isinstance(payload, dict):
            data = payload.get("data", [])
        else:
            data = payload

        if data is None:
            data = []

        if not isinstance(data, list):
            data = [data]

        return {
            "source": "NSE",
            "index_requested": index,
            "index_normalized": index_name,
            "endpoint": "/api/NextApi/apiClient/GetQuoteApi",
            "functionName": "getEquityStockIndices",
            "count": len(data),
            "data": data,
            "raw": payload,
        }

    except Exception as e:
        errors.append(f"NextApi endpoint: {str(e)}")

    raise HTTPException(
        status_code=502,
        detail=(
            f"NSE constituent data unavailable for index '{index}'. "
            + " | ".join(errors)
        )
    )


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


@app.get("/")
def root():
    return {
        "service": "Indian Equity Research API",
        "status": "online",
        "version": "0.3.0",
        "data_layers": ["NSE", "master-universe", "index-membership"],
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
    date: str = Query(
        ...,
        description="Trading date in YYYY-MM-DD format"
    )
):
    """
    NSE full securities bhavcopy with delivery information.
    """
    try:
        df = nse.get(
            "capital_market",
            "equities_sme",
            "sec_bhavdata_full",
            date
        )

        return {
            "source": "NSE",
            "dataset": "sec_bhavdata_full",
            "date": date,
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE equity data unavailable for {date}. "
                f"Check that the date is a trading day. "
                f"Underlying error: {str(e)}"
            )
        )


@app.get("/nse/indices")
def nse_indices(
    date: str = Query(
        ...,
        description="Trading date in YYYY-MM-DD format"
    )
):
    """
    NSE all-index daily closing data.
    """
    try:
        df = nse.get(
            "capital_market",
            "indices",
            "ind_close_all",
            date
        )

        return {
            "source": "NSE",
            "dataset": "ind_close_all",
            "date": date,
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE index data unavailable for {date}. "
                f"Check that the date is a trading day. "
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
    date: str = Query(
        ...,
        description="Trading date in YYYY-MM-DD format"
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

        df = nse.get(
            "capital_market",
            "mutual_fund",
            dataset_key,
            date
        )

        return {
            "source": "NSE",
            "subcategory": "mutual_fund",
            "dataset": dataset_key,
            "name": dataset_name,
            "date": date,
            "count": len(df),
            "data": dataframe_to_records(df)
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE mutual-fund data unavailable for {date}. "
                f"Underlying error: {str(e)}"
            )
        )
