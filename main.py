from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timedelta
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
    version="0.6.0"
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
    cache_key = (
        date,
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
        date
    )

    symbols = _extract_equity_symbols(df)

    result = {
        "source": "NSE",
        "date": date,
        "dataset": "sec_bhavdata_full",
        "count": len(symbols),
        "symbols": symbols,
        "membership_included": bool(include_membership),
        "membership_strategy": (
            "disabled_by_default"
            if not include_membership
            else "targeted_slice"
        )
    }

    if include_membership:
        offset = max(0, int(membership_offset))

        limit = (
            max_symbols
            if max_symbols is not None
            else MASTER_MEMBERSHIP_MAX_SYMBOLS
        )

        limit = max(1, min(int(limit), len(symbols)))

        selected = symbols[offset:offset + limit]
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
    date: str = Query(
        ...,
        description="Trading date in YYYY-MM-DD format"
    ),
    include_membership: bool = Query(
        False,
        description=(
            "Optional stock-to-index enrichment. Keep false for broad "
            "universe discovery. When true, only max_symbols stocks "
            "starting at membership_offset are enriched."
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
    ),
    membership_offset: int = Query(
        0,
        ge=0,
        description=(
            "Starting position in the master symbol list for optional "
            "membership enrichment."
        )
    )
):
    """
    Return the broad NSE equity discovery universe for a trading date.

    Broad mode:
        include_membership=false

    Targeted enrichment mode:
        include_membership=true&max_symbols=5

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
    date: str = Query(
        ...,
        description="Trading date in YYYY-MM-DD format"
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
        "date": date,
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
def _get_historical_equity(symbol, from_date, to_date, series="EQ"):
    symbol_name = _normalize_symbol(symbol)
    if not symbol_name:
        raise ValueError("A valid NSE equity symbol is required.")

    start = _parse_ddmmyyyy(from_date, "from_date")
    end = _parse_ddmmyyyy(to_date, "to_date")
    if start is None:
        end = end or datetime.now()
        start = end - timedelta(days=30)
    if end is None:
        end = datetime.now()
    if start > end:
        raise ValueError("from_date cannot be later than to_date")

    series_name = str(series or "EQ").strip().upper()
    cache_key = (
        symbol_name,
        _format_nse_date(start),
        _format_nse_date(end),
        series_name,
    )

    with _HISTORICAL_CACHE_LOCK:
        if cache_key in _HISTORICAL_CACHE:
            return _HISTORICAL_CACHE[cache_key]

    # NSE's historical equity API is commonly limited to roughly one year.
    # Chunk long requests rather than silently returning incomplete history.
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=360), end)
        params = {
            "symbol": symbol_name,
            "series": json.dumps([series_name], separators=(",", ":")),
            "from": _format_nse_date(cursor),
            "to": _format_nse_date(chunk_end),
        }
        payload = nse_api_get("/api/historical/cm/equity", params=params)
        rows = _unwrap_records(payload)
        chunks.extend(rows)
        cursor = chunk_end + timedelta(days=1)

    # De-duplicate by date when an upstream response overlaps a boundary.
    dedup = {}
    for row in chunks:
        if not isinstance(row, dict):
            continue
        stamp = (
            row.get("CH_TIMESTAMP")
            or row.get("mTIMESTAMP")
            or row.get("date")
            or row.get("Date")
        )
        key = str(stamp) if stamp is not None else json.dumps(row, sort_keys=True)
        dedup[key] = row

    rows = list(dedup.values())
    rows.sort(
        key=lambda x: str(
            x.get("CH_TIMESTAMP")
            or x.get("mTIMESTAMP")
            or x.get("date")
            or x.get("Date")
            or ""
        )
    )

    result = {
        "source": "NSE",
        "symbol": symbol_name,
        "series": series_name,
        "from_date": _format_nse_date(start),
        "to_date": _format_nse_date(end),
        "count": len(rows),
        "endpoint": "/api/historical/cm/equity",
        "data": rows,
    }
    with _HISTORICAL_CACHE_LOCK:
        _HISTORICAL_CACHE[cache_key] = result
    return result


@app.get("/nse/historical-equity")
def nse_historical_equity(
    symbol: str = Query(..., description="NSE equity symbol, e.g. NETWEB, DIXON, KAYNES"),
    from_date: str | None = Query(None, description="Start date: DD-MM-YYYY or YYYY-MM-DD"),
    to_date: str | None = Query(None, description="End date: DD-MM-YYYY or YYYY-MM-DD"),
    series: str = Query("EQ", description="NSE equity series, normally EQ"),
):
    """Return daily historical OHLCV/turnover data for an NSE equity."""
    try:
        return _get_historical_equity(symbol, from_date, to_date, series)
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE historical equity data unavailable for '{symbol}'. Underlying error: {str(e)}",
        )


@app.get("/nse/historical-summary")
def nse_historical_summary(
    symbol: str = Query(..., description="NSE equity symbol"),
    from_date: str | None = Query(None, description="Optional start date"),
    to_date: str | None = Query(None, description="Optional end date"),
    series: str = Query("EQ", description="NSE equity series"),
):
    """Return historical data plus research-ready return, volatility and liquidity statistics."""
    try:
        result = _get_historical_equity(symbol, from_date, to_date, series)
        rows = result.get("data", [])

        def num(row, *keys):
            for key in keys:
                value = row.get(key) if isinstance(row, dict) else None
                try:
                    if value is not None and str(value).strip() != "":
                        return float(str(value).replace(",", ""))
                except (TypeError, ValueError):
                    pass
            return None

        closes = []
        volumes = []
        values = []
        trades = []
        for row in rows:
            close = num(row, "CH_CLOSING_PRICE", "close", "CLOSE")
            if close is not None:
                closes.append(close)
            volume = num(row, "CH_TOT_TRADED_QTY", "volume", "VOLUME")
            if volume is not None:
                volumes.append(volume)
            value = num(row, "CH_TOT_TRADED_VAL", "value", "VALUE")
            if value is not None:
                values.append(value)
            trade_count = num(row, "CH_TOTAL_TRADES", "No of trades", "NO OF TRADES")
            if trade_count is not None:
                trades.append(trade_count)

        returns = []
        for i in range(1, len(closes)):
            if closes[i - 1]:
                returns.append((closes[i] / closes[i - 1] - 1.0) * 100.0)

        period_return = None
        if len(closes) >= 2 and closes[0]:
            period_return = (closes[-1] / closes[0] - 1.0) * 100.0

        annualized_vol = None
        if len(returns) >= 2:
            mean = sum(returns) / len(returns)
            variance = sum((x - mean) ** 2 for x in returns) / (len(returns) - 1)
            annualized_vol = (variance ** 0.5) * (252 ** 0.5)

        running_high = None
        max_drawdown = 0.0
        for close in closes:
            running_high = close if running_high is None else max(running_high, close)
            if running_high:
                max_drawdown = min(max_drawdown, (close / running_high - 1.0) * 100.0)

        return {
            **result,
            "summary": {
                "period_return_pct": period_return,
                "annualized_volatility_pct": annualized_vol,
                "max_drawdown_pct": max_drawdown,
                "period_high": max(closes) if closes else None,
                "period_low": min(closes) if closes else None,
                "avg_daily_volume": (sum(volumes) / len(volumes)) if volumes else None,
                "avg_daily_traded_value": (sum(values) / len(values)) if values else None,
                "avg_daily_trades": (sum(trades) / len(trades)) if trades else None,
            },
        }
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"NSE historical summary unavailable for '{symbol}'. Underlying error: {str(e)}",
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

    return {
        "index": index_name,
        "exchange_source": exchange_result,
        "independent_source": independent,
        "exchange_symbol_count": len(exchange_symbols),
        "independent_symbol_count": len(independent_symbols),
        "common_symbol_count": len(exchange_symbols & independent_symbols),
        "only_exchange": sorted(exchange_symbols - independent_symbols),
        "only_independent": sorted(independent_symbols - exchange_symbols),
        "validation_status": (
            "validated" if exchange_symbols and independent_symbols and exchange_symbols == independent_symbols
            else "partial_or_mismatch"
        ),
    }

@app.get("/")
def root():
    return {
        "service": "Indian Equity Research API",
        "status": "online",
        "version": "0.6.0",
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
