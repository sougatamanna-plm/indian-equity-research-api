from fastapi import FastAPI, HTTPException, Query
from datetime import datetime
import json
import time
import requests
from urllib.parse import quote

from nsedata import nse


app = FastAPI(
    title="Indian Equity Research API",
    description="Free NSE/BSE research data gateway",
    version="0.2.0"
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

NSE_BASE = "https://www.nseindia.com"

_nse_session = None


def get_nse_session():
    """
    Create a warmed-up NSE browser-like session.
    NSE may reject requests that do not first establish
    a normal website session.
    """
    global _nse_session

    if _nse_session is not None:
        return _nse_session

    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/151.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "application/json,text/plain,*/*"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive",
    })

    # Warm the NSE session before calling the API.
    response = session.get(
        NSE_BASE + "/",
        timeout=20
    )

    response.raise_for_status()

    time.sleep(1)

    _nse_session = session

    return _nse_session


def nse_api_get(path, params=None):
    """
    GET an NSE JSON API endpoint using a warmed session.
    """
    session = get_nse_session()

    response = session.get(
        NSE_BASE + path,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    return response.json()

# ---------------------------------------------------------
# NSE INDEX DISCOVERY
# ---------------------------------------------------------

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
    """

    try:
        payload = nse_api_get(
            "/api/equity-stockIndices",
            params={"index": index}
        )

        data = payload.get("data", [])

        return {
            "source": "NSE",
            "index_requested": index,
            "count": len(data),
            "data": data,
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"NSE constituent data unavailable for "
                f"index '{index}'. "
                f"Underlying error: {str(e)}"
            )
        )




@app.get("/")
def root():
    return {
        "service": "Indian Equity Research API",
        "status": "online",
        "version": "0.2.0",
        "data_layers": ["NSE"],
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
