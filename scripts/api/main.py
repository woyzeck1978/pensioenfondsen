"""Read-only REST API over pension_funds.db (Fase 4 / ROADMAP F2).

A FastAPI sidecar next to the Streamlit dashboard on the Mac mini. Bound to
0.0.0.0:8503 and reached over Tailscale (100.107.33.80:8503) from the user's
own machines — for reuse in Excel / Power Query / the WAM-administratie. No
public exposure, so no auth layer.

Read-only by construction: every query runs on a `mode=ro` SQLite connection.
The dashboard's manual corrections (overrides in app_state.db) are layered on
top of the funds endpoints so the API matches what the dashboard shows.

Run (via launchd nl.wuite.pensioenfondsen.api):
    uvicorn main:app --host 0.0.0.0 --port 8503
Interactive docs: http://100.107.33.80:8503/api/docs
"""

from __future__ import annotations  # 3.9-deploy safe

import os
import sqlite3
import sys
from typing import Optional  # FastAPI resolves param hints at runtime; on 3.9
                             # `X | None` (PEP 604) fails, so use Optional[...].

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_THIS_DIR))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Reuse the dashboard's overrides layer so the API and UI agree.
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "utils_and_viz"))
try:
    import local_state
except Exception:  # pragma: no cover - defensive
    local_state = None

app = FastAPI(
    title="Nederlandse Pensioenfondsen API",
    description="Read-only data over Nederlandse pensioenfondsen "
                "(funds, historische metrics, nieuws).",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# NL-friendly sort keys -> actual funds columns (whitelist; prevents injection
# via order_by and keeps the surface stable).
_FUND_SORT = {
    "aum": "aum_euro_bn",
    "beleidsdekkingsgraad": "beleidsdekkingsgraad_pct",
    "deelnemers": "deelnemers_totaal",
    "naam": "name",
    "id": "id",
}


def _ro_conn() -> sqlite3.Connection:
    return sqlite3.connect("file:" + DB_PATH + "?mode=ro", uri=True, timeout=10)


def _read(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = _ro_conn()
    try:
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()


def _records(df: pd.DataFrame) -> list:
    """DataFrame -> JSON-safe list of dicts (NaN -> null)."""
    if df.empty:
        return []
    return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")


def _funds_with_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if local_state is not None and not df.empty:
        try:
            return local_state.apply_overrides(df, id_col="id")
        except Exception:
            return df
    return df


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {
        "name": "Nederlandse Pensioenfondsen API",
        "docs": "/api/docs",
        "endpoints": [
            "/api/health",
            "/api/funds",
            "/api/funds/{fund_id}",
            "/api/historical/{fund_id}",
            "/api/news",
        ],
    }


@app.get("/api/health")
def health() -> dict:
    df = _read("SELECT COUNT(*) AS n FROM funds")
    return {
        "status": "ok",
        "funds": int(df["n"][0]),
        "overrides_layer": local_state is not None,
        "db_mtime": os.path.getmtime(DB_PATH),
    }


@app.get("/api/funds")
def list_funds(
    category: Optional[str] = Query(None, description="Filter op funds.category"),
    min_aum: Optional[float] = Query(None, description="Minimale AUM (€ mld)"),
    max_aum: Optional[float] = Query(None, description="Maximale AUM (€ mld)"),
    ingevaren: Optional[str] = Query(
        None, description="Filter op wtp_invaren: ja / nee / uitgesteld"),
    order_by: str = Query("aum", description="aum | beleidsdekkingsgraad | "
                                             "deelnemers | naam | id"),
    desc: bool = Query(True, description="Aflopend sorteren"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    if order_by not in _FUND_SORT:
        raise HTTPException(400, f"order_by moet zijn: {sorted(_FUND_SORT)}")
    where, params = [], []
    if category:
        where.append("category = ?"); params.append(category)
    if min_aum is not None:
        where.append("aum_euro_bn >= ?"); params.append(min_aum)
    if max_aum is not None:
        where.append("aum_euro_bn <= ?"); params.append(max_aum)
    if ingevaren:
        where.append("wtp_invaren = ?"); params.append(ingevaren)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    col = _FUND_SORT[order_by]
    direction = "DESC" if desc else "ASC"
    # order_by column comes from the whitelist; limit/offset are ints.
    sql = (f"SELECT * FROM funds{where_sql} "
           f"ORDER BY {col} {direction} LIMIT ? OFFSET ?")
    df = _funds_with_overrides(_read(sql, tuple(params) + (limit, offset)))
    return {"count": len(df), "funds": _records(df)}


@app.get("/api/funds/{fund_id}")
def get_fund(fund_id: int) -> dict:
    df = _funds_with_overrides(_read("SELECT * FROM funds WHERE id = ?", (fund_id,)))
    rec = _records(df)
    if not rec:
        raise HTTPException(404, f"Fonds {fund_id} niet gevonden")
    return rec[0]


@app.get("/api/historical/{fund_id}")
def get_historical(fund_id: int) -> dict:
    df = _read(
        "SELECT * FROM historical_metrics WHERE fund_id = ? ORDER BY year",
        (fund_id,),
    )
    return {"fund_id": fund_id, "count": len(df), "metrics": _records(df)}


@app.get("/api/news")
def list_news(
    fund_id: Optional[int] = Query(None, description="Filter op fonds"),
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    where = "WHERE published_date IS NOT NULL"
    params: list = []
    if fund_id is not None:
        where += " AND fund_id = ?"; params.append(fund_id)
    df = _read(
        f"SELECT fund_id, title, published_date, url FROM news_articles "
        f"{where} ORDER BY published_date DESC LIMIT ?",
        tuple(params) + (limit,),
    )
    return {"count": len(df), "news": _records(df)}
