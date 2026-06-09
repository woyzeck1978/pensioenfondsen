"""Local, writable companion state for the self-hosted dashboard.

Lives in ``data/processed/app_state.db`` (gitignored) — deliberately SEPARATE
from ``pension_funds.db`` so that:

  * the bi-daily launchd scraper can keep writing + ``git push``-ing
    ``pension_funds.db`` without ever conflicting with human edits made in the
    dashboard;
  * manual corrections survive every scraper run and every ``git pull``.

This module only makes sense on a self-hosted deployment (Mac mini behind the
Cloudflare Tunnel) — on the old read-only Streamlit Cloud deployment a writable
DB would be wiped on every rebuild.

Three concerns, three tables:

  * ``overrides``   — manual corrections to ``funds.<column>``, applied on read
                      by :func:`apply_overrides`. The bron-DB is never mutated.
  * ``watchlist``   — pinned funds, persisted across sessions (used from Fase 2).
  * ``alerts_seen`` — which alert signals already fired (used from Fase 2).

plus an append-only ``edit_audit`` log of every override write.

Promoting an override into ``pension_funds.db`` is a separate, explicit step
(:func:`promote_override`) — never automatic. The default workflow keeps the
correction in the overrides layer so the next scraper run can't silently
clobber it.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd

# --- PATHS (mirror dashboard.py's self-locating pattern) ---
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_THIS_DIR))  # repo root
_PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
MAIN_DB_PATH = os.path.join(_PROCESSED_DIR, "pension_funds.db")
STATE_DB_PATH = os.path.join(_PROCESSED_DIR, "app_state.db")

# Columns a human may NOT override from the curation UI — identity / keys.
PROTECTED_COLUMNS = {"id", "name"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    """Open the writable state DB in WAL mode (concurrent reads while the
    dashboard writes; no lock contention with the read-only main-DB access)."""
    conn = sqlite3.connect(STATE_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_state_db() -> None:
    """Create the state tables if they don't exist. Idempotent; cheap to call
    on every dashboard start."""
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS overrides (
                fund_id     INTEGER NOT NULL,
                column_name TEXT    NOT NULL,
                new_value   TEXT,                 -- stored as text, cast on apply
                is_null     INTEGER NOT NULL DEFAULT 0,  -- 1 => override sets NULL
                coltype     TEXT,                 -- INTEGER / REAL / TEXT (from PRAGMA)
                source      TEXT    DEFAULT 'dashboard',
                note        TEXT,
                updated_ts  TEXT    NOT NULL,
                PRIMARY KEY (fund_id, column_name)
            );

            CREATE TABLE IF NOT EXISTS edit_audit (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_id     INTEGER NOT NULL,
                column_name TEXT    NOT NULL,
                old_value   TEXT,
                new_value   TEXT,
                is_null     INTEGER NOT NULL DEFAULT 0,
                action      TEXT    NOT NULL,      -- set / clear / promote
                source      TEXT    DEFAULT 'dashboard',
                note        TEXT,
                ts          TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                fund_id   INTEGER PRIMARY KEY,
                added_ts  TEXT NOT NULL,
                note      TEXT
            );

            -- Alert feed shown in the dashboard. dedup_key makes the engine
            -- idempotent: re-running it never double-inserts the same signal
            -- (INSERT OR IGNORE on the UNIQUE key).
            CREATE TABLE IF NOT EXISTS alerts (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_id    INTEGER,
                kind       TEXT    NOT NULL,    -- news / jaarverslag / dekkingsgraad
                dedup_key  TEXT    NOT NULL UNIQUE,
                title      TEXT    NOT NULL,
                detail     TEXT,
                ref_url    TEXT,
                created_ts TEXT    NOT NULL,
                is_read    INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_alerts_read ON alerts(is_read, id);
            """
        )


# --- COLUMN TYPE INTROSPECTION (from the main DB schema) ---
_coltype_cache: dict[str, str] | None = None


def funds_column_types() -> dict[str, str]:
    """Map ``funds`` column name -> declared SQLite type (INTEGER/REAL/TEXT).
    Cached after first read."""
    global _coltype_cache
    if _coltype_cache is None:
        conn = sqlite3.connect(MAIN_DB_PATH, timeout=10)
        try:
            rows = conn.execute("PRAGMA table_info(funds)").fetchall()
        finally:
            conn.close()
        out: dict[str, str] = {}
        for _cid, name, decl, *_rest in rows:
            decl = (decl or "TEXT").upper()
            if "INT" in decl:
                out[name] = "INTEGER"
            elif any(t in decl for t in ("REAL", "FLOA", "DOUB", "NUM", "DEC")):
                out[name] = "REAL"
            else:
                out[name] = "TEXT"
        _coltype_cache = out
    return _coltype_cache


def editable_columns() -> list[str]:
    """Funds columns a human may edit from the curation UI, sorted."""
    return sorted(c for c in funds_column_types() if c not in PROTECTED_COLUMNS)


def _cast(value, is_null: int, coltype: str | None):
    """Cast a stored text override back to its column's Python type."""
    if is_null:
        return None
    if value is None or value == "":
        return None
    if coltype == "REAL":
        return float(value)
    if coltype == "INTEGER":
        # tolerate "10" and "10.0"
        return int(float(value))
    return str(value)


# --- READS ---
def get_overrides() -> pd.DataFrame:
    """All overrides as a DataFrame (empty DataFrame with the right columns if
    none)."""
    with _connect() as conn:
        df = pd.read_sql_query(
            "SELECT fund_id, column_name, new_value, is_null, coltype, "
            "source, note, updated_ts FROM overrides ORDER BY updated_ts DESC",
            conn,
        )
    return df


def list_overrides_with_names() -> pd.DataFrame:
    """Overrides joined with the fund name, for display on the curation page."""
    ov = get_overrides()
    if ov.empty:
        return ov
    conn = sqlite3.connect(MAIN_DB_PATH, timeout=10)
    try:
        names = pd.read_sql_query("SELECT id AS fund_id, name FROM funds", conn)
    finally:
        conn.close()
    return ov.merge(names, on="fund_id", how="left")


def get_audit(limit: int = 100) -> pd.DataFrame:
    with _connect() as conn:
        return pd.read_sql_query(
            "SELECT ts, fund_id, column_name, action, old_value, new_value, "
            "is_null, note FROM edit_audit ORDER BY id DESC LIMIT ?",
            conn,
            params=(limit,),
        )


def apply_overrides(df: pd.DataFrame, id_col: str = "id") -> pd.DataFrame:
    """Return a copy of ``df`` with every override whose column is present in
    ``df`` applied. Only touches matching (fund_id, column) cells; everything
    else is untouched. Safe to call on any funds-derived DataFrame."""
    if df.empty or id_col not in df.columns:
        return df
    ov = get_overrides()
    if ov.empty:
        return df
    out = df.copy()
    for _, r in ov.iterrows():
        col = r["column_name"]
        if col not in out.columns:
            continue
        val = _cast(r["new_value"], int(r["is_null"]), r["coltype"])
        mask = out[id_col] == r["fund_id"]
        if mask.any():
            out.loc[mask, col] = val
    return out


# --- WRITES ---
def _current_main_value(fund_id: int, column: str):
    """Read the current bron-DB value (for audit old_value + the NULL-only
    guard). Returns the raw value or None."""
    conn = sqlite3.connect(MAIN_DB_PATH, timeout=10)
    try:
        # column is validated by the caller against editable_columns()
        row = conn.execute(
            f"SELECT {column} FROM funds WHERE id = ?", (fund_id,)
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def set_override(
    fund_id: int,
    column: str,
    value,
    *,
    is_null: bool = False,
    source: str = "dashboard",
    note: str = "",
    null_only: bool = True,
) -> tuple[bool, str]:
    """Upsert an override and append an audit row.

    Returns ``(ok, message)``. With ``null_only=True`` (the project's default
    fill-guard) the write is REFUSED when the bron-DB already holds a non-NULL
    value — the caller must explicitly pass ``null_only=False`` to overwrite a
    hand-curated value (e.g. fixing KPN's AUM 1.1 -> 10.0).
    """
    if column in PROTECTED_COLUMNS:
        return False, f"Kolom '{column}' is beschermd en niet bewerkbaar."
    if column not in funds_column_types():
        return False, f"Onbekende kolom '{column}'."

    coltype = funds_column_types()[column]
    current = _current_main_value(fund_id, column)

    if null_only and current is not None:
        return (
            False,
            f"NULL-only-guard actief: '{column}' heeft al een waarde "
            f"({current!r}). Zet de guard uit om te overschrijven.",
        )

    # Validate cast up-front so we never store an unparseable value.
    stored_value = None if is_null else ("" if value is None else str(value))
    if not is_null:
        try:
            _cast(stored_value, 0, coltype)
        except (TypeError, ValueError):
            return False, f"Waarde {value!r} past niet bij type {coltype}."

    ts = _now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO overrides
                (fund_id, column_name, new_value, is_null, coltype,
                 source, note, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_id, column_name) DO UPDATE SET
                new_value=excluded.new_value, is_null=excluded.is_null,
                coltype=excluded.coltype, source=excluded.source,
                note=excluded.note, updated_ts=excluded.updated_ts
            """,
            (fund_id, column, stored_value, int(is_null), coltype,
             source, note, ts),
        )
        conn.execute(
            """
            INSERT INTO edit_audit
                (fund_id, column_name, old_value, new_value, is_null,
                 action, source, note, ts)
            VALUES (?, ?, ?, ?, ?, 'set', ?, ?, ?)
            """,
            (fund_id, column,
             None if current is None else str(current),
             stored_value, int(is_null), source, note, ts),
        )
    disp = "NULL" if is_null else value
    return True, f"Override opgeslagen: {column} → {disp}."


def clear_override(fund_id: int, column: str, source: str = "dashboard") -> tuple[bool, str]:
    """Remove an override; the bron-DB value takes over again on next read."""
    ts = _now_iso()
    with _connect() as conn:
        cur = conn.execute(
            "SELECT new_value, is_null FROM overrides "
            "WHERE fund_id=? AND column_name=?",
            (fund_id, column),
        ).fetchone()
        if cur is None:
            return False, "Geen override om te verwijderen."
        conn.execute(
            "DELETE FROM overrides WHERE fund_id=? AND column_name=?",
            (fund_id, column),
        )
        conn.execute(
            """
            INSERT INTO edit_audit
                (fund_id, column_name, old_value, new_value, is_null,
                 action, source, note, ts)
            VALUES (?, ?, ?, NULL, ?, 'clear', ?, '', ?)
            """,
            (fund_id, column, cur["new_value"], cur["is_null"], source, ts),
        )
    return True, f"Override op '{column}' verwijderd."


def promote_override(fund_id: int, column: str, source: str = "dashboard") -> tuple[bool, str]:
    """Write an override permanently into ``pension_funds.db`` and drop it from
    the overrides layer. Use only when the pipeline no longer overwrites the
    column — otherwise the next scraper run reverts it. Logged as 'promote'."""
    if column in PROTECTED_COLUMNS or column not in funds_column_types():
        return False, f"Kolom '{column}' kan niet gepromoot worden."
    ov = get_overrides()
    row = ov[(ov.fund_id == fund_id) & (ov.column_name == column)]
    if row.empty:
        return False, "Geen override om te promoten."
    r = row.iloc[0]
    val = _cast(r["new_value"], int(r["is_null"]), r["coltype"])
    ts = _now_iso()
    main = sqlite3.connect(MAIN_DB_PATH, timeout=10)
    try:
        old = _current_main_value(fund_id, column)
        main.execute(f"UPDATE funds SET {column} = ? WHERE id = ?", (val, fund_id))
        main.commit()
    finally:
        main.close()
    with _connect() as conn:
        conn.execute(
            "DELETE FROM overrides WHERE fund_id=? AND column_name=?",
            (fund_id, column),
        )
        conn.execute(
            """
            INSERT INTO edit_audit
                (fund_id, column_name, old_value, new_value, is_null,
                 action, source, note, ts)
            VALUES (?, ?, ?, ?, ?, 'promote', ?, '', ?)
            """,
            (fund_id, column, None if old is None else str(old),
             r["new_value"], int(r["is_null"]), source, ts),
        )
    return True, f"'{column}' gepromoot naar pension_funds.db."


# --- WATCHLIST (persistent; keyed on stable fund_id) ---
WATCHLIST_MAX = 50


def list_watchlist() -> list[int]:
    """Watched fund_ids, most-recently-added first."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT fund_id FROM watchlist ORDER BY added_ts DESC"
        ).fetchall()
    return [int(r["fund_id"]) for r in rows]


def is_watched(fund_id: int) -> bool:
    with _connect() as conn:
        return conn.execute(
            "SELECT 1 FROM watchlist WHERE fund_id=?", (fund_id,)
        ).fetchone() is not None


def add_watch(fund_id: int, note: str = "") -> bool:
    with _connect() as conn:
        n = conn.execute("SELECT COUNT(*) FROM watchlist").fetchone()[0]
        if n >= WATCHLIST_MAX:
            return False
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (fund_id, added_ts, note) "
            "VALUES (?, ?, ?)",
            (fund_id, _now_iso(), note),
        )
    return True


def remove_watch(fund_id: int) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM watchlist WHERE fund_id=?", (fund_id,))


def toggle_watch(fund_id: int) -> bool:
    """Flip membership. Returns the new watched-state (True=now watched)."""
    if is_watched(fund_id):
        remove_watch(fund_id)
        return False
    add_watch(fund_id)
    return True


# --- ALERTS (feed shown in the dashboard; written by generate_alerts.py) ---
def add_alert(
    kind: str,
    dedup_key: str,
    title: str,
    *,
    fund_id: int | None = None,
    detail: str = "",
    ref_url: str = "",
) -> bool:
    """Insert one alert. Idempotent via dedup_key. Returns True if a NEW row was
    inserted (i.e. this signal hadn't been seen before)."""
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO alerts
                (fund_id, kind, dedup_key, title, detail, ref_url, created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (fund_id, kind, dedup_key, title, detail, ref_url, _now_iso()),
        )
        return cur.rowcount > 0


def get_alerts(unread_only: bool = False, limit: int = 100) -> pd.DataFrame:
    # fund name lives in the main DB; join via a two-step (no cross-DB ATTACH).
    with _connect() as conn:
        df = pd.read_sql_query(
            "SELECT id, fund_id, kind, title, detail, ref_url, created_ts, "
            "is_read FROM alerts "
            + ("WHERE is_read=0 " if unread_only else "")
            + "ORDER BY id DESC LIMIT ?",
            conn, params=(limit,),
        )
    if df.empty:
        return df
    conn = sqlite3.connect(MAIN_DB_PATH, timeout=10)
    try:
        names = pd.read_sql_query("SELECT id AS fund_id, name AS fund_name FROM funds", conn)
    finally:
        conn.close()
    return df.merge(names, on="fund_id", how="left")


def unread_alert_count() -> int:
    with _connect() as conn:
        return int(conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE is_read=0"
        ).fetchone()[0])


def mark_alerts_read(ids: list[int] | None = None) -> None:
    """Mark given alert ids read, or all if ids is None."""
    with _connect() as conn:
        if ids is None:
            conn.execute("UPDATE alerts SET is_read=1 WHERE is_read=0")
        elif ids:
            qs = ",".join("?" * len(ids))
            conn.execute(f"UPDATE alerts SET is_read=1 WHERE id IN ({qs})", ids)


if __name__ == "__main__":
    # Smoke test: init + a dry round-trip without touching the main DB.
    init_state_db()
    print("state db:", STATE_DB_PATH)
    print("editable columns:", len(editable_columns()))
    print("current overrides:", len(get_overrides()))
    print("watchlist:", list_watchlist())
    print("unread alerts:", unread_alert_count())
