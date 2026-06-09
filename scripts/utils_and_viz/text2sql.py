"""Live text-to-SQL over pension_funds.db via a local Ollama (Fase 3).

The self-hosted dashboard runs on the Mac mini, where Ollama already serves
`qwen2.5:7b-instruct` on localhost:11434 — strong at SQL. This module turns a
Dutch question into a SQLite SELECT, runs it READ-ONLY, and hands the result
back to the dashboard.

Two independent safety layers around the generated SQL:
  1. `is_safe_select()` — static guard: must be a single SELECT/WITH, no DML/DDL
     keywords, no statement chaining.
  2. `run_readonly()` — executes on a `mode=ro` connection, so even a guard
     bypass cannot write. A LIMIT is injected when the query has none.

Config via env (defaults suit the mini): PENSIOEN_OLLAMA_URL, PENSIOEN_OLLAMA_MODEL.
"""

from __future__ import annotations  # 3.9-deploy safe

import os
import re
import sqlite3

import pandas as pd
import requests

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_THIS_DIR))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

OLLAMA_URL = os.environ.get("PENSIOEN_OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("PENSIOEN_OLLAMA_MODEL", "qwen2.5:7b-instruct")

# Compact schema given to the model. Only the tables/columns worth querying,
# plus domain notes that materially improve accuracy.
SCHEMA_DESC = """\
SQLite-database met Nederlandse pensioenfondsen. Relevante tabellen:

funds (één rij per fonds, ~179 rijen):
  id INTEGER, name TEXT, category TEXT,
  aum_euro_bn REAL            -- vermogen onder beheer in miljarden euro
  dekkingsgraad_pct REAL, maanddekkingsgraad_pct REAL,
  beleidsdekkingsgraad_pct REAL, vereiste_dekkingsgraad_pct REAL,
  deelnemers_actief INTEGER, deelnemers_slapers INTEGER,
  deelnemers_gepensioneerd INTEGER, deelnemers_totaal INTEGER,
  uitvoerder TEXT, fiduciair_beheerder TEXT,
  equity_allocation_pct REAL, fixed_income_pct REAL, real_estate_pct REAL,
  alternatives_pct REAL,
  sfdr_article INTEGER, eu_taxonomy_pct REAL,
  wtp_invaren TEXT,           -- waarden: 'ja','nee','uitgesteld' of leeg
  wtp_contract_type TEXT,     -- bv. 'solidair','flexibel','flexibel + rdr','ftk'
  wtp_transitie_datum TEXT    -- ISO YYYY-MM-DD
  -- category-waarden: 'Tak' (bedrijfstak), 'Bedrijf' (ondernemingsfonds),
  --   'Beroep' (beroepspensioen), 'APF', 'PPI', 'Verzekeraar'

historical_metrics (tijdreeks per fonds × jaar, 2015–2025):
  fund_id INTEGER, year INTEGER, beleggingsrendement_pct REAL,
  beleidsdekkingsgraad_pct REAL, vereiste_dekkingsgraad_pct REAL,
  aum_euro_bn REAL, cpi_pct REAL, indexatieverlening_pct REAL,
  deelnemers_totaal INTEGER, zakelijke_waarden_pct REAL, rente_afdekking_pct REAL
  -- koppel aan funds via historical_metrics.fund_id = funds.id

news_articles (nieuwsberichten):
  fund_id INTEGER, title TEXT, published_date TEXT (ISO), url TEXT

Domein: 'dekkingsgraad' = financiële gezondheid (>100% = overschot).
'beleidsdekkingsgraad' = 12-maands gemiddelde. 'deelnemers' = actief/slapers/
gepensioneerd. 'invaren'/'Wtp' = overgang naar nieuw pensioenstelsel.\
"""

PROMPT_TEMPLATE = """\
Je bent een SQLite-expert. Hieronder het schema. Schrijf precies ÉÉN geldige
SQLite SELECT-query die de vraag beantwoordt. Regels:
- UITSLUITEND een SELECT-query (geen INSERT/UPDATE/DELETE/DDL).
- Geen uitleg, geen markdown, alleen de pure SQL.
- Gebruik LIMIT bij vragen om "top/hoogste/laagste N".
- Bij dekkingsgraad/AUM-vragen: negeer NULL-waarden waar zinvol.

SCHEMA:
{schema}

VRAAG: {question}

SQL:"""

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|replace|attach|detach|"
    r"pragma|vacuum|reindex|truncate|grant|revoke)\b",
    re.IGNORECASE,
)


def ollama_available(timeout: float = 2.0) -> bool:
    """Quick reachability + model presence check."""
    try:
        r = requests.get(OLLAMA_URL + "/api/tags", timeout=timeout)
        r.raise_for_status()
        models = [m.get("name", "") for m in r.json().get("models", [])]
        return any(OLLAMA_MODEL.split(":")[0] in m for m in models)
    except Exception:
        return False


def _strip_fences(text: str) -> str:
    """Remove ```sql ... ``` fences / stray prose, keep the SQL body."""
    text = text.strip()
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if m:
        text = m.group(1)
    return text.strip()


def generate_sql(question: str, timeout: float = 120.0) -> str:
    """Ask Ollama for a SQL query. Raises on transport/HTTP error."""
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "options": {"temperature": 0},
        "prompt": PROMPT_TEMPLATE.format(schema=SCHEMA_DESC, question=question),
    }
    r = requests.post(OLLAMA_URL + "/api/generate", json=payload, timeout=timeout)
    r.raise_for_status()
    return _strip_fences(r.json().get("response", ""))


def is_safe_select(sql: str) -> tuple[bool, str]:
    """Static read-only guard. Returns (ok, reason)."""
    s = sql.strip().rstrip(";").strip()
    if not s:
        return False, "lege query"
    low = s.lower()
    if not (low.startswith("select") or low.startswith("with")):
        return False, "alleen SELECT/WITH-queries zijn toegestaan"
    if ";" in s:
        return False, "meerdere statements zijn niet toegestaan"
    if _FORBIDDEN.search(s):
        return False, "query bevat een niet-toegestaan (schrijf-)sleutelwoord"
    return True, ""


def run_readonly(sql: str, limit: int = 1000) -> pd.DataFrame:
    """Execute on a mode=ro connection (defense-in-depth). Injects a LIMIT when
    absent so a runaway query can't return the whole DB."""
    s = sql.strip().rstrip(";")
    if not re.search(r"\blimit\b", s, re.IGNORECASE):
        s = f"SELECT * FROM (\n{s}\n) AS _wrapped LIMIT {limit}"
    uri = "file:" + DB_PATH + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        return pd.read_sql_query(s, conn)
    finally:
        conn.close()


def ask(question: str) -> dict:
    """Full pipeline. Returns a dict with sql / ok / error / dataframe."""
    out = {"sql": "", "ok": False, "error": "", "df": None}
    try:
        out["sql"] = generate_sql(question)
    except Exception as e:
        out["error"] = f"Ollama-aanroep mislukt: {e}"
        return out
    ok, reason = is_safe_select(out["sql"])
    if not ok:
        out["error"] = f"Query geweigerd door veiligheidsguard: {reason}"
        return out
    try:
        out["df"] = run_readonly(out["sql"])
        out["ok"] = True
    except Exception as e:
        out["error"] = f"Query-uitvoering mislukt: {e}"
    return out


if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) or "Top 5 fondsen met hoogste AUM die niet zijn ingevaren"
    print("Ollama bereikbaar:", ollama_available())
    res = ask(q)
    print("SQL:\n", res["sql"])
    print("OK:", res["ok"], "| error:", res["error"])
    if res["df"] is not None:
        print(res["df"].to_string(index=False))
