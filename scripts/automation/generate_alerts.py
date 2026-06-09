#!/usr/bin/env python3
"""Alert engine for the self-hosted dashboard (Fase 2 / ROADMAP D3).

Runs AFTER the bi-daily scraper (hooked into scrape_push.sh). Scans the freshly
updated pension_funds.db for three signals and writes any NEW ones into the
``alerts`` feed in app_state.db (idempotent via dedup_key — re-running never
double-inserts):

  1. news         — a watched fund published a news article since the cutoff.
  2. jaarverslag  — a watched fund (or any fund with --all-funds) announced a
                    new annual report for the current / previous year.
  3. dekkingsgraad— a watched fund's beleidsdekkingsgraad CROSSED one of the
                    thresholds (default 110/105/100) between its last two
                    monthly data points.

Delivery is pluggable and OFF by default — alerts always land in the dashboard
feed; outbound push only happens when an env var is set (see notify()).

Usage:
    python3 scripts/automation/generate_alerts.py --dry-run
    python3 scripts/automation/generate_alerts.py            # write to feed
    python3 scripts/automation/generate_alerts.py --all-funds --days 30
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

# Import the shared state module (lives in scripts/utils_and_viz/).
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_THIS_DIR))
sys.path.insert(0, os.path.join(_REPO, "scripts", "utils_and_viz"))
import local_state as ls  # noqa: E402

MAIN_DB = ls.MAIN_DB_PATH
GARBAGE_TITLES = {"nieuws", "sign in", "inloggen", "home", "actueel", ""}


def _main_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(MAIN_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _is_garbage(title: str | None) -> bool:
    if not title:
        return True
    return title.strip().lower() in GARBAGE_TITLES or len(title.strip()) < 6


# --- SCANNERS (each returns a list of alert dicts) ---
def scan_news(conn, fund_ids: list[int], days: int) -> list[dict]:
    if not fund_ids:
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    qs = ",".join("?" * len(fund_ids))
    rows = conn.execute(
        f"""SELECT fund_id, url, title, published_date
            FROM news_articles
            WHERE fund_id IN ({qs})
              AND published_date IS NOT NULL
              AND date(published_date) >= date(?)
            ORDER BY published_date DESC""",
        (*fund_ids, cutoff),
    ).fetchall()
    out = []
    for r in rows:
        if _is_garbage(r["title"]):
            continue
        out.append({
            "kind": "news",
            "dedup_key": f"news:{r['url']}",
            "title": r["title"].strip(),
            "detail": f"Gepubliceerd {r['published_date'][:10]}",
            "ref_url": r["url"],
            "fund_id": r["fund_id"],
        })
    return out


def scan_jaarverslag(conn, fund_ids: list[int] | None, years: list[int]) -> list[dict]:
    """fund_ids=None => scan all funds (broad radar)."""
    year_likes = " OR ".join(["title LIKE ?"] * len(years))
    params: list = [f"%jaarverslag%{y}%" for y in years]
    where = f"({year_likes}) AND published_date IS NOT NULL"
    if fund_ids:
        qs = ",".join("?" * len(fund_ids))
        where += f" AND fund_id IN ({qs})"
        params += fund_ids
    rows = conn.execute(
        f"""SELECT fund_id, url, title, published_date
            FROM news_articles WHERE {where}
            ORDER BY published_date DESC""",
        params,
    ).fetchall()
    out = []
    for r in rows:
        # derive the report year from the matched title
        yr = next((y for y in years if str(y) in (r["title"] or "")), years[0])
        out.append({
            "kind": "jaarverslag",
            "dedup_key": f"jaarverslag:{r['fund_id']}:{yr}",
            "title": f"Nieuw jaarverslag {yr}: {r['title'].strip()}",
            "detail": f"Aangekondigd {r['published_date'][:10]}",
            "ref_url": r["url"],
            "fund_id": r["fund_id"],
        })
    return out


def scan_dekkingsgraad(conn, fund_ids: list[int], thresholds: list[float]) -> list[dict]:
    if not fund_ids:
        return []
    out = []
    for fid in fund_ids:
        rows = conn.execute(
            """SELECT year, month, beleidsdekkingsgraad_pct AS dg
               FROM monthly_funding_ratios
               WHERE fund_id=? AND beleidsdekkingsgraad_pct IS NOT NULL
               ORDER BY year DESC, month DESC LIMIT 2""",
            (fid,),
        ).fetchall()
        if len(rows) < 2:
            continue
        curr, prev = rows[0], rows[1]
        c, p = curr["dg"], prev["dg"]
        period = f"{curr['year']}{curr['month']:02d}"
        for t in thresholds:
            if p >= t > c:
                out.append({
                    "kind": "dekkingsgraad",
                    "dedup_key": f"dg:{fid}:{t:g}:dn:{period}",
                    "title": f"Beleidsdekkingsgraad daalde onder {t:g}%",
                    "detail": f"{p:.1f}% → {c:.1f}% ({curr['year']}-{curr['month']:02d})",
                    "ref_url": "",
                    "fund_id": fid,
                })
            elif p < t <= c:
                out.append({
                    "kind": "dekkingsgraad",
                    "dedup_key": f"dg:{fid}:{t:g}:up:{period}",
                    "title": f"Beleidsdekkingsgraad steeg boven {t:g}%",
                    "detail": f"{p:.1f}% → {c:.1f}% ({curr['year']}-{curr['month']:02d})",
                    "ref_url": "",
                    "fund_id": fid,
                })
    return out


# --- DELIVERY (pluggable, off by default) ---
def notify(new_alerts: list[dict]) -> None:
    """Push a one-line summary outbound IF a channel env var is configured.
    Supported (any/all):
      PENSIOEN_NTFY_TOPIC   -> POST to https://ntfy.sh/<topic>
      PENSIOEN_ALERT_WEBHOOK-> POST JSON {text:...} to the given URL
    Without either, alerts live only in the dashboard feed."""
    if not new_alerts:
        return
    topic = os.environ.get("PENSIOEN_NTFY_TOPIC")
    webhook = os.environ.get("PENSIOEN_ALERT_WEBHOOK")
    if not (topic or webhook):
        return
    body = f"{len(new_alerts)} nieuwe pensioen-melding(en):\n" + "\n".join(
        f"• {a['title']}" for a in new_alerts[:10]
    )
    try:
        import requests
        if topic:
            requests.post(f"https://ntfy.sh/{topic}", data=body.encode("utf-8"),
                          timeout=10)
        if webhook:
            requests.post(webhook, json={"text": body}, timeout=10)
    except Exception as e:  # pragma: no cover - best-effort
        print(f"[notify] outbound failed: {e}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate dashboard alerts.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Scan and print, but write nothing.")
    ap.add_argument("--days", type=int, default=45,
                    help="News lookback window in days (default 45).")
    ap.add_argument("--thresholds", default="110,105,100",
                    help="Beleidsdekkingsgraad crossing thresholds.")
    ap.add_argument("--all-funds", action="store_true",
                    help="Scan jaarverslag signal across ALL funds, not just "
                         "the watchlist.")
    args = ap.parse_args()

    ls.init_state_db()
    watch = ls.list_watchlist()
    thresholds = [float(x) for x in args.thresholds.split(",") if x.strip()]
    this_year = datetime.now(timezone.utc).year
    years = [this_year, this_year - 1]

    conn = _main_conn()
    try:
        found: list[dict] = []
        found += scan_news(conn, watch, args.days)
        found += scan_jaarverslag(conn, None if args.all_funds else watch, years)
        found += scan_dekkingsgraad(conn, watch, thresholds)
    finally:
        conn.close()

    print(f"watchlist: {len(watch)} fund(s) | candidate signals: {len(found)}")

    new_alerts = []
    for a in found:
        if args.dry_run:
            print(f"  [DRY new?] {a['kind']:13} {a['title']}")
            continue
        is_new = ls.add_alert(
            a["kind"], a["dedup_key"], a["title"],
            fund_id=a["fund_id"], detail=a["detail"], ref_url=a["ref_url"],
        )
        if is_new:
            new_alerts.append(a)
            print(f"  [NEW] {a['kind']:13} {a['title']}")

    if not args.dry_run:
        print(f"{len(new_alerts)} new alert(s) written to feed.")
        notify(new_alerts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
