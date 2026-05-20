"""Backfill historical_metrics from dnb_quarterly_metrics.

For every fund × year that DNB reports, map:
  - Q4 Beleidsdekkingsgraad             -> beleidsdekkingsgraad_pct
  - Q4 Beleggingen voor risico fonds    -> aum_euro_bn  (value in 1000 EUR -> Bn EUR)
  - sum of Q1..Q4 Kwartaalrendement     -> beleggingsrendement_pct
                                           (true compound is product, but sum is
                                            within ~0.3 pp for typical ranges and
                                            matches DNB's own reporting style)

Insert a new historical_metrics row when (fund_id, year) doesn't exist
yet; otherwise only fill NULL cells (never overwrite). Reject obviously
broken values (rendement outside -50..50 pp, AUM <= 0, beleidsdg outside
50..250).

Run from scripts/db_management/:
    python3 backfill_historical_from_dnb.py            # apply
    python3 backfill_historical_from_dnb.py --dry-run
"""

import argparse
import sqlite3
from collections import defaultdict

DB_PATH = "../../data/processed/pension_funds.db"
AUM_DIVISOR = 1_000_000  # DNB Beleggingen is in 1000 EUR -> /1e6 -> Bn EUR


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Pre counts
    pre_rows = cur.execute("SELECT COUNT(*) FROM historical_metrics").fetchone()[0]
    pre_aum = cur.execute("SELECT COUNT(*) FROM historical_metrics WHERE aum_euro_bn IS NOT NULL").fetchone()[0]
    pre_beleids = cur.execute("SELECT COUNT(*) FROM historical_metrics WHERE beleidsdekkingsgraad_pct IS NOT NULL").fetchone()[0]
    pre_rend = cur.execute("SELECT COUNT(*) FROM historical_metrics WHERE beleggingsrendement_pct IS NOT NULL").fetchone()[0]
    print(f"[pre] historical_metrics rows={pre_rows}  "
          f"aum={pre_aum}  beleids={pre_beleids}  rend={pre_rend}")

    # Pull all DNB quarterly metrics that map to our targets
    rows = cur.execute("""
        SELECT fund_id, year, quarter, metric_name, value
        FROM dnb_quarterly_metrics
        WHERE metric_name IN (
            'Beleidsdekkingsgraad',
            'Vereiste dekkingsgraad (strategisch) (in percentages)',
            'Beleggingen voor risico fonds',
            'Kwartaalrendement beleggingen risico fonds (in percentages)'
        )
        AND value IS NOT NULL
    """).fetchall()

    # Aggregate per (fund, year)
    bucket: dict[tuple[int, int], dict] = defaultdict(
        lambda: {"beleids_q4": None, "vereiste_q4": None, "aum_q4": None, "rend_quarters": {}}
    )
    for fid, year, q, metric, value in rows:
        key = (fid, year)
        if metric == "Beleidsdekkingsgraad" and q == 4:
            bucket[key]["beleids_q4"] = value
        elif metric == "Vereiste dekkingsgraad (strategisch) (in percentages)" and q == 4:
            bucket[key]["vereiste_q4"] = value
        elif metric == "Beleggingen voor risico fonds" and q == 4:
            bucket[key]["aum_q4"] = value
        elif metric == "Kwartaalrendement beleggingen risico fonds (in percentages)":
            bucket[key]["rend_quarters"][q] = value

    # Existing (fund, year) in historical_metrics so we know what to UPDATE vs INSERT
    existing: dict[tuple[int, int], dict] = {}
    for row in cur.execute(
        "SELECT fund_id, year, aum_euro_bn, beleidsdekkingsgraad_pct, "
        "beleggingsrendement_pct, vereiste_dekkingsgraad_pct "
        "FROM historical_metrics"
    ):
        existing[(row[0], row[1])] = {
            "aum_euro_bn": row[2],
            "beleidsdekkingsgraad_pct": row[3],
            "beleggingsrendement_pct": row[4],
            "vereiste_dekkingsgraad_pct": row[5],
        }

    new_rows = 0
    fill_aum = fill_beleids = fill_rend = fill_vereiste = 0
    rejected_rend = rejected_aum = rejected_beleids = rejected_vereiste = 0

    for (fid, year), b in sorted(bucket.items()):
        # Compute candidate values + sanity check
        beleids = b["beleids_q4"]
        if beleids is not None:
            if not (50 <= beleids <= 250):
                rejected_beleids += 1
                beleids = None

        vereiste = b["vereiste_q4"]
        if vereiste is not None:
            if not (100 <= vereiste <= 150):  # typical NL FTK range is 105-130
                rejected_vereiste += 1
                vereiste = None

        aum_bn = None
        if b["aum_q4"] is not None:
            aum_bn = b["aum_q4"] / AUM_DIVISOR
            if not (0 < aum_bn < 1000):
                rejected_aum += 1
                aum_bn = None
            else:
                aum_bn = round(aum_bn, 3)

        rend = None
        qs = b["rend_quarters"]
        if len(qs) == 4:
            rend = round(sum(qs.values()), 2)
            if not (-50 <= rend <= 50):
                rejected_rend += 1
                rend = None

        if all(v is None for v in (beleids, vereiste, aum_bn, rend)):
            continue

        if (fid, year) in existing:
            ex = existing[(fid, year)]
            updates = []
            params = []
            if aum_bn is not None and ex["aum_euro_bn"] is None:
                updates.append("aum_euro_bn = ?"); params.append(aum_bn); fill_aum += 1
            if beleids is not None and ex["beleidsdekkingsgraad_pct"] is None:
                updates.append("beleidsdekkingsgraad_pct = ?"); params.append(beleids); fill_beleids += 1
            if vereiste is not None and ex["vereiste_dekkingsgraad_pct"] is None:
                updates.append("vereiste_dekkingsgraad_pct = ?"); params.append(vereiste); fill_vereiste += 1
            if rend is not None and ex["beleggingsrendement_pct"] is None:
                updates.append("beleggingsrendement_pct = ?"); params.append(rend); fill_rend += 1
            if updates and not args.dry_run:
                params.extend([fid, year])
                cur.execute(
                    f"UPDATE historical_metrics SET {', '.join(updates)} WHERE fund_id=? AND year=?",
                    params,
                )
        else:
            new_rows += 1
            if aum_bn is not None: fill_aum += 1
            if beleids is not None: fill_beleids += 1
            if vereiste is not None: fill_vereiste += 1
            if rend is not None: fill_rend += 1
            if not args.dry_run:
                cur.execute("""
                    INSERT INTO historical_metrics
                    (fund_id, year, aum_euro_bn, beleidsdekkingsgraad_pct,
                     vereiste_dekkingsgraad_pct, beleggingsrendement_pct)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (fid, year, aum_bn, beleids, vereiste, rend))

    if not args.dry_run:
        con.commit()

    print(f"\n[plan]   new (fund,year) rows: {new_rows}")
    print(f"[plan]   fills: aum+={fill_aum}  beleids+={fill_beleids}  "
          f"vereiste+={fill_vereiste}  rend+={fill_rend}")
    print(f"[plan]   rejected (sanity): aum={rejected_aum} beleids={rejected_beleids} "
          f"vereiste={rejected_vereiste} rend={rejected_rend}")

    if args.dry_run:
        print("\n[dry-run] no DB changes made.")
    else:
        post_rows = cur.execute("SELECT COUNT(*) FROM historical_metrics").fetchone()[0]
        post_aum = cur.execute("SELECT COUNT(*) FROM historical_metrics WHERE aum_euro_bn IS NOT NULL").fetchone()[0]
        post_beleids = cur.execute("SELECT COUNT(*) FROM historical_metrics WHERE beleidsdekkingsgraad_pct IS NOT NULL").fetchone()[0]
        post_rend = cur.execute("SELECT COUNT(*) FROM historical_metrics WHERE beleggingsrendement_pct IS NOT NULL").fetchone()[0]
        print(f"\n[post]  historical_metrics rows={post_rows} (+{post_rows-pre_rows})")
        print(f"        aum={post_aum} (+{post_aum-pre_aum})  "
              f"beleids={post_beleids} (+{post_beleids-pre_beleids})  "
              f"rend={post_rend} (+{post_rend-pre_rend})")

    con.close()


if __name__ == "__main__":
    main()
