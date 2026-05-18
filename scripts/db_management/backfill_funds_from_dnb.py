"""Backfill missing funds.beleidsdekkingsgraad_pct and funds.aum_euro_bn
from dnb_quarterly_metrics (most recent non-NULL quarterly value per fund).

Conservative: only fills NULL cells. Never overwrites existing hand-curated
values, even when DNB has a more recent figure. The diff between funds
and DNB for filled rows is a separate data-quality question.

Run from scripts/db_management/.

Usage:
    python3 backfill_funds_from_dnb.py            # apply
    python3 backfill_funds_from_dnb.py --dry-run
"""

import argparse
import sqlite3

DB_PATH = "../../data/processed/pension_funds.db"

# DNB Beleggingen voor risico fonds is reported in 1000 EUR. Divide by
# 1_000_000 to get billion EUR (because 1 mld = 1e9 EUR = 1e6 × 1000 EUR).
AUM_DIVISOR = 1_000_000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Pre counts
    null_beleids = cur.execute(
        "SELECT COUNT(*) FROM funds WHERE beleidsdekkingsgraad_pct IS NULL"
    ).fetchone()[0]
    null_aum = cur.execute(
        "SELECT COUNT(*) FROM funds WHERE aum_euro_bn IS NULL"
    ).fetchone()[0]
    print(f"[pre] NULL beleidsdg: {null_beleids}  NULL aum_euro_bn: {null_aum}")

    # ── beleidsdekkingsgraad ───────────────────────────────────────
    candidates_b = cur.execute(
        """
        SELECT f.id, f.name, d.value, d.year, d.quarter
        FROM funds f
        JOIN dnb_quarterly_metrics d ON d.fund_id = f.id
        WHERE f.beleidsdekkingsgraad_pct IS NULL
          AND d.metric_name = 'Beleidsdekkingsgraad'
          AND d.value IS NOT NULL
        ORDER BY f.id, d.year DESC, d.quarter DESC
        """
    ).fetchall()
    # Keep first (most recent) per fund
    seen: set[int] = set()
    beleids_updates: list[tuple[float, int]] = []
    beleids_sample: list[tuple[int, str, float, int, int]] = []
    for fid, name, val, y, q in candidates_b:
        if fid in seen:
            continue
        seen.add(fid)
        beleids_updates.append((float(val), fid))
        if len(beleids_sample) < 8:
            beleids_sample.append((fid, name, float(val), y, q))

    print(f"[beleidsdg] will fill {len(beleids_updates)} rows")
    if beleids_sample:
        print("  samples:")
        for fid, name, v, y, q in beleids_sample:
            print(f"    fid={fid:>3}  {name[:35]:35}  -> {v:.1f}%  ({y}Q{q})")

    # ── AUM ────────────────────────────────────────────────────────
    candidates_a = cur.execute(
        """
        SELECT f.id, f.name, d.value, d.year, d.quarter
        FROM funds f
        JOIN dnb_quarterly_metrics d ON d.fund_id = f.id
        WHERE f.aum_euro_bn IS NULL
          AND d.metric_name = 'Beleggingen voor risico fonds'
          AND d.value IS NOT NULL
        ORDER BY f.id, d.year DESC, d.quarter DESC
        """
    ).fetchall()
    seen.clear()
    aum_updates: list[tuple[float, int]] = []
    aum_sample: list[tuple[int, str, float, int, int]] = []
    for fid, name, val, y, q in candidates_a:
        if fid in seen:
            continue
        seen.add(fid)
        aum_bn = round(float(val) / AUM_DIVISOR, 3)
        aum_updates.append((aum_bn, fid))
        if len(aum_sample) < 8:
            aum_sample.append((fid, name, aum_bn, y, q))

    print(f"[aum] will fill {len(aum_updates)} rows")
    if aum_sample:
        print("  samples:")
        for fid, name, v, y, q in aum_sample:
            print(f"    fid={fid:>3}  {name[:35]:35}  -> €{v:.2f} Bn  ({y}Q{q})")

    if args.dry_run:
        print("\n[dry-run] no DB changes made.")
        return

    cur.executemany(
        "UPDATE funds SET beleidsdekkingsgraad_pct = ? WHERE id = ? AND beleidsdekkingsgraad_pct IS NULL",
        beleids_updates,
    )
    cur.executemany(
        "UPDATE funds SET aum_euro_bn = ? WHERE id = ? AND aum_euro_bn IS NULL",
        aum_updates,
    )
    con.commit()

    # Post counts
    null_beleids_after = cur.execute(
        "SELECT COUNT(*) FROM funds WHERE beleidsdekkingsgraad_pct IS NULL"
    ).fetchone()[0]
    null_aum_after = cur.execute(
        "SELECT COUNT(*) FROM funds WHERE aum_euro_bn IS NULL"
    ).fetchone()[0]
    print(f"\n[post] NULL beleidsdg: {null_beleids} -> {null_beleids_after}")
    print(f"[post] NULL aum_euro_bn: {null_aum} -> {null_aum_after}")
    con.close()


if __name__ == "__main__":
    main()
