"""Three small extras for historical_metrics:

  1) Copy funds.deelnemers_* (current snapshot) into the most-recent
     historical_metrics row per fund, NULL-only guard. Heuristic but
     gives the multi-year overview at least one filled column for the
     fund's right-most year — way better than empty.

  2) Fill cpi_pct uniformly per year from a hardcoded CBS NL table.
     CPI is the same number for every fund in a given year.

  3) Pivot funds.toeslag_actieven_YYYY_pct -> historical_metrics
     indexatieverlening_pct for each fund×year where it exists.

Run from scripts/db_management/:
    python3 backfill_history_extras.py            # apply
    python3 backfill_history_extras.py --dry-run
"""

import argparse
import re
import sqlite3

DB_PATH = "../../data/processed/pension_funds.db"

# CBS Statline NL CPI yearly average (% change y/y, source: CBS Statline,
# table 70936ned). 2025 is provisional. Constants — same for every fund.
CPI_NL = {
    2015: 0.6,
    2016: 0.3,
    2017: 1.4,
    2018: 1.7,
    2019: 2.6,
    2020: 1.3,
    2021: 2.7,
    2022: 10.0,
    2023: 3.8,
    2024: 3.3,
    2025: 3.1,
}

# Sanity: skip funds.deelnemers_actief values below this — they are
# almost certainly stubs / typos (e.g. ABN's '44' active).
MIN_ACTIEF = 100


def step1_deelnemers(con, dry_run: bool) -> dict:
    """Copy funds.deelnemers_* to the most-recent historical row per fund."""
    cur = con.cursor()
    # For each fund: find its max year in historical_metrics, and the values
    # in funds + the NULL state in that historical row
    rows = cur.execute("""
        WITH max_year AS (
            SELECT fund_id, MAX(year) AS y
            FROM historical_metrics
            GROUP BY fund_id
        )
        SELECT my.fund_id, my.y, f.deelnemers_actief, f.deelnemers_slapers,
               f.deelnemers_gepensioneerd, f.deelnemers_totaal,
               h.deelnemers_actief, h.deelnemers_slapers,
               h.deelnemers_pensioengerechtigd, h.deelnemers_totaal
        FROM max_year my
        JOIN funds f ON f.id = my.fund_id
        JOIN historical_metrics h ON h.fund_id = my.fund_id AND h.year = my.y
    """).fetchall()

    n_act = n_sla = n_gep = n_tot = 0
    skipped_actief = 0
    for (fid, year, f_a, f_s, f_g, f_t, h_a, h_s, h_g, h_t) in rows:
        updates, params = [], []
        # actief: only copy if it looks plausible
        if f_a is not None and h_a is None:
            if f_a >= MIN_ACTIEF:
                updates.append("deelnemers_actief = ?"); params.append(f_a); n_act += 1
            else:
                skipped_actief += 1
        if f_s is not None and h_s is None:
            updates.append("deelnemers_slapers = ?"); params.append(f_s); n_sla += 1
        if f_g is not None and h_g is None:
            updates.append("deelnemers_pensioengerechtigd = ?"); params.append(f_g); n_gep += 1
        if f_t is not None and h_t is None:
            updates.append("deelnemers_totaal = ?"); params.append(f_t); n_tot += 1
        if updates and not dry_run:
            params.extend([fid, year])
            cur.execute(
                f"UPDATE historical_metrics SET {', '.join(updates)} WHERE fund_id=? AND year=?",
                params,
            )
    return {"actief": n_act, "slapers": n_sla, "gepens": n_gep, "totaal": n_tot,
            "skipped_actief_too_low": skipped_actief}


def step2_cpi(con, dry_run: bool) -> int:
    cur = con.cursor()
    filled = 0
    for year, cpi in CPI_NL.items():
        c = cur.execute(
            "UPDATE historical_metrics SET cpi_pct = ? "
            "WHERE year = ? AND cpi_pct IS NULL",
            (cpi, year),
        ) if not dry_run else cur.execute(
            "SELECT COUNT(*) FROM historical_metrics WHERE year = ? AND cpi_pct IS NULL",
            (year,),
        )
        filled += c.rowcount if not dry_run else c.fetchone()[0]
    return filled


def step3_indexatie(con, dry_run: bool) -> int:
    """Pivot funds.toeslag_*_YYYY_pct or toeslag_actieven_YYYY_pct into history."""
    cur = con.cursor()
    cols = [r[1] for r in cur.execute("PRAGMA table_info(funds)").fetchall()]
    # Match toeslag_actieven_2026_pct, toeslag_2024_pct, etc.
    pat = re.compile(r"^toeslag_(?:actieven_)?(20\d{2})_pct$")
    year_cols: dict[int, list[str]] = {}
    for c in cols:
        m = pat.match(c)
        if m:
            year_cols.setdefault(int(m.group(1)), []).append(c)
    if not year_cols:
        return 0

    filled = 0
    for year, col_list in sorted(year_cols.items()):
        # Prefer the 'actieven' variant if present, otherwise the bare _YYYY column
        col = next((c for c in col_list if "actieven" in c), col_list[0])
        rows = cur.execute(f"SELECT id, {col} FROM funds WHERE {col} IS NOT NULL").fetchall()
        for fid, val in rows:
            if val is None:
                continue
            try:
                v = float(val)
            except Exception:
                continue
            if not (-5 <= v <= 25):  # sanity: NL indexation rarely outside this
                continue
            c = cur.execute(
                "UPDATE historical_metrics SET indexatieverlening_pct = ? "
                "WHERE fund_id = ? AND year = ? AND indexatieverlening_pct IS NULL",
                (v, fid, year),
            ) if not dry_run else cur.execute(
                "SELECT 1 FROM historical_metrics WHERE fund_id=? AND year=? AND indexatieverlening_pct IS NULL",
                (fid, year),
            )
            filled += c.rowcount if not dry_run else (1 if c.fetchone() else 0)
    return filled


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    print(f"=== step 1: deelnemers from funds -> most-recent historical row ===")
    s1 = step1_deelnemers(con, args.dry_run)
    print(f"  fills: actief+={s1['actief']} slapers+={s1['slapers']} "
          f"gepens+={s1['gepens']} totaal+={s1['totaal']}")
    print(f"  skipped (funds.deelnemers_actief < {MIN_ACTIEF}): {s1['skipped_actief_too_low']}")

    print(f"\n=== step 2: cpi_pct from CBS Statline table ===")
    n2 = step2_cpi(con, args.dry_run)
    print(f"  cpi_pct cells {'would be filled' if args.dry_run else 'filled'}: {n2}")

    print(f"\n=== step 3: indexatie from funds.toeslag_*_YYYY_pct ===")
    n3 = step3_indexatie(con, args.dry_run)
    print(f"  indexatieverlening_pct cells {'would be filled' if args.dry_run else 'filled'}: {n3}")

    if not args.dry_run:
        con.commit()
        print(f"\n[ok] commit done.")
    else:
        print(f"\n[dry-run] no DB changes made.")
    con.close()


if __name__ == "__main__":
    main()
