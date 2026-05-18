"""Normaliseer wtp_invaren en wtp_transitie_datum in de funds-tabel.

Vóór deze stap zit er in beide kolommen een rommeltje van vrije tekst:
'ja', 'Ja', '1-jan-26', '?', 'Uitgesteld', '2027 (juli)', 'nvt', enz.
Na de stap is:

  wtp_invaren           ∈ {'ja', 'nee', 'uitgesteld', NULL}
  wtp_transitie_datum   ISO YYYY-MM-DD of NULL

Datumstrings die per ongeluk in wtp_invaren stonden ('1-jan-26' etc.)
worden overgezet naar wtp_transitie_datum als die kolom voor dat fonds
nog leeg is. Sentinels (?/nvt/onbekend/blank) worden NULL.

Run vanuit scripts/db_management/ zodat het relatieve DB-pad klopt.

Gebruik:
    python3 normalize_wtp_fields.py            # voert UPDATEs uit
    python3 normalize_wtp_fields.py --dry-run  # logt alleen wat zou veranderen
"""

import argparse
import re
import sqlite3
from collections import Counter

DB_PATH = "../../data/processed/pension_funds.db"

DUTCH_MONTHS = {
    "jan": "01", "feb": "02", "mrt": "03", "mar": "03", "apr": "04",
    "mei": "05", "may": "05", "jun": "06", "jul": "07",
    "aug": "08", "sep": "09", "sept": "09", "okt": "10", "oct": "10",
    "nov": "11", "dec": "12",
}

NULL_SENTINELS = {"", "?", "nvt", "n.v.t.", "onbekend", "unknown", "geen", "n/a", "tbd"}


def normalize_invaren(raw: str) -> tuple[str | None, str | None]:
    """Return (invaren_value, suggested_date_iso).

    The suggested_date is non-None only when the raw value was a date string
    that doesn't belong in wtp_invaren — caller may move it to
    wtp_transitie_datum.
    """
    if raw is None:
        return None, None
    s = raw.strip()
    if s.lower() in NULL_SENTINELS:
        return None, None
    # Plain enum values
    low = s.lower()
    if low in {"ja", "yes", "true", "ingevaren", "done"}:
        return "ja", None
    if low in {"nee", "no", "false", "niet"}:
        return "nee", None
    if low.startswith("uitgesteld") or "postponed" in low:
        return "uitgesteld", None
    # Date-like strings sometimes ended up here
    iso = parse_to_iso(s)
    if iso:
        # Move the date out; leave invaren NULL until evidence says ja/nee.
        return None, iso
    return None, None


def parse_to_iso(raw: str) -> str | None:
    """Convert a Dutch-flavoured date string to YYYY-MM-DD or YYYY-01-01.

    Returns None for non-parseable strings.
    """
    if raw is None:
        return None
    s = raw.strip().lower()
    if s in NULL_SENTINELS:
        return None
    # Already ISO YYYY-MM-DD
    m = re.match(r"^(20\d{2})-(\d{2})-(\d{2})$", s)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    # YYYY-MM (no day)
    m = re.match(r"^(20\d{2})-(\d{1,2})$", s)
    if m:
        y, mo = m.group(1), m.group(2)
        return f"{y}-{int(mo):02d}-01"
    # YYYY only
    if re.match(r"^20\d{2}$", s):
        return f"{s}-01-01"
    # YYYY (dutch month) — '2027 (juli)'
    m = re.match(r"^(20\d{2})\s*\(([a-zA-Z]+)\)$", s)
    if m:
        y, mo_name = m.group(1), m.group(2)
        mo = DUTCH_MONTHS.get(mo_name[:4]) or DUTCH_MONTHS.get(mo_name[:3])
        if mo:
            return f"{y}-{mo}-01"
    # D-mon-YY  or  DD-mon-YYYY
    m = re.match(r"^(\d{1,2})[-\s]+([a-zA-Z]+)[-\s]+(\d{2,4})$", s)
    if m:
        d, mo_name, y = m.group(1), m.group(2), m.group(3)
        mo = DUTCH_MONTHS.get(mo_name[:4]) or DUTCH_MONTHS.get(mo_name[:3])
        if mo:
            year = int(y)
            if year < 100:
                year += 2000
            return f"{year}-{mo}-{int(d):02d}"
    # DD-MM-YYYY
    m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](20\d{2})$", s)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    # "januari 2026"
    m = re.match(r"^([a-zA-Z]+)\s+(20\d{2})$", s)
    if m:
        mo_name, y = m.group(1), m.group(2)
        mo = DUTCH_MONTHS.get(mo_name[:4]) or DUTCH_MONTHS.get(mo_name[:3])
        if mo:
            return f"{y}-{mo}-01"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    rows = cur.execute(
        "SELECT id, name, wtp_invaren, wtp_transitie_datum FROM funds"
    ).fetchall()

    invaren_before: Counter[str] = Counter()
    invaren_after: Counter[str] = Counter()
    date_before: Counter[str] = Counter()
    date_after: Counter[str] = Counter()
    moved_count = 0
    examples: list[str] = []

    updates: list[tuple[str | None, str | None, int]] = []

    for fid, name, inv_raw, date_raw in rows:
        invaren_before[inv_raw if inv_raw else "NULL"] += 1
        date_before[date_raw if date_raw else "NULL"] += 1

        new_inv, moved_date = normalize_invaren(inv_raw)
        new_date = parse_to_iso(date_raw) if date_raw else None

        # If invaren had a date but date column is empty, move it
        if moved_date and not new_date:
            new_date = moved_date
            moved_count += 1
            if len(examples) < 8:
                examples.append(
                    f"  fid={fid:>3} {name[:30]:30}  invaren {inv_raw!r:18} -> NULL, "
                    f"datum {date_raw!r:18} -> {new_date}"
                )

        invaren_after[new_inv if new_inv else "NULL"] += 1
        date_after[new_date if new_date else "NULL"] += 1

        if (new_inv != inv_raw) or (new_date != (date_raw if date_raw else None)):
            updates.append((new_inv, new_date, fid))

    # Reporting
    print("=" * 70)
    print(f"  wtp_invaren — before -> after")
    print("=" * 70)
    keys = sorted(set(invaren_before) | set(invaren_after), key=lambda k: -max(invaren_before[k], invaren_after[k]))
    for k in keys:
        print(f"  {str(k)[:24]:24}  {invaren_before[k]:>4}  ->  {invaren_after[k]:>4}")
    print()
    print("=" * 70)
    print(f"  wtp_transitie_datum — before -> after (top 15)")
    print("=" * 70)
    keys = sorted(set(date_before) | set(date_after), key=lambda k: -max(date_before[k], date_after[k]))[:25]
    for k in keys:
        print(f"  {str(k)[:24]:24}  {date_before[k]:>4}  ->  {date_after[k]:>4}")
    print()
    print(f"Cross-moved (invaren date -> datum kolom): {moved_count}")
    if examples:
        print("Sample moves:")
        for e in examples:
            print(e)
    print()
    print(f"Total row updates planned: {len(updates)}")

    if args.dry_run:
        print("\n[dry-run] no DB changes made.")
        con.close()
        return

    for new_inv, new_date, fid in updates:
        cur.execute(
            "UPDATE funds SET wtp_invaren=?, wtp_transitie_datum=? WHERE id=?",
            (new_inv, new_date, fid),
        )
    con.commit()
    print(f"\n[ok] {len(updates)} rows updated.")
    con.close()


if __name__ == "__main__":
    main()
