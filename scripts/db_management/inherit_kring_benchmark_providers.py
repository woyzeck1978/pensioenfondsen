"""Erf benchmark_providers voor APF-kringen zonder eigen jaarverslag-data.

Kringen publiceren zelden eigen jaarverslagen; het beleggingsraamwerk
(en dus de benchmark-providers) komt van het APF-platform. Regels, bewust
conservatief:

- HNP-kringen   : modale providerset van HNP-zusterkringen, alleen als
                  >= 2 zusterkringen exact dezelfde set hebben.
- Stap-kringen  : providerset van moeder Stap APF (id 67).
- Unilever-kringen (Forward/Progress): van moeder Unilever APF (id 68).
- De Nationale-kringen: OVERGESLAGEN — zusterkringen spreken elkaar tegen.

Geërfde waarden zijn een aanname, geen extractie. Daarom wordt de bron
vastgelegd in een nieuwe kolom funds.benchmark_providers_bron
('kring-overerving (…)'); direct geëxtraheerde rijen houden daar NULL.

NULL-only: bestaande benchmark_providers worden nooit overschreven.
Rollback: UPDATE funds SET benchmark_providers=NULL,
          benchmark_providers_bron=NULL WHERE benchmark_providers_bron IS NOT NULL;
"""
import argparse
import os
import sqlite3
from collections import Counter

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # HNP: modale set onder zusterkringen met data
    hnp_sets = [r[0] for r in cur.execute(
        "SELECT benchmark_providers FROM funds "
        "WHERE (name LIKE '%(Hnp)%' OR name LIKE '%(HNPF)%') "
        "AND benchmark_providers IS NOT NULL")]
    hnp_mode, hnp_n = (Counter(hnp_sets).most_common(1)[0]
                       if hnp_sets else (None, 0))

    stap_parent = cur.execute(
        "SELECT benchmark_providers FROM funds WHERE name = 'Stap APF'").fetchone()
    unilever_parent = cur.execute(
        "SELECT benchmark_providers FROM funds WHERE name = 'Unilever APF'").fetchone()

    plan = []  # (id, name, value, bron)
    if hnp_mode and hnp_n >= 2:
        for fid, name in cur.execute(
                "SELECT id, name FROM funds WHERE (name LIKE '%(Hnp)%' OR name LIKE '%(HNPF)%') "
                "AND benchmark_providers IS NULL"):
            plan.append((fid, name, hnp_mode,
                         f"kring-overerving (modale set van {hnp_n} HNP-zusterkringen)"))
    if stap_parent and stap_parent[0]:
        for fid, name in cur.execute(
                "SELECT id, name FROM funds WHERE name LIKE '%(Stap)%' "
                "AND benchmark_providers IS NULL"):
            plan.append((fid, name, stap_parent[0],
                         "kring-overerving (moeder Stap APF)"))
    if unilever_parent and unilever_parent[0]:
        for fid, name in cur.execute(
                "SELECT id, name FROM funds WHERE name LIKE '%(Unilever)%' "
                "AND benchmark_providers IS NULL"):
            plan.append((fid, name, unilever_parent[0],
                         "kring-overerving (moeder Unilever APF)"))

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== {mode} — kring-overerving benchmark_providers ===")
    print(f"HNP modale set ({hnp_n}x identiek): {hnp_mode}")
    print(f"Stap APF: {stap_parent[0] if stap_parent else '—'}")
    print(f"Unilever APF: {unilever_parent[0] if unilever_parent else '—'}\n")
    for fid, name, val, bron in plan:
        print(f"  id={fid:<4} {name[:42]:44s} -> {val}")
        print(f"          bron: {bron}")
    print(f"\n{len(plan)} kringen. De Nationale-kringen overgeslagen (inconsistent bewijs).")

    if not args.apply:
        print("Dry-run: er is NIETS geschreven.")
        con.close()
        return

    have_col = any(r[1] == "benchmark_providers_bron"
                   for r in cur.execute("PRAGMA table_info(funds)"))
    if not have_col:
        cur.execute("ALTER TABLE funds ADD COLUMN benchmark_providers_bron TEXT")
    for fid, _, val, bron in plan:
        cur.execute(
            "UPDATE funds SET benchmark_providers = ?, benchmark_providers_bron = ? "
            "WHERE id = ? AND benchmark_providers IS NULL", (val, bron, fid))
    con.commit()
    con.close()
    print(f"Klaar: {len(plan)} rijen geschreven (NULL-only, met bron-markering).")


if __name__ == "__main__":
    main()
