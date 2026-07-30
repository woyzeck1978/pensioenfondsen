"""Controleer voor álle fondsen de cijfers die de diepteanalyse-pagina toont.

De pagina zelf waarschuwt al bij een fonds dat je opent (AUM en actuele
dekkingsgraad, fondsentabel versus jaarverslagjaar). Dit script draait diezelfde
vergelijking over de hele set, plus controles die je met één fonds op je scherm
niet ziet: tellen de deelnemersgroepen op tot het totaal, sommeren de
allocaties tot 100%, staat de beleidsdekkingsgraad in de buurt van DNB.

Elke bevinding is een regel: fonds, controle, waarden, en hoe erg het is.

  hoog   — intern tegenstrijdig of onmogelijk; vrijwel zeker fout
  midden — wijkt af van een onafhankelijke bron (jaarverslag, DNB)
  laag   — ontbrekend kernveld; de pagina toont daar een streepje

Gebruik:
  python3 scripts/utils_and_viz/audit_deep_dive.py
  python3 scripts/utils_and_viz/audit_deep_dive.py --ernst hoog
  python3 scripts/utils_and_viz/audit_deep_dive.py --csv rapport.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Drempels. Dezelfde als de pagina gebruikt waar die al iets controleert, zodat
# een fonds dat hier opduikt ook op het scherm een waarschuwing krijgt.
DREMPEL_AUM_REL = 0.05         # 5% i.p.v. een vast bedrag
DREMPEL_DGR_PP = 1.0
DREMPEL_ZW_PP = 5.0
DREMPEL_DNB_PP = 5.0
DREMPEL_DEELNEMERS = 0.01      # 1% speling op de optelling
DREMPEL_ALLOCATIE_PP = 5.0
DGR_MIN, DGR_MAX = 60.0, 300.0   # HAL staat volgens DNB op 257,6% en is echt
AUM_MAX_BN = 600.0


def _bevinding(uit, fonds, ernst, controle, detail):
    uit.append({"fonds": fonds, "ernst": ernst, "controle": controle, "detail": detail})


def controleer(con) -> pd.DataFrame:
    funds = pd.read_sql_query("SELECT * FROM funds", con)
    hist = pd.read_sql_query("SELECT * FROM historical_metrics", con)
    dnb = pd.read_sql_query(
        """
        SELECT fund_id, value, year, quarter FROM dnb_quarterly_metrics
        WHERE metric_name LIKE 'Beleidsdekkingsgraad%'
        """, con)
    if not dnb.empty:
        dnb = (dnb.sort_values(["fund_id", "year", "quarter"])
                  .groupby("fund_id").tail(1).set_index("fund_id"))

    uit = []
    for _, f in funds.iterrows():
        naam = f["name"]
        fid = f["id"]

        # --- 1. Optellen de deelnemersgroepen tot het getoonde totaal?
        delen = [f["deelnemers_actief"], f["deelnemers_slapers"], f["deelnemers_gepensioneerd"]]
        if pd.notnull(f["deelnemers_totaal"]) and all(pd.notnull(d) for d in delen):
            som = sum(delen)
            tot = f["deelnemers_totaal"]
            if tot and abs(som - tot) / tot > DREMPEL_DEELNEMERS:
                _bevinding(uit, naam, "hoog", "deelnemers tellen niet op",
                           f"actief+slapers+gepensioneerd = {som:,.0f} vs totaal {tot:,.0f}"
                           .replace(",", "."))

        # --- 2. Sommeren de allocaties tot ongeveer 100%?
        alloc = {"aandelen": f["equity_allocation_pct"], "vastrentend": f["fixed_income_pct"],
                 "vastgoed": f["real_estate_pct"], "alternatives": f["alternatives_pct"]}
        gevuld = {k: v for k, v in alloc.items() if pd.notnull(v)}
        if len(gevuld) == 4:
            som = sum(gevuld.values())
            if abs(som - 100.0) > DREMPEL_ALLOCATIE_PP:
                _bevinding(uit, naam, "hoog", "allocatie telt niet op tot 100%",
                           f"{som:.1f}% — " + ", ".join(f"{k} {v:.1f}" for k, v in gevuld.items()))

        # --- 3. Dekkingsgraden binnen een mogelijk bereik?
        for kol, label in [("dekkingsgraad_pct", "actuele dekkingsgraad"),
                           ("beleidsdekkingsgraad_pct", "beleidsdekkingsgraad"),
                           ("vereiste_dekkingsgraad_pct", "vereiste dekkingsgraad")]:
            w = f[kol]
            if pd.notnull(w) and not (DGR_MIN <= w <= DGR_MAX):
                _bevinding(uit, naam, "hoog", f"{label} onmogelijk", f"{w:.1f}%")

        if pd.notnull(f["aum_euro_bn"]) and (f["aum_euro_bn"] <= 0 or f["aum_euro_bn"] > AUM_MAX_BN):
            _bevinding(uit, naam, "hoog", "AUM onmogelijk", f"{f['aum_euro_bn']:.1f} mrd")

        # --- 4. Actueel versus beleid: beleid is een 12-maandsgemiddelde en
        #        kan niet ver van de actuele stand af liggen.
        a, b = f["dekkingsgraad_pct"], f["beleidsdekkingsgraad_pct"]
        if pd.notnull(a) and pd.notnull(b) and abs(a - b) > 25:
            _bevinding(uit, naam, "midden", "actueel en beleid liggen ver uiteen",
                       f"actueel {a:.1f}% vs beleid {b:.1f}%")

        # --- 5. Fondsentabel versus het jaarverslagjaar (wat de pagina naast
        #        elkaar zet in het tabblad Kerncijfers).
        fh = hist[hist["fund_id"] == fid]
        if not fh.empty:
            laatste = fh.loc[fh["year"].idxmax()]
            jr = int(laatste["year"])
            # AUM relatief vergelijken: 0,5 mrd verschil is bij een fonds van
            # 542 mrd ruis en bij een fonds van 1 mrd een halvering.
            wf, wh = f["aum_euro_bn"], laatste["aum_euro_bn"]
            if pd.notnull(wf) and pd.notnull(wh) and wh:
                if abs(wf - wh) / wh > DREMPEL_AUM_REL:
                    _bevinding(uit, naam, "midden", f"AUM wijkt af van FY{jr}",
                               f"fondsentabel {wf:.1f} mrd vs jaarverslag {wh:.1f} mrd "
                               f"({(wf - wh) / wh * 100:+.0f}%)")

            wf, wh = f["dekkingsgraad_pct"], laatste["nominale_dekkingsgraad_pct"]
            if pd.notnull(wf) and pd.notnull(wh) and abs(wf - wh) > DREMPEL_DGR_PP:
                _bevinding(uit, naam, "midden", f"actuele dekkingsgraad wijkt af van FY{jr}",
                           f"fondsentabel {wf:.1f}% vs jaarverslag {wh:.1f}%")

            # zakelijke_waarden_pct is de DNB-noemer: aandelen plus vastgoed plus
            # alternatives. Rechtstreeks tegen equity_allocation_pct leggen
            # vergelijkt appels met peren — dat gaf bij bijna elk fonds vals alarm.
            zw = laatste["zakelijke_waarden_pct"]
            delen_zw = [f["equity_allocation_pct"], f["real_estate_pct"], f["alternatives_pct"]]
            if pd.notnull(zw) and all(pd.notnull(d) for d in delen_zw):
                som_zw = sum(delen_zw)
                if abs(som_zw - zw) > DREMPEL_ZW_PP:
                    _bevinding(uit, naam, "midden", f"zakelijke waarden wijken af van FY{jr}",
                               f"aandelen+vastgoed+alternatives {som_zw:.1f}% vs "
                               f"jaarverslag {zw:.1f}%")

            # Sprongen tussen opeenvolgende jaren die op een tikfout wijzen.
            reeks = fh.sort_values("year")
            vorige = None
            for _, r in reeks.iterrows():
                w = r["nominale_dekkingsgraad_pct"]
                if pd.notnull(w) and vorige is not None and abs(w - vorige[1]) > 30:
                    _bevinding(uit, naam, "midden", "dekkingsgraad springt tussen jaren",
                               f"{int(vorige[0])}: {vorige[1]:.1f}% → {int(r['year'])}: {w:.1f}%")
                if pd.notnull(w):
                    vorige = (r["year"], w)

        # --- 6. Beleidsdekkingsgraad versus de laatste DNB-kwartaalstand.
        if not dnb.empty and fid in dnb.index and pd.notnull(f["beleidsdekkingsgraad_pct"]):
            d = dnb.loc[fid]
            if pd.notnull(d["value"]) and abs(f["beleidsdekkingsgraad_pct"] - d["value"]) > DREMPEL_DNB_PP:
                _bevinding(uit, naam, "midden", "beleidsdekkingsgraad wijkt af van DNB",
                           f"fondsentabel {f['beleidsdekkingsgraad_pct']:.1f}% vs DNB "
                           f"{d['value']:.1f}% ({int(d['year'])}Q{int(d['quarter'])})")

        # --- 7. Kernvelden die de pagina als streepje toont.
        ontbreekt = [label for kol, label in [
            ("aum_euro_bn", "AUM"), ("dekkingsgraad_pct", "dekkingsgraad"),
            ("deelnemers_totaal", "deelnemers"), ("uitvoerder", "uitvoerder"),
        ] if pd.isnull(f[kol]) or (isinstance(f[kol], str) and not f[kol].strip())]
        if ontbreekt:
            _bevinding(uit, naam, "laag", "kernveld leeg", ", ".join(ontbreekt))

    return pd.DataFrame(uit)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ernst", choices=["hoog", "midden", "laag"],
                    help="toon alleen deze ernst")
    ap.add_argument("--csv", help="schrijf de bevindingen ook naar dit bestand")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    df = controleer(con)
    n_fondsen = con.execute("SELECT COUNT(*) FROM funds").fetchone()[0]
    con.close()

    if df.empty:
        print(f"{n_fondsen} fondsen gecontroleerd — niets gevonden.")
        return 0

    if args.ernst:
        df = df[df["ernst"] == args.ernst]

    volgorde = {"hoog": 0, "midden": 1, "laag": 2}
    df = df.sort_values(["ernst", "fonds"], key=lambda s: s.map(volgorde) if s.name == "ernst" else s)

    tellen = df["ernst"].value_counts().to_dict()
    print(f"{n_fondsen} fondsen gecontroleerd — {len(df)} bevindingen "
          f"({tellen.get('hoog', 0)} hoog, {tellen.get('midden', 0)} midden, "
          f"{tellen.get('laag', 0)} laag)\n")

    huidige = None
    for _, r in df.iterrows():
        if r["ernst"] != huidige:
            huidige = r["ernst"]
            print(f"\n── {huidige.upper()} " + "─" * 60)
        print(f"  {r['fonds'][:38]:<39} {r['controle'][:36]:<37} {r['detail']}")

    if args.csv:
        df.to_csv(args.csv, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"\nWeggeschreven naar {args.csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
