"""Lees deelnemersaantallen uit de kerncijfertabel van het jaarverslag.

Waarom niet met een regex: die getallen staan in tabellen, en platgeslagen
PDF-tekst maakt daar "404 Totaal aantal deelnemers 6" van. PyMuPDF herkent de
tabel wel als structuur, met kopregel en kolommen, en dan is het eenvoudig.

Wat daarbij aan het licht kwam: de bestaande waarden in historical_metrics zijn
niet zomaar fout, ze komen uit de verkeerde kolom. Thales staat over 2025 op een
totaal van 6.377, en dat is exact de kolom van 2024; het verslag geeft 6.790 voor
2025. Een kerncijfertabel toont vijf jaargangen naast elkaar en wie de kop niet
leest, pakt de verkeerde.

Daarom leest dit script de kopregel en zoekt daarin het gevraagde jaartal, in
plaats van te vertrouwen op de volgorde.

  python3 scripts/db_management/lees_deelnemers_tabel.py --jaar 2025
  python3 scripts/db_management/lees_deelnemers_tabel.py --jaar 2025 --apply
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Rijlabels zoals ze in kerncijfertabellen voorkomen, per veld.
LABELS = {
    "actief": re.compile(r"^\s*(actieve? deelnemers|actieven|deelnemers actief)", re.I),
    "slapers": re.compile(r"^\s*(gewezen deelnemers|slapers|premievrije)", re.I),
    "gepensioneerd": re.compile(r"^\s*(pensioengerechtigden?|ingegane pensioenen|gepensioneerden)", re.I),
    "totaal": re.compile(r"^\s*totaal(\s+aantal)?\s+deelnemers", re.I),
}
# Een regel als '- waarvan arbeidsongeschikt' is een onderverdeling, geen categorie.
SUBRIJ = re.compile(r"^\s*[-•]|waarvan", re.I)


def _getal(cel) -> int | None:
    if cel is None:
        return None
    tekst = str(cel).strip().replace(".", "").replace(" ", "")
    if not re.fullmatch(r"-?\d{2,9}", tekst):
        return None
    n = int(tekst)
    return n if 10 <= n <= 3_000_000 else None


def uit_tabel(pdf_pad: str, jaar: int) -> dict[str, int] | None:
    """Deelnemersaantallen voor dit boekjaar, of None als de tabel niet te vinden is."""
    import fitz

    # Verslagen met een rommelige structuurboom laten MuPDF luid klagen op
    # stderr; die meldingen zeggen niets over de tabelherkenning en verdringen
    # de uitvoer.
    try:
        fitz.TOOLS.mupdf_display_errors(False)
    except Exception:
        pass

    pad = pdf_pad if os.path.isabs(pdf_pad) else os.path.join(BASE_DIR, pdf_pad)
    if not os.path.exists(pad):
        return None
    doc = fitz.open(pad)
    beste = None
    for bladzijde in doc:
        if "deelnemer" not in bladzijde.get_text().lower():
            continue
        try:
            tabellen = bladzijde.find_tables()
        except Exception:
            continue
        for tb in tabellen:
            rijen = tb.extract()
            if len(rijen) < 3:
                continue
            # In welke kolom staat het gevraagde jaar? De kop kan '2025' of
            # '31-12-2025' zijn.
            kolom = None
            for r in rijen[:3]:
                for i, cel in enumerate(r):
                    if cel and re.search(rf"\b{jaar}\b", str(cel)):
                        kolom = i
                        break
                if kolom is not None:
                    break
            if kolom is None:
                continue
            vondst: dict[str, int] = {}
            for r in rijen:
                label = str(r[0] or "")
                if SUBRIJ.search(label):
                    continue
                for veld, patroon in LABELS.items():
                    if veld in vondst or not patroon.search(label):
                        continue
                    if kolom < len(r):
                        n = _getal(r[kolom])
                        if n is not None:
                            vondst[veld] = n
            # De tabel met de meeste velden wint; een kerncijferoverzicht heeft ze alle vier.
            if vondst and (beste is None or len(vondst) > len(beste)):
                beste = vondst
    doc.close()
    return beste


# Tweede route, voor verslagen die hun kerncijfers zonder tabellijnen zetten.
# find_tables() ziet daar niets, maar get_text(sort=True) houdt de kolommen op
# volgorde en dan is de kopregel met jaartallen genoeg om te weten welke waarde
# bij welk jaar hoort.
KOPJAREN = re.compile(r"(20\d\d)")
# Percentages hebben een komma; aantallen niet. Zo blijft '7.497 41,4' te scheiden.
AANTAL = re.compile(r"\b\d{1,3}(?:\.\d{3})+\b|\b\d{2,7}\b")
REGELLABEL = {
    "actief": re.compile(r"^\s*(deelnemers|actieve deelnemers|actieven)\b", re.I),
    "slapers": re.compile(r"^\s*(gewezen )?deelnemers\b", re.I),
    "gepensioneerd": re.compile(r"^\s*pensioengerechtigden\b", re.I),
}


def uit_kerncijfers(pdf_pad: str, jaar: int) -> dict[str, int] | None:
    """Lees het kerncijferblok van een verslag zonder tabellijnen."""
    import fitz

    try:
        fitz.TOOLS.mupdf_display_errors(False)
    except Exception:
        pass
    pad = pdf_pad if os.path.isabs(pdf_pad) else os.path.join(BASE_DIR, pdf_pad)
    if not os.path.exists(pad):
        return None
    doc = fitz.open(pad)
    uit = None
    for bladzijde in doc:
        tekst = bladzijde.get_text()
        if "ewezen deelnemers" not in tekst or "ensioengerechtigden" not in tekst:
            continue
        regels = bladzijde.get_text("text", sort=True).split("\n")
        kolom = None
        vondst: dict[str, int] = {}
        for regel in regels:
            jaren = KOPJAREN.findall(regel)
            # Een kopregel is een rij van louter jaartallen.
            if len(jaren) >= 3 and len(AANTAL.findall(regel)) == len(jaren):
                kolom = jaren.index(str(jaar)) if str(jaar) in jaren else None
                continue
            if kolom is None:
                continue
            getallen = [int(g.replace(".", "")) for g in AANTAL.findall(regel)]
            if len(getallen) <= kolom:
                continue
            for veld in ("gepensioneerd", "slapers", "actief"):
                if veld in vondst or not REGELLABEL[veld].search(regel):
                    continue
                waarde = getallen[kolom]
                if 10 <= waarde <= 3_000_000:
                    vondst[veld] = waarde
                break
        if len(vondst) == 3:
            uit = vondst
            uit["totaal"] = sum(vondst.values())
            break
    doc.close()
    return uit


def lees_alle_jaren(pdf_pad: str, jaren: range) -> dict[int, dict[str, int]]:
    """Alle gevraagde jaargangen in één keer uit hetzelfde verslag.

    De eerste versie riep per jaar uit_tabel() aan, en die opent het document en
    loopt alle pagina's af. Voor vijf jaargangen betekende dat vijf keer een
    verslag van soms duizend pagina's lezen om er telkens één kolom uit te halen.
    Een kerncijfertabel bevat die jaren naast elkaar, dus één doorloop volstaat.
    """
    import fitz

    try:
        fitz.TOOLS.mupdf_display_errors(False)
    except Exception:
        pass
    pad = pdf_pad if os.path.isabs(pdf_pad) else os.path.join(BASE_DIR, pdf_pad)
    if not os.path.exists(pad):
        return {}

    doc = fitz.open(pad)
    bladzijden = []
    for bladzijde in doc:
        tekst = bladzijde.get_text()
        if "deelnemer" in tekst.lower() or "ensioengerechtigden" in tekst:
            bladzijden.append((bladzijde, tekst))
    uit: dict[int, dict[str, int]] = {}
    for jaar in jaren:
        beste = None
        for bladzijde, _tekst in bladzijden:
            try:
                tabellen = bladzijde.find_tables()
            except Exception:
                tabellen = []
            for tb in tabellen:
                vondst = _uit_rijen(tb.extract(), jaar)
                if vondst and (beste is None or len(vondst) > len(beste)):
                    beste = vondst
        if beste:
            uit[jaar] = beste
    doc.close()
    return uit


def _uit_rijen(rijen: list, jaar: int) -> dict[str, int] | None:
    """Zoek in een uitgepakte tabel de kolom van dit jaar en lees de labels."""
    if len(rijen) < 3:
        return None
    kolom = None
    for r in rijen[:3]:
        for i, cel in enumerate(r):
            if cel and re.search(rf"\b{jaar}\b", str(cel)):
                kolom = i
                break
        if kolom is not None:
            break
    if kolom is None:
        return None
    vondst: dict[str, int] = {}
    for r in rijen:
        label = str(r[0] or "")
        if SUBRIJ.search(label):
            continue
        for veld, patroon in LABELS.items():
            if veld in vondst or not patroon.search(label):
                continue
            if kolom < len(r):
                n = _getal(r[kolom])
                if n is not None:
                    vondst[veld] = n
    return vondst or None


def bron_voor(con, fid: int, jaar: int) -> str | None:
    r = con.execute("""SELECT pad FROM ophaal_wachtrij WHERE fund_id=? AND jaar=?
                       AND pad IS NOT NULL""", (fid, jaar)).fetchone()
    if r:
        return r[0]
    g = sorted(glob.glob(os.path.join(BASE_DIR, "data", "annual_reports", f"{fid}_*_{jaar}.pdf")))
    return os.path.relpath(g[0], BASE_DIR) if g else None


def alle_jaren(args) -> int:
    """Vul meerdere jaargangen uit één verslag.

    Een kerncijfertabel toont vijf jaren naast elkaar. Waar de jaarreeks over
    2021 nog maar 24 fondsen met een deelnemersaantal had, staat dat cijfer
    gewoon in het verslag over 2025 — een kolom verderop. Eén bestand lezen vult
    dus vijf rijen, en de kolomkeuze gebeurt op het jaartal in de kopregel, niet
    op volgorde.

    Alleen lege velden worden gevuld; bestaande waarden blijven staan, ook als
    ze afwijken. Wat afwijkt wordt gemeld.
    """
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    # Kringen overslaan. Hun 'eigen' verslag is het verzamelverslag van de koepel,
    # met tien tot vijftien kringen erin, en deze lezer kent het hoofdstukfilter
    # niet dat wachtrij.py daarvoor heeft. Zonder deze uitzondering kregen
    # Arcadis, CK1, CRH en Randstad alle vier dezelfde uitsplitsing — precies de
    # vervuiling die vanochtend was opgeruimd.
    fondsen = con.execute("""
        SELECT DISTINCT f.id, f.name, f.aum_euro_bn FROM funds f
        JOIN historical_metrics h ON h.fund_id = f.id
        WHERE COALESCE(f.is_pensioenfonds, 1) = 1
          AND f.name NOT LIKE 'Kring %' AND f.name NOT LIKE 'Pensioenkring %'
          AND f.name NOT LIKE '% APF'
        ORDER BY f.name""").fetchall()

    kolom = {"actief": "deelnemers_actief", "slapers": "deelnemers_slapers",
             "gepensioneerd": "deelnemers_pensioengerechtigd", "totaal": "deelnemers_totaal"}
    gevuld, afwijkend, onwaarschijnlijk, voorstel, fondsen_geraakt = 0, [], [], [], 0
    for f in fondsen[:args.max]:
        bron = bron_voor(con, f["id"], args.jaar)
        if not bron:
            continue
        raak = False
        per_jaar = lees_alle_jaren(bron, range(args.jaar - 4, args.jaar + 1))
        for jaar in range(args.jaar - 4, args.jaar + 1):
            gelezen = per_jaar.get(jaar) or uit_kerncijfers(bron, jaar)
            if not gelezen:
                continue
            rij = con.execute("""SELECT rowid AS rid, deelnemers_actief a, deelnemers_slapers s,
                    deelnemers_pensioengerechtigd g, deelnemers_totaal t
                    FROM historical_metrics WHERE fund_id=? AND year=?""",
                              (f["id"], jaar)).fetchone()
            if not rij:
                continue
            oud = {"actief": rij["a"], "slapers": rij["s"],
                   "gepensioneerd": rij["g"], "totaal": rij["t"]}
            # Toets tegen het vermogen voordat er iets wordt weggeschreven. Zonder
            # die toets kreeg Ahold Delhaize over 2024 twee miljoen actieve
            # deelnemers: de lezer had een bedragentabel te pakken in plaats van
            # een aantallentabel, en beide bestaan uit getallen van zes cijfers.
            if f["aum_euro_bn"]:
                grootste = max(gelezen.values())
                per = f["aum_euro_bn"] * 1e9 / grootste if grootste else 0
                if not (1_000 <= per <= 5_000_000):
                    onwaarschijnlijk.append(
                        f"{f['name'][:28]:30s} FY{jaar} {grootste:,} deelnemers bij "
                        f"{f['aum_euro_bn']:.2f} mrd = {per:,.0f} euro per persoon".replace(",", "."))
                    continue

            # Toets tegen de buurjaren. Een deelnemersbestand kruipt; het
            # verdrievoudigt niet en het krimpt niet met een factor dertig. Ahold
            # Delhaize kwam anders op 2.004.310 actieven over 2024 te staan naast
            # 61.885 over 2025 — de lezer had een bedragentabel te pakken, en die
            # kwam ongeschonden door de vermogenstoets omdat het fonds groot is.
            sprong = False
            for veld, waarde in gelezen.items():
                buur = con.execute(f"""SELECT {kolom[veld]} w FROM historical_metrics
                    WHERE fund_id=? AND year BETWEEN ? AND ? AND year<>? AND {kolom[veld]} IS NOT NULL
                    ORDER BY ABS(year-?) LIMIT 1""",
                                   (f["id"], jaar - 2, jaar + 2, jaar, jaar)).fetchone()
                if buur and buur["w"] and not (1/3 <= waarde / buur["w"] <= 3):
                    onwaarschijnlijk.append(
                        f"{f['name'][:28]:30s} FY{jaar} {veld} {waarde:,} naast {buur['w']:,} "
                        f"in een buurjaar".replace(",", "."))
                    sprong = True
                    break
            if sprong:
                continue

            for veld, waarde in gelezen.items():
                if oud[veld] is None:
                    con.execute(f"UPDATE historical_metrics SET {kolom[veld]} = ? WHERE rowid = ?",
                                (waarde, rij["rid"]))
                    gevuld += 1
                    raak = True
                    voorstel.append(f"{f['name'][:26]:28s} FY{jaar} {veld:<14} {waarde:>10,}"
                                    .replace(",", "."))
                elif oud[veld] != waarde:
                    afwijkend.append(f"{f['name'][:28]:30s} FY{jaar} {veld}: "
                                     f"tabel {oud[veld]} vs verslag {waarde}")
        fondsen_geraakt += raak
    if not args.apply:
        con.rollback()
        print(f"DROOGLOOP — {gevuld} lege velden zouden worden gevuld bij "
              f"{fondsen_geraakt} fondsen, uit verslagen over {args.jaar}\n")
        for regel in voorstel:
            print("  " + regel)
    else:
        kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
        shutil.copy2(DB_PATH, kopie)
        con.commit()
        print(f"Back-up: {os.path.basename(kopie)}")
        print(f"{gevuld} lege velden gevuld bij {fondsen_geraakt} fondsen, "
              f"uit verslagen over {args.jaar}")
    if onwaarschijnlijk:
        print(f"\n{len(onwaarschijnlijk)} keer overgeslagen omdat het aantal niet bij het "
              f"vermogen past:")
        for regel in onwaarschijnlijk[:10]:
            print("  " + regel)
    if afwijkend:
        print(f"\n{len(afwijkend)} bestaande waarden wijken af van het verslag "
              f"(niet overschreven):")
        for regel in afwijkend[:15]:
            print("  " + regel)
    con.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jaar", type=int, default=2025)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max", type=int, default=60)
    ap.add_argument("--alle-jaren", action="store_true",
                    help="lees uit hetzelfde verslag ook de eerdere jaargangen")
    args = ap.parse_args()

    if args.alle_jaren:
        return alle_jaren(args)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rijen = con.execute("""
        SELECT h.rowid rid, h.fund_id, f.name, h.deelnemers_actief a, h.deelnemers_slapers s,
               h.deelnemers_pensioengerechtigd g, h.deelnemers_totaal t
        FROM historical_metrics h JOIN funds f ON f.id = h.fund_id
        WHERE h.year = ? ORDER BY f.name""", (args.jaar,)).fetchall()

    voorstel, ongewijzigd, geen_bron = [], 0, 0
    for r in rijen[:args.max]:
        bron = bron_voor(con, r["fund_id"], args.jaar)
        if not bron:
            geen_bron += 1
            continue
        gelezen = uit_tabel(bron, args.jaar) or uit_kerncijfers(bron, args.jaar)
        if not gelezen or "totaal" not in gelezen:
            continue
        oud = {"actief": r["a"], "slapers": r["s"], "gepensioneerd": r["g"], "totaal": r["t"]}
        anders = {k: v for k, v in gelezen.items() if oud.get(k) != v}
        if anders:
            voorstel.append((r, bron, gelezen, anders))
        else:
            ongewijzigd += 1

    print(f"{len(rijen)} rijen over {args.jaar}; {geen_bron} zonder verslag, "
          f"{ongewijzigd} al gelijk aan het verslag\n")
    print(f"{len(voorstel)} rijen wijken af van hun eigen jaarverslag:\n")
    for r, bron, gelezen, anders in voorstel:
        oud = f"{r['a']}/{r['s']}/{r['g']}/{r['t']}"
        nieuw = "/".join(str(gelezen.get(k, "—")) for k in
                         ("actief", "slapers", "gepensioneerd", "totaal"))
        print(f"  {r['name'][:32]:<34} {oud:<28} -> {nieuw}")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")
    kolom = {"actief": "deelnemers_actief", "slapers": "deelnemers_slapers",
             "gepensioneerd": "deelnemers_pensioengerechtigd", "totaal": "deelnemers_totaal"}
    for r, _bron, gelezen, _anders in voorstel:
        for veld, waarde in gelezen.items():
            con.execute(f"UPDATE historical_metrics SET {kolom[veld]} = ? WHERE rowid = ?",
                        (waarde, r["rid"]))
    con.commit()
    print(f"{len(voorstel)} rijen hersteld uit het jaarverslag.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
