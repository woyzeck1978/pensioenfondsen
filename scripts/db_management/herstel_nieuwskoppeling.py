"""Koppel nieuwsberichten aan het fonds waar het domein van de URL bij hoort.

De monitor bezoekt de website van een fonds en bewaart elke link die op een
nieuwsbericht lijkt onder dat fonds-id. Drie dingen gaan daarbij mis.

  verkeerd fonds   De link wijst naar een ander fonds. Soms omdat twee fondsen
                   naar elkaar linken (KLM Grond had 44 berichten van het
                   cabinefonds staan, dat er zelf nul had), soms omdat een
                   fondsrij het domein van een ander fonds als website heeft.
                   Zo kwamen 34 berichten van pensioenfondsapf.nl bij fonds 64
                   terecht terwijl dat domein van 75 is — waarna de ophaler het
                   jaarverslag van 75 bij 64 afleverde en de analyse over het
                   verkeerde fonds ging.

  geen nieuws      Deelknoppen en vertaallinks. "JavaScript is not available."
                   van twitter.com, "Sign in" van linkedin.com, "Nieuws" van
                   translate.google.nl. Die staan in de voettekst van elk
                   bericht en zeggen niets over enig fonds.

  wees             Het fonds is bij het ontdubbelen verwijderd, het nieuws niet.
                   21 berichten stonden op de id's 61, 95 en 153, die geen van
                   drieen nog in `funds` voorkomen.

Twee regels die het verschil maken met naief domein-vergelijken:

  1. Eerst de volledige hostnaam, pas daarna het hoofddomein. `nn.cdcpensioen.nl`
     en `ing.cdcpensioen.nl` zijn twee verschillende fondsen; afkappen op de
     laatste twee labels zou ze op een hoop gooien en zeven ING-berichten naar
     NN verhuizen.
  2. Nooit een fonds van zijn eigen domein weghalen. Bij koepel-APF's delen de
     kringen het domein van de koepel met opzet: elf Stap-kringen op
     stappensioen.nl, drie Hnp-kringen op hnpf.nl. Een bericht op je eigen
     website is van jou, ook als het domein formeel bij de koepel hoort.

Wat het script niet doet: een domein verplaatsen dat bij geen enkel fonds hoort.
De Geschilleninstantie Pensioenfondsen is een sectorinstantie, geen fonds; die
berichten worden gemeld, niet stilzwijgend herplaatst.

  python3 scripts/db_management/herstel_nieuwskoppeling.py
  python3 scripts/db_management/herstel_nieuwskoppeling.py --apply
"""

from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

# Domeinen die nooit een nieuwsbericht van een fonds opleveren.
RUIS = ("twitter.com", "x.com", "linkedin.com", "facebook.com", "translate.google",
        "google.com", "google.nl", "whatsapp.com")


def host(url: str | None) -> str | None:
    """www.pensioenfondsapf.nl -> pensioenfondsapf.nl, subdomein behouden."""
    if not url or url.startswith("mailto:"):
        return None
    h = urlparse(url if "//" in url else "https://" + url).netloc.lower().split(":")[0]
    return (h[4:] if h.startswith("www.") else h) or None


def hoofddomein(h: str | None) -> str | None:
    if not h:
        return None
    deel = h.split(".")
    return ".".join(deel[-2:]) if len(deel) >= 2 else h


def eigenaars(con) -> tuple[dict, dict]:
    """Twee kaarten: op volledige hostnaam en op hoofddomein.

    Deelt meer dan een fonds hetzelfde domein, dan wint het fonds met de meeste
    DNB-kwartaalrijen. Dat is het fonds dat echt bestaat: duplicaatrijen en
    verkeerd ingevulde websites hebben er geen. Bij gelijke stand wint het
    laagste id, zodat de uitkomst niet per run verschilt.
    """
    per_host: dict[str, list] = defaultdict(list)
    per_domein: dict[str, list] = defaultdict(list)
    for fid, naam, site, dnb in con.execute("""
            SELECT f.id, f.name, f.website,
                   (SELECT COUNT(*) FROM dnb_quarterly_metrics d WHERE d.fund_id = f.id)
            FROM funds f WHERE f.website IS NOT NULL"""):
        h = host(site)
        if h and not any(r in h for r in RUIS):
            per_host[h].append((-dnb, fid, naam))
            per_domein[hoofddomein(h)].append((-dnb, fid, naam))
    kies = lambda kaart: {k: (sorted(v)[0][1], sorted(v)[0][2]) for k, v in kaart.items()}
    return kies(per_host), kies(per_domein)


def analyseer(con):
    op_host, op_domein = eigenaars(con)
    namen = dict(con.execute("SELECT id, name FROM funds"))
    eigen = {fid: host(w) for fid, w in con.execute("SELECT id, website FROM funds")}

    verplaats: dict[tuple, list] = defaultdict(list)
    ruis: list = []
    vreemd: dict[tuple, list] = defaultdict(list)
    wezen: dict[tuple, list] = defaultdict(list)

    for rid, fid, url in con.execute(
            "SELECT rowid, fund_id, url FROM news_articles WHERE url IS NOT NULL"):
        h = host(url)
        if not h:
            continue
        if any(r in h for r in RUIS):
            ruis.append((rid, hoofddomein(h)))
            continue
        if fid not in namen:
            # Heeft het domein een levend fonds, dan verhuist de wees daarheen;
            # anders is er niets om hem aan op te hangen.
            erfgenaam = op_host.get(h) or op_domein.get(hoofddomein(h))
            if erfgenaam:
                verplaats[(fid, erfgenaam[0], h)].append(rid)
            else:
                wezen[(fid, h)].append(rid)
            continue
        # Regel 2: op je eigen (sub)domein zit je goed, ook bij een koepel-APF.
        mijn = eigen.get(fid)
        if mijn and (h == mijn or hoofddomein(h) == hoofddomein(mijn)):
            continue
        # Regel 1: eerst de volledige hostnaam, dan pas het hoofddomein.
        doel = op_host.get(h) or op_domein.get(hoofddomein(h))
        if doel is None:
            vreemd[(fid, h)].append(rid)
        elif doel[0] != fid:
            verplaats[(fid, doel[0], h)].append(rid)
    return verplaats, ruis, vreemd, wezen, namen


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf de correcties weg")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    verplaats, ruis, vreemd, wezen, namen = analyseer(con)

    n_v = sum(len(v) for v in verplaats.values())
    print(f"{n_v} berichten staan bij een ander fonds dan waar hun domein bij hoort:\n")
    for (van, naar, h), rids in sorted(verplaats.items(), key=lambda x: -len(x[1])):
        print(f"  {len(rids):>3}x  {h:<26} {van:>4} {namen.get(van, '?')[:24]:<26}"
              f" -> {naar:>4} {namen.get(naar, '?')[:26]}")

    print(f"\n{len(ruis)} berichten zijn deelknoppen of vertaallinks, geen nieuws:")
    for d in sorted({r[1] for r in ruis}):
        print(f"  {sum(1 for r in ruis if r[1] == d):>3}x  {d}")

    if wezen:
        n_w = sum(len(v) for v in wezen.values())
        print(f"\n{n_w} berichten hangen aan een fonds dat niet meer bestaat "
              f"— worden verwijderd:")
        for (fid, h), rids in sorted(wezen.items(), key=lambda x: -len(x[1])):
            print(f"  {len(rids):>3}x  {h:<34} fonds {fid}")

    if vreemd:
        n_x = sum(len(v) for v in vreemd.values())
        print(f"\n{n_x} berichten staan op een domein dat bij geen enkel fonds hoort "
              f"— met de hand beoordelen:")
        for (fid, h), rids in sorted(vreemd.items(), key=lambda x: -len(x[1])):
            print(f"  {len(rids):>3}x  {h:<34} nu bij {fid} {namen.get(fid, '?')[:26]}")

    if not args.apply:
        print("\nDroogloop. Draai met --apply om dit weg te schrijven.")
        con.close()
        return 0

    kopie = DB_PATH.replace(".db", f".backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, kopie)
    print(f"\nBack-up: {os.path.basename(kopie)}")

    for (_van, naar, _h), rids in verplaats.items():
        con.executemany("UPDATE news_articles SET fund_id=? WHERE rowid=?",
                        [(naar, r) for r in rids])
    weg = [(r[0],) for r in ruis] + [(r,) for v in wezen.values() for r in v]
    con.executemany("DELETE FROM news_articles WHERE rowid=?", weg)
    con.commit()
    print(f"{n_v} berichten verplaatst, {len(weg)} verwijderd "
          f"({len(ruis)} ruis, {len(weg) - len(ruis)} zonder fonds).")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
