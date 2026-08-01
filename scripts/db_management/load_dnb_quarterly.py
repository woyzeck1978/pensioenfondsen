"""Load DNB per-fund quarterly JSON into the database.

Two tables get touched:
  * dnb_quarterly_metrics  — full feed (all 14 metrics × fund × quarter)
  * monthly_funding_ratios — beleidsdekkingsgraad mirror so existing
    dashboard/Excel wiring picks it up. Quarter-end month {3,6,9,12}.
    actuele_dekkingsgraad_pct is left NULL because DNB does not publish it.

Fund-name mapping is substring-based against funds.name. Unmatched DNB
rapporteurs and unmatched DB fund names are written to a markdown report.

Run from scripts/db_management/ so the relative DB path resolves.
"""

import json
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

DB_PATH = "../../data/processed/pension_funds.db"
JSON_PATH = "../../data/processed/dnb_per_fund_quarterly_raw.json"
UNMATCHED_PATH = "../../data/processed/dnb_unmatched.md"

# Manual overrides where automatic normalisation can't reconcile names.
# Extend over time. Format: DNB rapporteur (exact, .strip()) -> funds.id.
MANUAL_MAP: dict[str, int] = {
    # --- Toegevoegd na de opschoning van juli 2026 -------------------------
    # DNB hanteert voor de Centraal Beheer-kringen consequent "(Achmea)" en
    # gebruikt eigen kringnummers; daarnaast brak het hernoemen van BPFBouw en
    # het toevoegen van twintig APF-kringen de automatische naamkoppeling.
    "Bouwnijverheid": 15,
    "Kring flexible regeling (Hnp)": 229,
    "Pensioenkring Cargill (Hnp)": 230,
    "BPF Foodservice": 56,
    "Pensioenkring CRH (Hnp)": 187,
    "Pensioenkring Van Lanschot (Hnp)": 193,
    # Centraal Beheer APF — DNB noemt dit APF "Achmea"
    "Kring 1 - DC (Achmea)": 209,
    "Kring DB evenwicht (Achmea)": 210,
    "Kring 4 - Koopkracht (Achmea)": 211,
    "Kring 2 - DB premie (Achmea)": 212,
    "Kring 3 - Stabiliteit (Achmea)": 213,
    "Kring HPE (Achmea)": 214,
    "Kring RBS (Achmea)": 215,
    "Kring equensWorldline (Achmea)": 216,
    "Kring Sligro (Achmea)": 217,
    "Kring Deutche Bank NL (Achmea)": 218,
    "Kring Grolsch (Achmea)": 219,
    "Kring Chemours (Achmea)": 220,
    "Kring ES Nederland (Achmea)": 221,
    "Kring HP Nederland (Achmea)": 222,
    "Kring Bavaria (Achmea)": 223,
    # De Nationale APF — DNB gebruikt alleen de kringletter
    "Kring DC (De Nationale)": 224,
    "Kring G (De Nationale)": 225,
    "Kring H (De Nationale)": 226,

    "ABP": 9,
    "APG": 76,            # DNB "APG" (if it appears) maps to APG asset manager pension fund
    "Personeelspensioenfonds APG": 76,
    "BPL": 16,
    "Witteveen & Bos": 137,
    "Metaal en Techniek": 24,
    "Metalektro, bedrijfstakpensioenfonds": 58,
    "Roeiers in het Rotterdamse havengebied": 7,
    "Bibliotheken openbare": 29,
    "Recreatie (SPR)": 34,
    "Vlakglas, Verf, het Glasbewerkings- en het Glazeniersbedrijf": 62,
    "Vlees- en Vleeswarenindustrie en de Gemaksvoedingindustrie": 63,
    "Werk en (re)Integratie": 38,
    "Mode-, Interieur-, Tapijt- en Textielindustrie": 25,
    "Medische Specialisten": 5,
    "KLM-Cabinepersoneel": 48,
    "Cap Gemini Nederland": 83,
    "HaskoningDHV": 104,
    "Samenwerking / Slagersbedrijf": 35,
    "PGB": 32,
    "Betonproduktenindustrie": 14,
    "Bouwmaterialen": 18,                         # HiBiN = Bouwmaterialen
    "ING": 108,
    "PDN": 91,                                    # DSM Nederland = Pensioenfonds DSM Nederland
    "Zuivel en aanverwante industrie": 43,        # BPZ; dash placement differs from DB name
    "APF": 75,                                    # Generic DNB "APF" assumed = AkzoNobel APF

    # --- Toegevoegd 2026-08-01 --------------------------------------------
    # Deze rapporteurs stonden op None met als reden "not in DB". Dat was een
    # cirkelredenering: ze ontbraken juist omdat niemand ze had toegevoegd. Het
    # schoonmaakfonds heeft 6,8 miljard en 44 kwartalen historie. Zie
    # herstel_ontbrekende_fondsen.py voor hoe de rijen zijn aangemaakt.
    "Schoonmaak- en Glazenwassersbedrijf": 61,
    "Flexsecurity": 95,
    "Tandartsen en Tandarts-Specialisten": 231,
    "Honeywell": 232,
    "Nedlloyd": 233,
    "Pensura": 234,
    "YARA Nederland": 235,
    "Coram": 236,
    "Calpam": 237,
    # CRH en Wolters Kluwer rapporteren tot en met 2024Q4, hun kring begint in
    # 2025Q1: dezelfde stichting, nieuwe vlag. Beide stonden hierboven al
    # gekoppeld en werden verderop stilzwijgend op None gezet — een dubbele
    # sleutel in een dict-literal, waarvan de laatste wint.
    "CRH": 187,
    "Wolters Kluwer Nederland": 194,
    # Twee rapporteurs op hetzelfde fonds mag alleen handmatig: de automatische
    # koppeling slaat een fonds over zodra het bezet is, waardoor de kring zijn
    # eigen kwartalen kwijtraakte aan zijn voorganger.
    "Pensioenkring Wolters Kluwer NL (Hnp)": 194,
    # DNB kent Staples en de Stap-kring als aparte rapporteurs. Rij 177 droeg de
    # naam van de kring maar het verslag van Staples; op 2026-08-01 gesplitst.
    "Staples": 177,
    "Kring TotalEnergies NL (Stap)": 238,

    # Wel bekend bij DNB, bewust niet gekoppeld — koppelen zou dubbeltellen:
    "Shell Nederland": None,        # SNPS; fid 51 dekt SSPF en SNPS samen
    "ING Bank CDC fonds": None,     # fid 108 dekt ING inclusief CDC
    "Grolsche bierbrouwerij": None, # overlapt vier kwartalen met Kring Grolsch (219)
    "Lanschot": None,               # fid 193 heeft de reeks al over dezelfde jaren
    # HNPF (64) and Nationale-NL APF (145) intentionally not mapped: DNB
    # reports these APFs per kring ("Kring X (Achmea/Hnp/Stap)"), not under
    # the umbrella. Leaving them unmatched until a kring->APF aggregator is built.
}


def normalize(name: str) -> str:
    """Aggressive normalization for fuzzy matching DB <-> DNB names."""
    s = name.lower().strip()
    for tok in [
        "(", ")", " pensioenfonds", "pensioenfonds", "stichting ",
        " pf", "pf ", " bpf", "bpf ", " ppi", " apf",
        "bedrijf", "bedrijven", "industrie", "fonds",
    ]:
        s = s.replace(tok, " ")
    return " ".join(s.split())


def build_name_map(db_names, dnb_names):
    """Return {dnb_name: (fund_id, db_name, kind)} matches.

    Order of precedence: MANUAL_MAP > exact normalized > substring.
    """
    db_norm = {normalize(n): (fid, n) for fid, n in db_names}
    dnb_norm = {normalize(n): n for n in dnb_names}
    by_fid = {fid: n for fid, n in db_names}
    mapping: dict[str, tuple[int, str, str]] = {}

    # 0. manual overrides
    for dnb_name, fid in MANUAL_MAP.items():
        if fid is None:
            continue
        if fid not in by_fid:
            print(f"  WARN: MANUAL_MAP refers to unknown fund_id {fid} ({dnb_name})")
            continue
        mapping[dnb_name] = (fid, by_fid[fid], "manual")

    used_db = {fid for fid, _, _ in mapping.values()}

    # 1. exact normalized match
    for dn_norm, dnb_orig in dnb_norm.items():
        if not dn_norm or dnb_orig in mapping:
            continue
        if dn_norm in db_norm:
            fid, db_orig = db_norm[dn_norm]
            if fid in used_db:
                continue
            mapping[dnb_orig] = (fid, db_orig, "exact")
            used_db.add(fid)

    # 2. substring (only DB names not yet claimed); 3-char minimum
    for dn_norm, dnb_orig in dnb_norm.items():
        if dnb_orig in mapping or not dn_norm:
            continue
        candidate = None
        for db_n_norm, (fid, db_orig) in db_norm.items():
            if fid in used_db or not db_n_norm:
                continue
            if dn_norm in db_n_norm or db_n_norm in dn_norm:
                if min(len(dn_norm), len(db_n_norm)) >= 3:
                    candidate = (fid, db_orig); break
        if candidate:
            fid, db_orig = candidate
            mapping[dnb_orig] = (fid, db_orig, "substr")
            used_db.add(fid)
    return mapping


def epoch_ms_to_year_quarter(ms: int) -> tuple[int, int]:
    """DNB Periode is epoch milliseconds UTC. Map to (year, quarter)."""
    d = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return d.year, (d.month - 1) // 3 + 1


def quarter_end_month(q: int) -> int:
    return {1: 3, 2: 6, 3: 9, 4: 12}[q]


def main():
    with open(JSON_PATH) as f:
        payload = json.load(f)
    rows = payload["data"]
    print(f"DNB rows in feed: {len(rows):,}")

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON")

    db_names = con.execute("SELECT id, name FROM funds").fetchall()
    dnb_names = sorted({r[0].strip() for r in rows})
    print(f"DB funds: {len(db_names)} | DNB rapporteurs: {len(dnb_names)}")

    name_map = build_name_map(db_names, dnb_names)
    print(f"matched: {len(name_map)} / {len(dnb_names)}")

    # Stats counters
    inserted_dqm = 0
    inserted_mfr = 0
    skipped_unmatched = 0
    skipped_null = 0

    cur = con.cursor()
    for rapporteur, post, periode_ms, value in rows:
        rapporteur = rapporteur.strip()
        if rapporteur not in name_map:
            skipped_unmatched += 1
            continue
        if value is None:
            skipped_null += 1
            continue
        fund_id = name_map[rapporteur][0]
        year, quarter = epoch_ms_to_year_quarter(periode_ms)
        metric = post.strip()
        try:
            cur.execute(
                "INSERT INTO dnb_quarterly_metrics (fund_id, year, quarter, metric_name, value) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(fund_id, year, quarter, metric_name) DO UPDATE SET "
                "value = excluded.value, fetched_at = CURRENT_TIMESTAMP",
                (fund_id, year, quarter, metric, value),
            )
            inserted_dqm += 1
        except sqlite3.Error as e:
            print(f"  err dqm: {e} for fid={fund_id} {year}Q{quarter} {metric}")

        if metric.lower() == "beleidsdekkingsgraad":
            month = quarter_end_month(quarter)
            try:
                cur.execute(
                    "INSERT INTO monthly_funding_ratios (fund_id, year, month, beleidsdekkingsgraad_pct) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(fund_id, year, month) DO UPDATE SET "
                    "beleidsdekkingsgraad_pct = excluded.beleidsdekkingsgraad_pct",
                    (fund_id, year, month, value),
                )
                inserted_mfr += 1
            except sqlite3.Error as e:
                print(f"  err mfr: {e} for fid={fund_id} {year}-{month}")

    con.commit()

    # Unmatched report
    unmatched_dnb = [n for n in dnb_names if n not in name_map]
    matched_db_ids = {v[0] for v in name_map.values()}
    unmatched_db = [(fid, n) for fid, n in db_names if fid not in matched_db_ids]

    os.makedirs(os.path.dirname(UNMATCHED_PATH), exist_ok=True)
    with open(UNMATCHED_PATH, "w") as f:
        f.write("# DNB ↔ pension_funds.db name mismatches\n\n")
        f.write(f"Generated {datetime.now().isoformat(timespec='seconds')}\n\n")
        f.write(f"DB funds: {len(db_names)} | DNB rapporteurs: {len(dnb_names)} | matched: {len(name_map)}\n\n")
        f.write("## DNB rapporteurs without a DB match\n\n")
        for n in unmatched_dnb:
            f.write(f"- `{n}`\n")
        f.write(f"\n## DB funds without a DNB match\n\n")
        for fid, n in unmatched_db:
            f.write(f"- [{fid}] `{n}`\n")
        f.write("\n## Auto-matched (review for false positives)\n\n")
        for dnb_orig, (fid, db_orig, kind) in sorted(name_map.items()):
            f.write(f"- [{kind}] fid={fid} `{db_orig}` ⇄ DNB `{dnb_orig}`\n")

    print(f"\nupserted into dnb_quarterly_metrics: {inserted_dqm:,}")
    print(f"upserted into monthly_funding_ratios: {inserted_mfr:,}")
    print(f"skipped (unmatched fund):            {skipped_unmatched:,}")
    print(f"skipped (null value):                {skipped_null:,}")
    print(f"unmatched DNB rapporteurs: {len(unmatched_dnb)}")
    print(f"unmatched DB funds:        {len(unmatched_db)}")
    print(f"detail -> {UNMATCHED_PATH}")
    con.close()


if __name__ == "__main__":
    main()
