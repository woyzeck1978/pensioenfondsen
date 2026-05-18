"""Scrape pension fund websites for two missing fields:
    * deelnemers_totaal / actieve / slapers / gepensioneerd
    * uitvoerder (administrative provider name)

Strategy:
    - Visit the fund's homepage + a few likely 'over ons' / 'organisatie' /
      'over-het-fonds' paths
    - Pattern-match the rendered text for:
        - '<number> deelnemers' (Dutch thousand separator)
        - 'aantal deelnemers : <number>'
        - 'actieve deelnemers' / 'gepensioneerden' / 'slapers'
        - Known administrator names (TKP, APG, Achmea, AZL, Mercer, AON, Centric, Robeco, …)
    - Write findings to a scratch table extracted_metadata so a separate
      step can review and apply.

Conservative: per-fund, take the highest-confidence single value. No DB
updates from this script — that happens in the apply step.

Usage:
    python3 scrape_fund_metadata.py             # all funds with any NULL
    python3 scrape_fund_metadata.py --limit 5   # smoke test
"""

import argparse
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

DB_PATH = "../../data/processed/pension_funds.db"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.7"}

# Paths to probe after the homepage
PATHS = [
    "/",
    "/over-ons", "/over-ons/",
    "/over-het-fonds", "/over-het-fonds/",
    "/over-het-pensioenfonds", "/over-het-pensioenfonds/",
    "/over", "/organisatie",
    "/wie-zijn-wij", "/wie-zijn-we",
    "/het-fonds", "/het-fonds/",
    "/feiten-en-cijfers", "/cijfers",
    "/jaarverslag",
]

# Known administrator / uitvoerder names. Search in order — first match wins.
# Specific multi-word names first to avoid 'TKP' matching inside 'TKPI'.
UITVOERDERS = [
    "Achmea Pensioenservices", "Achmea Pensioen Services",
    "Centric Pension and Insurance Solutions", "Centric",
    "AZL Pensioenfondsadministratie", "AZL",
    "TKP Pensioen", "TKP Investments", "TKP",
    "APG Service Partners", "APG",
    "PGGM",
    "MN", "Mn Services",
    "Blue Sky Group", "BlueSkyGroup",
    "Mercer Nederland", "Mercer",
    "AON Hewitt", "AON",
    "Cardano",
    "Achmea Investment Management", "Achmea",
    "Robeco PPI", "Robeco",
    "Goldman Sachs Asset Management",
    "BNP Paribas",
    "Aegon Asset Management", "Aegon",
    "ING Pension Service",
    "Visma Idella", "Visma",
    "Capgemini Pensioenfonds", "Capgemini",
    "Allianz",
    "Be Frank",
]

# Stuff to filter out as obvious not-uitvoerder matches inside body text
# (these appear in news copy without indicating administrator).
UITVOERDER_NEG_CONTEXT = (
    "aandeelhouder", "vakbond", "partner", "concurrent",
)

# Regex for "<n> [actieve] [gepensioneerde] deelnemers". NL uses '.' as thousand
# separator. We accept '.' or ',' (cosmetic), require at least 100 to skip 'we
# hebben 3 deelnemers in onze raad'.
RE_DEELNEMERS = re.compile(
    r"\b(\d{1,3}(?:[.\s]\d{3})+|\d{4,7})\s+"
    r"(?:actieve\s+|gewezen\s+|gepensioneerde\s+|deelnemende\s+)?"
    r"(deelnemers|pensioengerechtigden|gepensioneerden|slapers)\b",
    re.IGNORECASE,
)
# 'aantal deelnemers: 1.234'
RE_LABELED = re.compile(
    r"aantal\s+(deelnemers|pensioengerechtigden|gepensioneerden|slapers|actieve\s+deelnemers)\s*[:\-]?\s*"
    r"(\d{1,3}(?:[.\s]\d{3})+|\d{4,7})",
    re.IGNORECASE,
)


def to_int(s: str) -> int | None:
    s = re.sub(r"[\s.]", "", s)
    try:
        return int(s)
    except Exception:
        return None


def fetch(url: str, session: requests.Session) -> str | None:
    try:
        r = session.get(url, headers=HEADERS, timeout=8, allow_redirects=True)
        if r.status_code >= 400:
            return None
        return r.text
    except Exception:
        return None


def extract_uitvoerder(text: str) -> str | None:
    """First exact-match wins. Skip if surrounded by neg-context words."""
    tl = text.lower()
    for cand in UITVOERDERS:
        idx = tl.find(cand.lower())
        if idx < 0:
            continue
        # 80-char window around match
        lo, hi = max(0, idx - 60), idx + len(cand) + 60
        window = tl[lo:hi]
        if any(neg in window for neg in UITVOERDER_NEG_CONTEXT):
            continue
        return cand
    return None


def extract_deelnemer_counts(text: str) -> dict[str, int]:
    """Return {category: largest_count_seen}."""
    out: dict[str, int] = {}

    def bump(key: str, val: int | None):
        if val is None or val < 100:
            return
        if key not in out or val > out[key]:
            out[key] = val

    for m in RE_LABELED.finditer(text):
        kind = m.group(1).lower().strip()
        bump(_norm_kind(kind), to_int(m.group(2)))
    for m in RE_DEELNEMERS.finditer(text):
        kind = m.group(2).lower().strip()
        prefix_window = text[max(0, m.start()-25):m.start()].lower()
        if "actiev" in prefix_window:
            bump("actief", to_int(m.group(1)))
        elif "gewezen" in prefix_window or "slaper" in prefix_window:
            bump("slapers", to_int(m.group(1)))
        elif "gepensione" in prefix_window:
            bump("gepensioneerd", to_int(m.group(1)))
        else:
            bump(_norm_kind(kind), to_int(m.group(1)))
    return out


def _norm_kind(kind: str) -> str:
    kind = kind.lower()
    if "slaper" in kind: return "slapers"
    if "gepensione" in kind or "pensioengerecht" in kind: return "gepensioneerd"
    if "actief" in kind or "actieve" in kind: return "actief"
    return "totaal"


CANDIDATE_LINK_KEYWORDS = (
    "over-ons", "over_ons", "over het fonds", "over-het-fonds", "over het pensioenfonds",
    "wie-zijn", "wie zijn", "het-fonds", "het fonds",
    "feiten", "cijfers", "kerncijfers", "feiten-en-cijfers",
    "organisatie", "bestuur",
    "uitvoerder", "uitvoering",
)


def find_candidate_urls(homepage_html: str, base_url: str) -> list[str]:
    """Return links on the homepage whose href OR anchor-text contains keywords."""
    try:
        soup = BeautifulSoup(homepage_html, "html.parser")
    except Exception:
        return []
    base_netloc = urlparse(base_url).netloc.lower()
    found: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text(" ", strip=True) or "").lower()
        href_l = href.lower()
        if not any(kw in href_l or kw in text for kw in CANDIDATE_LINK_KEYWORDS):
            continue
        full = urljoin(base_url, href)
        if urlparse(full).netloc.lower() and base_netloc not in urlparse(full).netloc.lower():
            continue  # off-domain
        if full in seen:
            continue
        seen.add(full)
        found.append(full)
        if len(found) >= 12:
            break
    return found


def scrape_one(row, session: requests.Session) -> dict:
    fid, name, website = row
    out = {"fund_id": fid, "name": name,
           "actief": None, "slapers": None, "gepensioneerd": None,
           "totaal": None, "uitvoerder": None, "source_url": None}
    if not website or website in ("None", ""):
        return out
    base = website if website.startswith("http") else f"https://{website}"
    base = base.rstrip("/")

    counts: dict[str, int] = {}
    uitv: str | None = None
    src_url: str | None = None

    # 1) Fetch homepage to discover relevant in-site links
    homepage_html = fetch(base + "/", session)
    if homepage_html is None:
        return out

    urls_to_visit: list[str] = [base + "/"]
    urls_to_visit.extend(find_candidate_urls(homepage_html, base + "/"))
    # Also include the static path guesses as a fallback
    for p in PATHS[1:]:
        if not p.startswith("/"):
            continue
        candidate = base + p
        if candidate not in urls_to_visit:
            urls_to_visit.append(candidate)

    # 2) Visit each (with a soft cap)
    for url in urls_to_visit[:18]:
        html = homepage_html if url == base + "/" else fetch(url, session)
        if not html:
            continue
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            continue
        text = soup.get_text(" ", strip=True)
        if len(text) < 200:
            continue

        page_counts = extract_deelnemer_counts(text)
        for k, v in page_counts.items():
            if k not in counts or v > counts[k]:
                counts[k] = v
                src_url = url

        if uitv is None:
            cand = extract_uitvoerder(text)
            if cand:
                uitv = cand
                src_url = src_url or url

        if counts.get("totaal") and uitv:
            break

    # Derive totaal if we have components but no explicit total
    if "totaal" not in counts and any(k in counts for k in ("actief", "slapers", "gepensioneerd")):
        s = sum(counts.get(k, 0) for k in ("actief", "slapers", "gepensioneerd"))
        if s >= 100:
            counts["totaal"] = s

    out.update({k: counts.get(k) for k in ("actief", "slapers", "gepensioneerd", "totaal")})
    out["uitvoerder"] = uitv
    out["source_url"] = src_url
    return out


_local = threading.local()


def get_session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        _local.s = s
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS extracted_metadata (
            fund_id INTEGER PRIMARY KEY,
            actief INTEGER,
            slapers INTEGER,
            gepensioneerd INTEGER,
            totaal INTEGER,
            uitvoerder TEXT,
            source_url TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(fund_id) REFERENCES funds(id)
        )
    """)
    con.commit()

    q = """
        SELECT id, name, website FROM funds
        WHERE (
            deelnemers_totaal IS NULL
            OR uitvoerder IS NULL OR uitvoerder = ''
            OR deelnemers_actief IS NULL
        )
        AND website IS NOT NULL AND website != '' AND website != 'None'
    """
    if args.limit > 0:
        q += f" LIMIT {int(args.limit)}"
    rows = cur.execute(q).fetchall()
    con.close()
    n = len(rows)
    print(f"Target: {n} funds with at least one missing field + a website (workers={args.workers})")

    start = time.time()
    n_any = n_uitv = n_total = 0
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, r in enumerate(ex.map(lambda row: scrape_one(row, get_session()), rows), 1):
            results.append(r)
            if r["uitvoerder"]: n_uitv += 1
            if r["totaal"]: n_total += 1
            if any(r[k] for k in ("actief", "slapers", "gepensioneerd", "totaal", "uitvoerder")):
                n_any += 1
            if i % 25 == 0 or i == n:
                el = time.time() - start
                print(f"  [{i}/{n}] any={n_any} uitv={n_uitv} totaal={n_total} rate={i/max(el,0.1):.1f}/s")

    # Write to extracted_metadata
    con = sqlite3.connect(DB_PATH)
    for r in results:
        con.execute("""
            INSERT INTO extracted_metadata (fund_id, actief, slapers, gepensioneerd, totaal, uitvoerder, source_url)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(fund_id) DO UPDATE SET
                actief=excluded.actief, slapers=excluded.slapers,
                gepensioneerd=excluded.gepensioneerd, totaal=excluded.totaal,
                uitvoerder=excluded.uitvoerder, source_url=excluded.source_url,
                scraped_at=CURRENT_TIMESTAMP
        """, (r["fund_id"], r["actief"], r["slapers"], r["gepensioneerd"],
              r["totaal"], r["uitvoerder"], r["source_url"]))
    con.commit()
    con.close()
    print(f"\nDone in {time.time()-start:.0f}s. Hits: any={n_any}, uitvoerder={n_uitv}, totaal={n_total}")


if __name__ == "__main__":
    main()
