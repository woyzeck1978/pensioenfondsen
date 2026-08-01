"""Promote scraped_documents news URLs to news_articles with parsed title + date.

`scraped_documents` accumulates `doc_type='news'` URLs from website crawls. Many
listing titles are generic CTAs ('Lees verder', 'Lees meer', 'Downloads'). This
script visits each URL not yet in `news_articles`, extracts a real headline
(og:title / h1 / <title>) and a published date when visible, then INSERTs into
`news_articles` (UNIQUE on url).

Uses `requests` + BeautifulSoup rather than Playwright — pension fund news
articles are server-rendered HTML, no JS needed. Playwright's chromium also
trips HTTP/2 protocol errors on some fund sites; requests handles them cleanly.

Run from scripts/data_collection/ so the relative DB path resolves.

Usage:
    python3 parse_news_articles.py             # all backlog
    python3 parse_news_articles.py --limit 50  # smoke test
"""

from __future__ import annotations  # 3.9-deploy safe: defer `str | None` hints

import argparse
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import requests
from bs4 import BeautifulSoup

DB_PATH = "../../data/processed/pension_funds.db"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.7"}

DUTCH_MONTHS = {
    "januari": "01", "februari": "02", "maart": "03", "april": "04",
    "mei": "05", "juni": "06", "juli": "07", "augustus": "08",
    "september": "09", "oktober": "10", "november": "11", "december": "12",
    "jan": "01", "feb": "02", "mrt": "03", "apr": "04",
    "jun": "06", "jul": "07", "aug": "08", "sep": "09", "okt": "10", "nov": "11", "dec": "12",
}

RE_DUTCH_DATE = re.compile(
    r"\b(\d{1,2})\s+(januari|februari|maart|april|mei|juni|juli|augustus|september|oktober|november|december|jan|feb|mrt|apr|jun|jul|aug|sep|okt|nov|dec)\s+(20\d{2})\b",
    re.IGNORECASE,
)
RE_ISO_DATE = re.compile(r"\b(20\d{2})[\-/](\d{1,2})[\-/](\d{1,2})\b")
RE_NL_DATE = re.compile(r"\b(\d{1,2})[\-/](\d{1,2})[\-/](20\d{2})\b")

# URL patterns:
#   /2025/december/de-pensioenen-worden-in-2026-verhoogd
#   /2024/november/...
RE_URL_DUTCH = re.compile(
    r"/(20\d{2})/(januari|februari|maart|april|mei|juni|juli|augustus|september|oktober|november|december)/",
    re.IGNORECASE,
)
#   /20250109--wijziging-inloggen-met-digid  (yyyymmdd)
RE_URL_YYYYMMDD = re.compile(r"/(20\d{2})(\d{2})(\d{2})[/-]")
#   /2025/11/05/...  or  /nieuws/2025-11-05-...
RE_URL_ISO_PATH = re.compile(r"[/-](20\d{2})[/-](\d{2})[/-](\d{2})")
#   /11-05-2026-update-... (Dutch DD-MM-YYYY)
RE_URL_DUTCH_DMY = re.compile(r"/(\d{2})-(\d{2})-(20\d{2})[-/]")


def date_from_url(url: str) -> str | None:
    """Date encoded directly in the URL slug — high-confidence signal."""
    today = date.today().isoformat()
    m = RE_URL_DUTCH.search(url)
    if m:
        candidate = f"{m.group(1)}-{DUTCH_MONTHS[m.group(2).lower()]}-01"
        if candidate <= today:
            return candidate
    m = RE_URL_YYYYMMDD.search(url)
    if m:
        candidate = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        if candidate <= today:
            return candidate
    # Dutch DD-MM-YYYY is checked BEFORE ISO_PATH because URLs sometimes
    # contain both (e.g. /nieuws-2026/11-05-2026-…) and the Dutch form
    # is the explicit publication date while the year segment is just a
    # collection bucket.
    m = RE_URL_DUTCH_DMY.search(url)
    if m:
        dd, mo, yy = m.group(1), m.group(2), m.group(3)
        if 1 <= int(mo) <= 12 and 1 <= int(dd) <= 31:
            candidate = f"{yy}-{mo}-{dd}"
            if candidate <= today:
                return candidate
    m = RE_URL_ISO_PATH.search(url)
    if m:
        candidate = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        if candidate <= today:
            return candidate
    return None

GENERIC_TITLES = {
    "lees verder", "lees meer", "meer lezen", "downloads", "download",
    "klik hier", "naar het bericht", "ga naar", "open link", "open",
    "pagina niet gevonden (404)", "pagina niet gevonden", "page not found",
    "404", "error", "fout", "media & pers", "media & pers", "nieuws",
}


def parse_published_date(
    text: str, *, limit_chars: int = 1500, trust_today: bool = False
) -> str | None:
    """Find the publication date near the article's start.

    Articles typically print the byline date in the first paragraph. Sites
    also stamp the article header with the publish date — but the page
    footer often contains today's "last updated" timestamp that pollutes
    a naive search. Strategy: scan only the first `limit_chars` of text
    (header/byline zone) and pick the FIRST plausible past date.

    `trust_today=True` is for structured sources (<time datetime>,
    <meta article:published_time>) where finding today's date almost
    certainly means the article was actually published today. For free
    body text, default `trust_today=False` — finding only today is more
    often a "last updated" footer than a real byline.
    """
    if not text:
        return None
    today = date.today().isoformat()
    region = text[:limit_chars]
    seen_today = False
    for m in RE_DUTCH_DATE.finditer(region):
        d = f"{m.group(3)}-{DUTCH_MONTHS[m.group(2).lower()]}-{int(m.group(1)):02d}"
        if d == today:
            seen_today = True; continue
        if d < today:
            return d
    for m in RE_ISO_DATE.finditer(region):
        d = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        if d == today:
            seen_today = True; continue
        if d < today:
            return d
    for m in RE_NL_DATE.finditer(region):
        d = f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
        if d == today:
            seen_today = True; continue
        if d < today:
            return d
    return today if (seen_today and trust_today) else None


def looks_generic(title: str | None) -> bool:
    if not title:
        return True
    t = title.lower().strip()
    return t in GENERIC_TITLES or len(t) < 5


def clean_title(t: str | None) -> str | None:
    if not t:
        return None
    # publishers tail like ' | SiteName' or ' - SiteName'
    t = re.split(r"\s+[\|\-–]\s+", t)[0].strip()
    # collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def fetch_article(url: str, session: requests.Session) -> tuple[str | None, str | None]:
    """Return (headline, published_date_iso_or_None) for one URL."""
    try:
        r = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
    except Exception:
        return None, None
    if r.status_code >= 400:
        return None, None
    try:
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None, None

    headline = None
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        headline = clean_title(og["content"])
    if looks_generic(headline):
        h1 = soup.find("h1")
        if h1:
            headline = clean_title(h1.get_text(" ", strip=True))
    if looks_generic(headline):
        t = soup.find("title")
        if t:
            headline = clean_title(t.get_text(" ", strip=True))

    pub_date = None
    # 1) Date encoded in the URL — highest confidence
    pub_date = date_from_url(url)
    # 2) <time datetime="..."> — structured source, trust today
    if not pub_date:
        time_el = soup.find("time", attrs={"datetime": True})
        if time_el and time_el.get("datetime"):
            pub_date = parse_published_date(time_el["datetime"], trust_today=True)
    # 3) <meta property="article:published_time"> — structured, trust today
    if not pub_date:
        meta_pub = soup.find("meta", attrs={"property": "article:published_time"})
        if meta_pub and meta_pub.get("content"):
            pub_date = parse_published_date(meta_pub["content"], trust_today=True)
    # 4) body text — scan only the byline zone (first 1500 chars).
    # trust_today=False: a lone "today" here is usually a footer cache stamp.
    if not pub_date:
        text = soup.get_text(" ", strip=True)
        pub_date = parse_published_date(text)

    return headline, pub_date


# Per-thread session for connection pooling
_local = threading.local()


def get_session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        _local.s = s
    return s


db_lock = threading.Lock()


def canoniek(url: str) -> str:
    """URL zonder fragment en zonder afsluitende slash.

    De tabel heeft UNIQUE op url, en een fonds dat zowel .../nieuws/bericht als
    .../nieuws/bericht#main oplevert kreeg daardoor twee rijen met dezelfde
    titel en datum. Dat waren 552 overtollige berichten, zichtbaar als
    dubbelingen op de nieuwspagina.
    """
    return url.split("#")[0].rstrip("/")


def worker(row, db_path):
    fund_id, url, fallback_title = row
    url = canoniek(url)
    headline, pub_date = fetch_article(url, get_session())
    if looks_generic(headline):
        if not looks_generic(fallback_title):
            headline = clean_title(fallback_title)
        else:
            headline = None
    with db_lock:
        conn = sqlite3.connect(db_path, timeout=20)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO news_articles (fund_id, url, title, published_date) "
                "VALUES (?, ?, ?, ?)",
                (fund_id, url, headline, pub_date),
            )
            conn.commit()
        finally:
            conn.close()
    return (url, headline is not None, pub_date is not None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    q = """
        SELECT s.fund_id, s.url, s.title
        FROM scraped_documents s
        WHERE s.doc_type='news'
          AND s.url NOT IN (SELECT url FROM news_articles)
    """
    if args.limit > 0:
        q += f" LIMIT {int(args.limit)}"
    rows = conn.execute(q).fetchall()
    conn.close()
    n = len(rows)
    print(f"Backlog: {n} unparsed news URLs (workers={args.workers})", flush=True)
    if n == 0:
        return

    start = time.time()
    n_headline = n_date = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, res in enumerate(ex.map(lambda r: worker(r, DB_PATH), rows), 1):
            url, has_h, has_d = res
            if has_h: n_headline += 1
            if has_d: n_date += 1
            if i % 50 == 0 or i == n:
                elapsed = time.time() - start
                rate = i / max(elapsed, 0.1)
                eta = (n - i) / max(rate, 0.1)
                print(
                    f"  [{i}/{n}] headline_ok={n_headline} date_ok={n_date} "
                    f"rate={rate:.1f}/s eta={eta:.0f}s",
                    flush=True,
                )
    print(f"Done in {time.time()-start:.0f}s. headline_ok={n_headline}, date_ok={n_date}")


if __name__ == "__main__":
    main()
