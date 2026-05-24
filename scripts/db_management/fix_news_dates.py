"""Backfill news_articles.published_date for NULL rows via HTTP fetch.

URL-slug patterns are handled by parse_news_articles.date_from_url; the
URL-only backfill was already applied on 2026-05-23 (commit ff75562) and
fixed 673 rows. This script is the HTTP-fetch tier for the remaining
NULL rows where the slug does not encode a date.

Strategy:
  1. Select WHERE published_date IS NULL/empty
  2. Skip rows with title 'Challenge Validation' (Cloudflare-blocked,
     ~179 rows — fetching would burn time for guaranteed no-content)
  3. Concurrent HTTP GETs (12 workers, 10s timeout each)
  4. Extract date from <meta article:published_time>, <time datetime>,
     or JSON-LD `datePublished`. First hit wins.
  5. Validate 2010 <= year <= today; reject otherwise
  6. Bulk UPDATE at the end

Run from project root:
    python3 scripts/db_management/fix_news_dates.py
"""

import os
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests
from bs4 import BeautifulSoup

import json

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "..", "..", "data", "processed", "pension_funds.db")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}

WORKERS = 12
TIMEOUT = 10
ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def extract_date(soup: BeautifulSoup) -> str | None:
    # 1) <meta property="article:published_time">
    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and meta.get("content"):
        m = ISO_DATE.match(meta["content"].strip())
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # 2) <time datetime="...">
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        m = ISO_DATE.match(t["datetime"].strip())
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # 3) JSON-LD datePublished
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if isinstance(obj, dict) and obj.get("datePublished"):
                m = ISO_DATE.match(str(obj["datePublished"]).strip())
                if m:
                    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def fetch_one(nid: int, url: str) -> tuple[int, str | None, str | None]:
    """Return (id, date_or_None, error_or_None)."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    except requests.RequestException as e:
        return nid, None, f"net: {type(e).__name__}"
    if r.status_code >= 400:
        return nid, None, f"http {r.status_code}"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        return nid, None, f"parse: {e}"
    d = extract_date(soup)
    if not d:
        return nid, None, "no date in meta/time/jsonld"
    year = int(d[:4])
    today = date.today().isoformat()
    if year < 2010 or d > today:
        return nid, None, f"out-of-range: {d}"
    return nid, d, None


def main():
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        """
        SELECT id, url, date(scraped_at) AS scrape_d FROM news_articles
        WHERE (published_date IS NULL OR published_date = '')
          AND COALESCE(title,'') NOT LIKE 'Challenge%Validation%'
          AND COALESCE(title,'') NOT LIKE '%JavaScript%not%available%'
          AND LOWER(TRIM(COALESCE(title,''))) NOT IN (
                'nieuws','nieuwsberichten','sign in',
                'selecteer hier uw profiel:','media & pers',
                'overzicht','home','menu','404','error','fout'
          )
        """
    ).fetchall()
    print(f"NULL rows to fetch: {len(rows)}", flush=True)

    scrape_by_id = {nid: sd for nid, _, sd in rows}
    updates: list[tuple[str, int]] = []
    no_date = 0
    after_scrape = 0
    errors: dict[str, int] = {}
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_one, nid, url): nid for nid, url, _ in rows}
        for fut in as_completed(futures):
            nid, d, err = fut.result()
            done += 1
            if d:
                # Guard: fetched date can't be after the row's scrape date.
                # Pages sometimes display the latest-update timestamp instead
                # of the original publish date — that'd produce a future
                # published_date relative to when we crawled.
                if scrape_by_id.get(nid) and d > scrape_by_id[nid]:
                    after_scrape += 1
                else:
                    updates.append((d, nid))
            elif err:
                no_date += 1
                k = err.split(":")[0]
                errors[k] = errors.get(k, 0) + 1
            if done % 50 == 0 or done == len(rows):
                print(f"  {done}/{len(rows)}  hits={len(updates)}  "
                      f"misses={no_date}  after-scrape-rejected={after_scrape}",
                      flush=True)

    print(f"\nWriting {len(updates)} updates")
    con.executemany("UPDATE news_articles SET published_date = ? WHERE id = ?", updates)
    con.commit()

    print(f"hits:                  {len(updates)}")
    print(f"after-scrape rejected: {after_scrape}")
    print(f"misses:                {no_date}")
    print("breakdown:")
    for k, v in sorted(errors.items(), key=lambda x: -x[1]):
        print(f"  {k:<20} {v}")
    con.close()


if __name__ == "__main__":
    sys.exit(main())
