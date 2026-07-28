import sqlite3
import urllib.parse
import re

def extract_date_from_url(url):
    parsed = urllib.parse.urlparse(url)
    path = parsed.path
    match = re.search(r'/((?:19|20)\d{2})/(\d{2})/(\d{2})/', path)
    if match: return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r'/((?:19|20)\d{2})(\d{2})(\d{2})[-/]', path)
    if match: return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r'/((?:19|20)\d{2})/(\d{2})/', path)
    if match: return f"{match.group(1)}-{match.group(2)}-01"
    match = re.search(r'/((?:19|20)\d{2})/', path)
    if match: return f"{match.group(1)}-01-01"
    return None

conn = sqlite3.connect("data/processed/pension_funds.db")
c = conn.cursor()
c.execute("SELECT id, url, published_date FROM news_articles WHERE url LIKE '%20250625-jaarverslag-spf-2024-staat-online%'")
row = c.fetchone()
if row:
    id, url, published_date = row
    print("Found row:", id, url, published_date)
    new_date = extract_date_from_url(url)
    print("Extracted new date:", new_date)
    if new_date and new_date != published_date:
        print("Executing update...")
        c.execute("UPDATE news_articles SET published_date = ? WHERE id = ?", (new_date, id))
        conn.commit()
        print("Update committed!")
        
        c.execute("SELECT published_date FROM news_articles WHERE id = ?", (id,))
        print("Verify DB:", c.fetchone()[0])
else:
    print("No row found!")
conn.close()
