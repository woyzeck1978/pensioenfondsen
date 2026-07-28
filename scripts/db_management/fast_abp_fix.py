import sqlite3
import re
from urllib.parse import urlparse

DB_PATH = "data/processed/pension_funds.db"

def extract_dutch_month_date(url):
    parsed = urlparse(url)
    path = parsed.path.lower()
    dutch_months = {
        'januari': '01', 'februari': '02', 'maart': '03', 'april': '04',
        'mei': '05', 'juni': '06', 'juli': '07', 'augustus': '08',
        'september': '09', 'oktober': '10', 'november': '11', 'december': '12'
    }
    match = re.search(r'/((?:19|20)\d{2})/([a-z]+)/', path)
    if match and match.group(2) in dutch_months:
        return f"{match.group(1)}-{dutch_months[match.group(2)]}-01"
    return None

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()
c.execute("SELECT n.id, n.url FROM news_articles n JOIN funds f ON n.fund_id = f.id WHERE f.name LIKE '%ABP%'")
rows = c.fetchall()
updates = []
for row in rows:
    id, url = row
    new_date = extract_dutch_month_date(url)
    if new_date:
        updates.append((new_date, id))

if updates:
    c.executemany("UPDATE news_articles SET published_date = ? WHERE id = ?", updates)
    conn.commit()
    print(f"Updated {len(updates)} ABP records instantly.")
conn.close()
