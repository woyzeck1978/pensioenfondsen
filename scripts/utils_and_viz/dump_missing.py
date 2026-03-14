import sqlite3
import os

db_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/processed/pension_funds.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

query = """
SELECT f.name, f.website 
FROM funds f 
LEFT JOIN scraped_documents s ON f.id = s.fund_id 
WHERE s.id IS NULL 
AND f.website IS NOT NULL 
AND f.website != '' 
AND f.website != 'None'
ORDER BY f.name
"""

cur.execute(query)
missing_funds = cur.fetchall()

out_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/missing_dump.txt"
with open(out_path, "w") as f:
    f.write(f"TOTAL MISSING FUNDS: {len(missing_funds)}\n\n")
    for name, website in missing_funds:
        f.write(f"- {name} ({website})\n")
        
conn.close()
