import sys
import sqlite3
from playwright.sync_api import sync_playwright

sys.path.append("/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen")
from scripts.monitor_websites_playwright import scan_fund_playwright

db_path = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/data/processed/pension_funds.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

names = ['%Recreatie%', '%Roeiers%', '%Schoonmaak%', '%Tobacon%', '%Unilever%', '%PWRI%', '%SPW%']
funds_to_test = []

for name in names:
    cur.execute("SELECT id, name, website FROM funds WHERE name LIKE ? LIMIT 1", (name,))
    row = cur.fetchone()
    if row:
        funds_to_test.append(row)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={'width': 1920, 'height': 1080}
    )
    
    for fund in funds_to_test:
        print(f"Testing fund: {fund['name']} ({fund['website']})")
        items = scan_fund_playwright(fund['id'], fund['name'], fund['website'], context, conn)
        print(f"-> Found {len(items) if items else 0} items.")
        if items:
            for item in items:
                print(f"   - {item['doc_type']}: {item['url']}")
        print("\n")
        
    browser.close()
conn.close()
