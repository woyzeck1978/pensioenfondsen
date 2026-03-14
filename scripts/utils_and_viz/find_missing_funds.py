import pandas as pd
import requests
import sqlite3
import io

url = "https://exelerating.com/nl/insights/overzicht-dekkingsgraad-pensioenfondsen/"
headers = {'User-Agent': 'Mozilla/5.0'}

response = requests.get(url, headers=headers)
tables = pd.read_html(io.StringIO(response.text))
df = tables[0]

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()
cursor.execute("SELECT id, name, dekkingsgraad_pct FROM funds")
all_funds = cursor.fetchall()

missing = []

for _, row in df.iterrows():
    exe_name = str(row.iloc[0]).strip()
    if exe_name == 'nan': continue
    
    # Check if exe_name in DB
    found = False
    for f_id, f_name, f_dek in all_funds:
        if exe_name.lower() in f_name.lower() or f_name.lower() in exe_name.lower():
            if f_dek:
                found = True
                break
    if not found:
        missing.append(exe_name)

print(f"Funds on Exelerating missing from our DB or missing Coverage Ratios: {len(missing)}")
for m in missing[:15]:
    print(m)

conn.close()
