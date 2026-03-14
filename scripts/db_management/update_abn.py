import urllib.request
from bs4 import BeautifulSoup
import re
import csv
import sqlite3

# 1. Calculate and update equity percentage for ABN AMRO (fund id 71)
equity = 4581015
total = 28736309
abn_equity_pct = equity / total * 100
print(f"ABN AMRO Equity Pct: {abn_equity_pct:.2f}%")

# Append to CSV
with open("../../data/processed/extracted_allocations.csv", "a", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["71", f"{abn_equity_pct:.2f}", "71_ABN_Amro_Jaarverslag_2024.pdf"])

# Update DB For Equity
conn = sqlite3.connect("../../data/processed/pension_funds.db")
cursor = conn.cursor()
cursor.execute("""
INSERT INTO equity_allocations_extracted (fund_id, allocation_pct, source_file) 
VALUES (71, ?, '71_ABN_Amro_Jaarverslag_2024.pdf') 
ON CONFLICT(fund_id) DO UPDATE SET allocation_pct=?, source_file='71_ABN_Amro_Jaarverslag_2024.pdf';
""", (abn_equity_pct, abn_equity_pct))

# 2. Fetch ABN AMRO dekkingsgraad from Exelerating
import ssl
url = "https://exelerating.com/nl/insights/overzicht-dekkingsgraad-pensioenfondsen/"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
context = ssl._create_unverified_context()
html = urllib.request.urlopen(req, context=context).read()
soup = BeautifulSoup(html, 'html.parser')

abn_dekkingsgraad = None
# ABN AMRO in the table
text_raw = soup.get_text()
# find ABN AMRO Bank and the next percentage
import re
match = re.search(r'ABN AMRO Bank\s+[\d\.]*\s*(\d+,\d+)%', text_raw)
if match:
    dekkingsgraad_str = match.group(1).replace(',', '.')
    abn_dekkingsgraad = float(dekkingsgraad_str)
    print(f"Found ABN AMRO Dekkingsgraad: {abn_dekkingsgraad}%")
else:
    print("Could not parse Dekkingsgraad via regex, searching broader...")
    match2 = re.search(r'ABN AMRO Bank.*?(\d+,\d+)%', text_raw, re.DOTALL)
    if match2:
        dekkingsgraad_str = match2.group(1).replace(',', '.')
        abn_dekkingsgraad = float(dekkingsgraad_str)
        print(f"Found ABN AMRO Dekkingsgraad (broad): {abn_dekkingsgraad}%")
        
if abn_dekkingsgraad:
    # We update the 'funds' table with the dekkingsgraad_pct
    cursor.execute("UPDATE funds SET dekkingsgraad_pct=? WHERE id=71", (abn_dekkingsgraad,))

conn.commit()
conn.close()
print("Finished updating ABN AMRO data.")
