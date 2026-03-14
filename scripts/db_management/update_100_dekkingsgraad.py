import sqlite3
from bs4 import BeautifulSoup
import re

name_mapping = {
    19: "Horecabedrijf", # Horeca & Catering (Hospitality)
    20: "Kappersbedrijf", # Kappersbedrijf (Hairdressers)
    21: "Koopvaardij", # Koopvaardij (Merchant Navy)
    42: "Zorgverzekeraars", # Zorgverzekeraars / SBZ (Health Insurers)
    50: "Mediahuis", # Mediahuis Nederland (Mhpf)
    60: "Schilders", # Schilders-, Afwerkings- en Glaszetbedrijf (Painters and Glaziers)
    64: "APF", # APF Het Nederlandse Pensioenfonds
    66: "De Nationale APF", # De Nationale APF
    67: "Stap APF", # Stap APF
    71: "ABN AMRO", # ABN AMRO
    81: "Brocacef", # Brocacef
    84: "Cargill", # Cargill
    85: "Citigroup", # Citigroup
    94: "Exxonmobil", # Exxonmobil
    103: "HAL", # HAL
    110: "Kas Bank", # KAS BANK
    117: "Bisdommen", # Nederlandse Bisdommen
    124: "Robeco", # Robeco
    130: "Sportfondsen", # Sportfondsen
    145: "NN", # Nationale-Nederlanden
}

conn = sqlite3.connect("../../data/processed/pension_funds.db")
cursor = conn.cursor()

cursor.execute("SELECT id, name FROM funds WHERE dekkingsgraad_pct = 100.0;")
funds_100 = cursor.fetchall()
print(f"Found {len(funds_100)} funds with 100.0% dekkingsgraad.")

with open("data/exelerating_dekkingsgraad.html", "r") as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')
updated_count = 0

for fund_id, fund_name in funds_100:
    search_term = name_mapping.get(fund_id, fund_name).lower()
    found_pct = None
    
    for tr in soup.find_all('tr'):
        # Just grab all text in the row
        row_text = tr.get_text(separator=' ', strip=True).lower()
        if search_term in row_text:
            tds = tr.find_all('td')
            if len(tds) >= 4:
                val_text = tds[3].get_text(strip=True)
                pct_match = re.search(r'([\d,\.]+)%', val_text)
                if pct_match:
                    pct_str = pct_match.group(1).replace(',', '.')
                    try:
                        found_pct = float(pct_str)
                        break
                    except:
                        pass
                else:
                    pct_str = val_text.replace('%', '').replace(',', '.')
                    try:
                        found_pct = float(pct_str)
                        break
                    except:
                        pass
                        
    if found_pct is not None:
        print(f"[{fund_id}] {fund_name} -> Found: {found_pct}%")
        cursor.execute("UPDATE funds SET dekkingsgraad_pct=? WHERE id=?", (found_pct, fund_id))
        updated_count += 1
    else:
        print(f"[{fund_id}] {fund_name} -> Mapped as '{search_term}', but NOT FOUND")

conn.commit()
conn.close()

print(f"\nFinal tally: Updated {updated_count} out of {len(funds_100)} funds.")
