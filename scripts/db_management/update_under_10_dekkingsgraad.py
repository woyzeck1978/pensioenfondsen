import sqlite3
from bs4 import BeautifulSoup

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()
cursor.execute("SELECT id, name FROM funds WHERE dekkingsgraad_pct < 10;")
funds_to_fix = cursor.fetchall()

print(f"Funds to fix: {len(funds_to_fix)}")

with open('data/exelerating_dekkingsgraad.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')

updated_count = 0
for tr in soup.find_all('tr'):
    tds = tr.find_all('td')
    if len(tds) >= 4:
        name_td = tds[0].get_text(strip=True).lower()
        dek_td = tds[2].get_text(strip=True).replace('%', '').replace(',', '.')
        bel_td = tds[3].get_text(strip=True).replace('%', '').replace(',', '.')
        
        try:
            dek_val = float(dek_td)
            bel_val = float(bel_td)
        except ValueError:
            continue
            
        for fund_id, fund_name in funds_to_fix:
            search_name = fund_name.replace("Pensioenfonds", "").replace("Stichting", "").replace("SPF", "").strip().lower()
            
            match_bool = False
            if "abp" in name_td and "abp" in search_name: match_bool = True
            elif "pfzw" in name_td and "pfzw" in search_name: match_bool = True
            elif "hoogovens" in name_td and "hoogovens" in search_name: match_bool = True
            elif "bpl" in name_td and "bpl pensioen" in search_name: match_bool = True
            elif "sbz" in search_name and "zorgverzekeraars" in name_td: match_bool = True
            elif "koopvaardij" in search_name and "koopvaardij" in name_td: match_bool = True
            elif "horeca" in search_name and "horeca" in name_td: match_bool = True
            elif "mediahuis" in search_name and "mediahuis" in name_td: match_bool = True
            elif "kappersbedrijf" in search_name and "kappersbedrijf" in name_td: match_bool = True
            elif "schilders" in search_name and "schilders" in name_td: match_bool = True
            elif "apf het nederlandse" in search_name and "apf het nederlandse" in name_td: match_bool = True
            elif "brocacef" in search_name and "brocacef" in name_td: match_bool = True
            elif "cargill" in search_name and "cargill" in name_td: match_bool = True
            elif "citigroup" in search_name and "citigroup" in name_td: match_bool = True
            elif "hal" in search_name and "hal" in name_td: match_bool = True
            elif "kas bank" in search_name and "kas" in name_td: match_bool = True
            elif "bisdommen" in search_name and "bisdommen" in name_td: match_bool = True
            elif "robeco" in search_name and "robeco" in name_td: match_bool = True
            elif "sportfondsen" in search_name and "sportfondsen" in name_td: match_bool = True
            elif "nationale-nederlanden" in search_name and "nationale-nederlanden" in name_td: match_bool = True
            elif search_name in name_td: match_bool = True
                
            if match_bool:
                print(f"Matched '{fund_name}' to '{name_td}'. Fixing -> Dek: {dek_val}%, Bel: {bel_val}%")
                cursor.execute("UPDATE funds SET dekkingsgraad_pct=?, beleidsdekkingsgraad_pct=? WHERE id=?", (dek_val, bel_val, fund_id))
                updated_count += 1
                break

conn.commit()
conn.close()
print(f"Fixed {updated_count} out of {len(funds_to_fix)} funds.")
