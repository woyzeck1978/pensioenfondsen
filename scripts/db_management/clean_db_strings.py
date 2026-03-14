import sqlite3
import re

def clean_strings():
    print("Starting clean_strings script...")
    db_path = 'data/processed/pension_funds.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Clean Beleggingsmix
    print("Querying beleggingsmix...")
    cursor.execute("SELECT id, name, beleggingsmix FROM funds WHERE beleggingsmix IS NOT NULL AND fixed_income_pct IS NULL")
    rows_mix = cursor.fetchall()
    print(f"Found {len(rows_mix)} funds with unparsed beleggingsmix.")
    
    updates_mix = 0
    for row in rows_mix:
        fid, name, mix = row[0], row[1], str(row[2]).lower()
        
        fi_match = re.search(r'(vastrentend|obligat|fixed)[^\d]*(\d+[,.]?\d*)%?', mix)
        re_match = re.search(r'(vastgoed|real.*estate)[^\d]*(\d+[,.]?\d*)%?', mix)
        alt_match = re.search(r'(alternatie|alternative)[^\d]*(\d+[,.]?\d*)%?', mix)
        
        fi_val = float(fi_match.group(2).replace(',', '.')) if fi_match else None
        re_val = float(re_match.group(2).replace(',', '.')) if re_match else None
        alt_val = float(alt_match.group(2).replace(',', '.')) if alt_match else None
        
        if fi_val is not None or re_val is not None or alt_val is not None:
            cursor.execute("UPDATE funds SET fixed_income_pct=?, real_estate_pct=?, alternatives_pct=? WHERE id=?", 
                           (fi_val, re_val, alt_val, fid))
            updates_mix += 1

    # 2. Clean Costs
    print("Querying costs...")
    cursor.execute("SELECT id, name, vermogensbeheerkosten, transactiekosten FROM funds WHERE (vermogensbeheerkosten IS NOT NULL AND vermogensbeheerkosten_pct IS NULL) OR (transactiekosten IS NOT NULL AND transactiekosten_pct IS NULL)")
    rows_cost = cursor.fetchall()
    print(f"Found {len(rows_cost)} funds with unparsed costs.")
    
    updates_cost = 0
    for row in rows_cost:
        fid, name, vk, tk = row[0], row[1], str(row[2]), str(row[3])
        
        vk_match = re.search(r'(\d+[,.]\d+)%?', vk) if vk and vk != 'None' and vk != 'nan' else None
        tk_match = re.search(r'(\d+[,.]\d+)%?', tk) if tk and tk != 'None' and tk != 'nan' else None
        
        vk_val = float(vk_match.group(1).replace(',', '.')) if vk_match else None
        tk_val = float(tk_match.group(1).replace(',', '.')) if tk_match else None
        
        if vk_val is not None or tk_val is not None:
            cursor.execute("UPDATE funds SET vermogensbeheerkosten_pct=?, transactiekosten_pct=? WHERE id=?", 
                           (vk_val, tk_val, fid))
            updates_cost += 1

    conn.commit()
    conn.close()
    
    print(f"Cleaned Beleggingsmix for {updates_mix} funds.")
    print(f"Cleaned Cost Metrics for {updates_cost} funds.")

if __name__ == "__main__":
    clean_strings()
