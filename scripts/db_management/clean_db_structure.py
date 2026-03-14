import sqlite3
import pandas as pd
import re
import os

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")

def parse_cost(cost_str):
    if not cost_str or pd.isna(cost_str):
        return None
    cost_str = str(cost_str).replace('%', '').replace(',', '.').strip()
    try:
        return float(cost_str)
    except ValueError:
        return None

def extract_alloc(mix_str, keywords):
    if not mix_str or pd.isna(mix_str):
        return None
    
    parts = str(mix_str).split('|')
    for part in parts:
        part_lower = part.lower()
        if any(kw in part_lower for kw in keywords):
            # Extract number
            # e.g. "Vastrentend: 45,2%" or "Vastgoed: 12%"
            match = re.search(r'([\d,.]+)', part)
            if match:
                val_str = match.group(1).replace(',', '.')
                try:
                    return float(val_str)
                except ValueError:
                    continue
    return None

def clean_database():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Add new numeric columns if they don't exist
    try:
        cursor.execute("ALTER TABLE funds ADD COLUMN fixed_income_pct REAL")
    except sqlite3.OperationalError:
        pass # Already exists
        
    try:
        cursor.execute("ALTER TABLE funds ADD COLUMN real_estate_pct REAL")
    except sqlite3.OperationalError:
        pass
        
    try:
        cursor.execute("ALTER TABLE funds ADD COLUMN alternatives_pct REAL")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE funds ADD COLUMN vermogensbeheerkosten_pct REAL")
    except sqlite3.OperationalError:
        pass
        
    try:
        cursor.execute("ALTER TABLE funds ADD COLUMN transactiekosten_pct REAL")
    except sqlite3.OperationalError:
        pass

    df = pd.read_sql_query('SELECT id, beleggingsmix, vermogensbeheerkosten, transactiekosten FROM funds', conn)
    
    updates = 0
    
    for idx, row in df.iterrows():
        f_id = row['id']
        mix = row['beleggingsmix']
        v_cost = row['vermogensbeheerkosten']
        t_cost = row['transactiekosten']
        
        fixed_income = extract_alloc(mix, ['vastrentend', 'obligaties', 'fixed income'])
        real_estate = extract_alloc(mix, ['vastgoed', 'real estate'])
        alternatives = extract_alloc(mix, ['alternatie', 'overig'])
        
        v_val = parse_cost(v_cost)
        t_val = parse_cost(t_cost)
        
        # We only update if we found something to keep the UPDATE statements efficient
        cursor.execute("""
            UPDATE funds 
            SET fixed_income_pct = ?,
                real_estate_pct = ?,
                alternatives_pct = ?,
                vermogensbeheerkosten_pct = ?,
                transactiekosten_pct = ?
            WHERE id = ?
        """, (fixed_income, real_estate, alternatives, v_val, t_val, f_id))
        updates += 1
        
    conn.commit()
    conn.close()
    
    print(f"Successfully cleaned and structured data for {updates} funds.")

if __name__ == '__main__':
    clean_database()
