import pandas as pd
import sqlite3
import numpy as np

# Load DB
conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

# 1. Add new columns
new_cols = {
    'dnb_beleids_dgr': 'REAL',
    'dnb_rente_afdekking_pct': 'REAL',
    'dnb_zakelijke_waarden_pct': 'REAL',
    'wtp_transitie_datum': 'TEXT',
    'wtp_contract_type': 'TEXT',
    'wtp_invaren': 'TEXT',
    'wtp_deelnemers_k': 'REAL',
    'status': 'TEXT'
}

for col, dtype in new_cols.items():
    try:
        cursor.execute(f"ALTER TABLE funds ADD COLUMN {col} {dtype}")
    except sqlite3.OperationalError:
        pass # Column already exists

conn.commit()

# Read DNB
dnb = pd.read_excel('../data/DNB Gegevens individuele pensioenfondsen 2023-2025.xlsx', header=1)
dnb_rows = []
for _, row in dnb.iterrows():
    name = str(row.iloc[0]).strip()
    if name != 'nan':
        dnb_rows.append({
            'name': name,
            'dnb_beleids_dgr': row.iloc[1] if pd.notnull(row.iloc[1]) else None,
            'dnb_rente_afdekking_pct': row.iloc[2] if pd.notnull(row.iloc[2]) else None,
            'dnb_zakelijke_waarden_pct': row.iloc[3] if pd.notnull(row.iloc[3]) else None
        })

# Read WTP
wtp = pd.read_excel('../data/Overzicht pensioentransitie wtp.xlsx', header=4)
wtp_rows = []
for _, row in wtp.iterrows():
    name = str(row.iloc[2]).strip()
    if name != 'nan':
        # Compile year
        t_date = None
        y25, y26, y27, y28 = row.iloc[3], row.iloc[5], row.iloc[7], row.iloc[9]
        if pd.notnull(y25): t_date = str(y25).split(' ')[0]
        elif pd.notnull(y26): t_date = str(y26).split(' ')[0]
        elif pd.notnull(y27): t_date = str(y27).split(' ')[0]
        elif pd.notnull(y28): t_date = str(y28).split(' ')[0]
        
        wtp_rows.append({
            'name': name,
            'wtp_transitie_datum': t_date,
            'wtp_contract_type': row.iloc[12] if pd.notnull(row.iloc[12]) else None,
            'wtp_invaren': row.iloc[13] if pd.notnull(row.iloc[13]) else None,
            'wtp_deelnemers_k': row.iloc[14] if pd.notnull(row.iloc[14]) else None
        })

def clean_name(n):
    return n.lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace("(wordt gesloten)", "").replace("bedrijfstak", "").replace("spf", "").strip()

cursor.execute("SELECT id, name FROM funds")
existing_funds = cursor.fetchall()

def find_match(target_name):
    target = clean_name(target_name)
    for f_id, f_name in existing_funds:
        db_clean = clean_name(f_name)
        if target == db_clean or (len(target) > 5 and target in db_clean) or (len(db_clean) > 5 and db_clean in target):
            return f_id
    if "pme" in target_name.lower() or "metalektro" in target_name.lower():
        for f_id, f_name in existing_funds:
            if "pme" in f_name.lower(): return f_id
    if "pmt" in target_name.lower() or "metaal en techniek" in target_name.lower():
        for f_id, f_name in existing_funds:
            if "pmt" in f_name.lower(): return f_id
    if "bouw" in target_name.lower():
        for f_id, f_name in existing_funds:
            if "bouw" in f_name.lower() and "bpf" in f_name.lower(): return f_id
    return None

updates = 0
added = 0

# Merge DNB
for row in dnb_rows:
    f_id = find_match(row['name'])
    if f_id:
        cursor.execute("UPDATE funds SET dnb_beleids_dgr=?, dnb_rente_afdekking_pct=?, dnb_zakelijke_waarden_pct=? WHERE id=?", 
            (row['dnb_beleids_dgr'], row['dnb_rente_afdekking_pct'], row['dnb_zakelijke_waarden_pct'], f_id))
        updates += 1
    else:
        cursor.execute("INSERT INTO funds (name, dnb_beleids_dgr, dnb_rente_afdekking_pct, dnb_zakelijke_waarden_pct, data_source) VALUES (?, ?, ?, ?, 'DNB Appendix')", 
            (row['name'], row['dnb_beleids_dgr'], row['dnb_rente_afdekking_pct'], row['dnb_zakelijke_waarden_pct']))
        added += 1

# Refetch after appends
cursor.execute("SELECT id, name FROM funds")
existing_funds = cursor.fetchall()

# Merge WTP
for row in wtp_rows:
    f_id = find_match(row['name'])
    if f_id:
        cursor.execute("UPDATE funds SET wtp_transitie_datum=?, wtp_contract_type=?, wtp_invaren=?, wtp_deelnemers_k=? WHERE id=?", 
            (row['wtp_transitie_datum'], row['wtp_contract_type'], row['wtp_invaren'], row['wtp_deelnemers_k'], f_id))
        updates += 1

conn.commit()
conn.close()

print(f"Successfully processed {updates} matches and appended {added} new funds!")
