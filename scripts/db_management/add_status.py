import pandas as pd
import sqlite3

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

# 1. Establish default status
try:
    cursor.execute("ALTER TABLE funds ADD COLUMN status TEXT DEFAULT 'Open'")
except sqlite3.OperationalError:
    cursor.execute("UPDATE funds SET status = 'Open'")

conn.commit()

closed_funds = []

# Parse WTP
try:
    wtp = pd.read_excel('../data/Overzicht pensioentransitie wtp.xlsx', header=4)
    for _, row in wtp.iterrows():
        name = str(row.iloc[2]).strip()
        if "(gesloten)" in name.lower() or "wordt gesloten" in name.lower() or "liquidatie" in name.lower():
            clean_name = name.lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace("(wordt gesloten)", "").replace("bedrijfstak", "").replace("spf", "").strip()
            closed_funds.append(clean_name)
except:
    pass

# Parse DNB
try:
    dnb = pd.read_excel('../data/DNB Gegevens individuele pensioenfondsen 2023-2025.xlsx', header=1)
    for _, row in dnb.iterrows():
        name = str(row.iloc[0]).strip()
        if "(gesloten)" in name.lower() or "wordt gesloten" in name.lower() or "liquidatie" in name.lower():
            clean_name = name.lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace("(wordt gesloten)", "").replace("bedrijfstak", "").replace("spf", "").strip()
            closed_funds.append(clean_name)
except:
    pass

# Retrieve our funds
cursor.execute("SELECT id, name FROM funds")
existing_funds = cursor.fetchall()

def clean_db_name(n):
    return n.lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace("(wordt gesloten)", "").replace("bedrijfstak", "").replace("spf", "").strip()

applied_closed = 0

for f_id, f_name in existing_funds:
    db_clean = clean_db_name(f_name)
    is_closed = False
    
    # Direct name check just in case
    if "(gesloten)" in f_name.lower() or "wordt gesloten" in f_name.lower() or "liquidatie" in f_name.lower():
        is_closed = True
    else:
        # Cross reference against parsed closed list
        for target in closed_funds:
            if target == db_clean or (len(target) > 5 and target in db_clean) or (len(db_clean) > 5 and db_clean in target):
                is_closed = True
                break
                
    if is_closed:
        cursor.execute("UPDATE funds SET status = 'Gesloten' WHERE id = ?", (f_id,))
        applied_closed += 1

conn.commit()
conn.close()

print(f"Successfully labelled {applied_closed} funds as Gesloten. The rest are Open.")
