import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

orig_id = 108  # ING / ING CDC
dup_id = 161   # ING Bank CDC fonds

cursor.execute("SELECT name, dnb_beleids_dgr, dnb_rente_afdekking_pct, dnb_zakelijke_waarden_pct, wtp_transitie_datum, wtp_contract_type, wtp_invaren, wtp_deelnemers_k FROM funds WHERE id=?", (dup_id,))
metrics = cursor.fetchone()

if metrics:
    name = metrics[0]
    params = metrics[1:]
    print(f"MERGING: '{name}' (ID {dup_id}) INTO -> Original ID {orig_id}")
    
    cursor.execute("""
        UPDATE funds SET 
        dnb_beleids_dgr=COALESCE(?, dnb_beleids_dgr),
        dnb_rente_afdekking_pct=COALESCE(?, dnb_rente_afdekking_pct),
        dnb_zakelijke_waarden_pct=COALESCE(?, dnb_zakelijke_waarden_pct),
        wtp_transitie_datum=COALESCE(?, wtp_transitie_datum),
        wtp_contract_type=COALESCE(?, wtp_contract_type),
        wtp_invaren=COALESCE(?, wtp_invaren),
        wtp_deelnemers_k=COALESCE(?, wtp_deelnemers_k)
        WHERE id=?
    """, (*params, orig_id))
    
cursor.execute("DELETE FROM funds WHERE id=?", (dup_id,))

conn.commit()

cursor.execute("SELECT COUNT(*) FROM funds")
final_count = cursor.fetchone()[0]
conn.close()

print(f"\nFinal Database Row Count: {final_count}")
