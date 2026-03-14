import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

# Tuples of (Target Original ID, Duplicate Appended ID)
merges = [
    (137, 204), # Witteveen+Bos <- Witteveen & Bos
    (7, 199),   # Roeiers <- Roeiers in het Rotterdamse havengebied
    (5, 179),   # Medisch Specialisten <- Medische Specialisten
    (34, 197)   # Recreatie <- Recreatie (SPR)
]

merged_count = 0

for orig_id, dup_id in merges:
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
    merged_count += 1

conn.commit()

cursor.execute("SELECT COUNT(*) FROM funds")
final_count = cursor.fetchone()[0]
conn.close()

print(f"\nSuccessfully surgically merged {merged_count} user-flagged duplicates.")
print(f"Final Database Row Count: {final_count}")
