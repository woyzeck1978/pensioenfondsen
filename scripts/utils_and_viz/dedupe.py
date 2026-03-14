import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

# Get all
query = "SELECT id, name, data_source FROM funds"
df = pd.read_sql_query(query, conn)

original = df[df['data_source'] != 'DNB Appendix']
new_funds = df[df['data_source'] == 'DNB Appendix']

# Manual alias mapping: New Name -> Original Name substring
mappings = {
    "ABP": "abp",
    "AHOLD": "ahold",
    "Atos Origin": "atos",
    "Bakkersbedrijf BPF": "bakkers",
    "Betonproduktenindustrie": "beton",
    "Cap Gemini Nederland": "capgemini",
    "Horecabedrijf": "horeca",
    "IKEA": "ikea",
    "ING": "ing",
    "KLM-Cabinepersoneel": "klm",
    "PDN": "dsm",
    "PGB": "pgb",
    "Shell": "shell",
    "Vlakglas, Verf, het Glasbewerkings- en het Glazeniersbedrijf": "vlakglas",
    "Zuivel en aanverwante industrie": "zuivel",
    "Leefomgeving": "leefomgeving",
    "Mode-, Interieur-, Tapijt- en Textielindustrie": "mitt",
    "Samenwerking / Slagersbedrijf": "slagers",
    "Vlees- en Vleeswarenindustrie en de Gemaksvoedingindustrie": "vlees",
    "Cosun": "cosun"
}

def clean_for_match(n):
    return String(n).lower().replace("pensioenfonds", "").replace("stichting", "").replace(" ", "").strip()

merged_count = 0

for _, new_row in new_funds.iterrows():
    new_id = new_row['id']
    new_name = new_row['name']
    
    target_original_id = None
    
    # 1. Custom dict match
    for key, val in mappings.items():
        if key.lower() == new_name.lower():
            # Find in original
            for _, orig_row in original.iterrows():
                if val in str(orig_row['name']).lower():
                    target_original_id = orig_row['id']
                    break
            break
            
    # 2. Aggressive substring match
    if not target_original_id:
        clean_new = new_name.lower().replace("pensioenfonds", "").replace("stichting", "").replace(" ", "").strip()
        for _, orig_row in original.iterrows():
            clean_orig = str(orig_row['name']).lower().replace("pensioenfonds", "").replace("stichting", "").replace(" ", "").strip()
            if len(clean_new) > 4 and clean_new in clean_orig:
                target_original_id = orig_row['id']
                break
            if len(clean_orig) > 4 and clean_orig in clean_new:
                target_original_id = orig_row['id']
                break

    if target_original_id:
        print(f"MERGING: '{new_name}' (ID {new_id}) INTO -> Original ID {target_original_id}")
        
        # We need to copy the DNB metrics from new_id to target_original_id
        cursor.execute("SELECT dnb_beleids_dgr, dnb_rente_afdekking_pct, dnb_zakelijke_waarden_pct, wtp_transitie_datum, wtp_contract_type, wtp_invaren, wtp_deelnemers_k FROM funds WHERE id=?", (new_id,))
        metrics = cursor.fetchone()
        
        if metrics:
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
            """, (*metrics, target_original_id))
            
        # Delete the new duplicate row
        cursor.execute("DELETE FROM funds WHERE id=?", (new_id,))
        merged_count += 1

conn.commit()
cursor.execute("SELECT COUNT(*) FROM funds")
final_count = cursor.fetchone()[0]
conn.close()

print(f"\nSuccessfully merged and deleted {merged_count} duplicates.")
print(f"Final Database Row Count: {final_count}")
