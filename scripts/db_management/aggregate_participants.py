import sqlite3
import pandas as pd

def sum_participants():
    db_path = 'data/processed/pension_funds.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Read funds where deelnemers_totaal is null, but we have some sub-metrics
    query = """
    SELECT id, name, deelnemers_actief, deelnemers_slapers, deelnemers_gepensioneerd, deelnemers_totaal
    FROM funds 
    WHERE deelnemers_totaal IS NULL 
    AND (deelnemers_actief IS NOT NULL OR deelnemers_slapers IS NOT NULL OR deelnemers_gepensioneerd IS NOT NULL)
    """
    df = pd.read_sql_query(query, conn)
    
    updates = 0
    for _, row in df.iterrows():
        # Fill NAs with 0 for summation
        actief = row['deelnemers_actief'] if pd.notna(row['deelnemers_actief']) else 0
        slapers = row['deelnemers_slapers'] if pd.notna(row['deelnemers_slapers']) else 0
        gepensioneerd = row['deelnemers_gepensioneerd'] if pd.notna(row['deelnemers_gepensioneerd']) else 0
        
        # We only want to sum if we have at least ALL THREE to be accurate, 
        # or if we are confident the others are actually 0.
        # But DNB data often splits it nicely. Let's just sum them if any are present.
        total = int(actief + slapers + gepensioneerd)
        
        if total > 0:
            cursor.execute("UPDATE funds SET deelnemers_totaal = ? WHERE id = ?", (total, row['id']))
            updates += 1
            print(f"Updated {row['name']}: Total = {total} (Actief: {actief}, Slapers: {slapers}, Gepensioneerd: {gepensioneerd})")
    
    conn.commit()
    conn.close()
    print(f"\nSuccessfully aggregated and updated {updates} funds with total participant counts.")

if __name__ == "__main__":
    sum_participants()
