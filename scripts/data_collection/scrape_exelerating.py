import pandas as pd
import requests
import sqlite3
import io

url = "https://exelerating.com/nl/insights/overzicht-dekkingsgraad-pensioenfondsen/"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    # Read all tables from the HTML
    tables = pd.read_html(io.StringIO(response.text))
    
    if not tables:
        print("No tables found using pandas.")
        exit(1)
        
    df = tables[0]
    
    # Clean column names
    df.columns = ['Pensioenfonds', 'Datum', 'Dekkingsgraad', 'Beleidsdekkingsgraad']
    
    updates = []
    for _, row in df.iterrows():
        name = str(row.iloc[0]).strip()
        dek_str = str(row.iloc[2]).strip().replace('%', '').replace(',', '.')
        bel_str = str(row.iloc[3]).strip().replace('%', '').replace(',', '.')
        
        if name != 'nan' and dek_str != 'nan' and bel_str != 'nan':
            try:
                dek = float(dek_str)
                bel = float(bel_str)
                updates.append((name, dek, bel))
            except ValueError:
                pass
                
    print(f"Extracted {len(updates)} valid rows.")
    
    # Connect to DB
    conn = sqlite3.connect('../../data/processed/pension_funds.db')
    cursor = conn.cursor()
    updated_count = 0
    
    for name, dek, bel in updates:
        search_term = name.replace("Pensioenfonds ", "").replace("Stichting ", "").replace("SPF ", "").strip()
        cursor.execute("SELECT id, name, dekkingsgraad_pct FROM funds WHERE name LIKE ?", ('%'+search_term+'%',))
        matches = cursor.fetchall()
        
        # Exact Fallbacks
        if not matches:
            if "ABP" in name: search_term = "ABP"
            elif "PFZW" in name: search_term = "PFZW"
            elif "KLM" in name: search_term = "KLM"
            elif "ING" in name: search_term = "ING"
            elif "Hoogovens" in name: search_term = "Hoogovens"
            elif "Achmea" in name: search_term = "Achmea"
            elif "BPL" in name: search_term = "BPL Pensioen"
            
            cursor.execute("SELECT id, name, dekkingsgraad_pct FROM funds WHERE name LIKE ?", ('%'+search_term+'%',))
            matches = cursor.fetchall()
            
        for match in matches:
            fund_id, fund_name, current_dek = match
            # ONLY UPDATE IF IT IS EMPTY OR ZERO SO WE DONT OVERWRITE OUR HARD WORK
            if not current_dek or current_dek == 0.0 or current_dek == "None":
                print(f"Updating missing coverage ratio for DB Fund '{fund_name}' (Matched: '{name}') to {dek}% / {bel}%")
                cursor.execute("UPDATE funds SET dekkingsgraad_pct = ?, maanddekkingsgraad_pct = ?, beleidsdekkingsgraad_pct = ?, data_source = 'Exelerating.com Bulk' WHERE id = ?", (dek, dek, bel, fund_id))
                updated_count += 1
                
    conn.commit()
    conn.close()
    print(f"Successfully updated {updated_count} fund records in the database.")

except Exception as e:
    print(f"Error: {e}")
