import pandas as pd
import sqlite3

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

cursor.execute("SELECT id, name, category, uitvoerder, data_source FROM funds")
funds = cursor.fetchall()

def clean_name(n):
    return String(n).lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace(" ", "").strip()

try:
    wtp = pd.read_excel('../data/Overzicht pensioentransitie wtp.xlsx', header=4)
    
    updated_cat = 0
    updated_uit = 0
    
    for _, row in wtp.iterrows():
        raw_name = str(row.iloc[2]).strip()
        if raw_name == 'nan': continue
            
        target_cat = str(row.iloc[1]).strip() if pd.notnull(row.iloc[1]) else None
        target_uit = str(row.iloc[15]).strip() if pd.notnull(row.iloc[15]) else None
        
        target_clean = raw_name.lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace(" ", "").strip()
        
        # Find matching DB fund
        for f_id, f_name, db_cat, db_uit, ds in funds:
            db_clean = f_name.lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace(" ", "").strip()
            
            if target_clean == db_clean or (len(target_clean) > 5 and target_clean in db_clean) or (len(db_clean) > 5 and db_clean in target_clean):
                # We have a match! Let's update if null
                
                # Update Category
                if target_cat and target_cat.lower() != 'nan':
                    if not db_cat or str(db_cat).strip() == '' or str(db_cat).lower() == 'nan' or str(db_cat).lower() == 'none' or ds == 'DNB Appendix':
                        pass_cat = target_cat
                        # Map wtp types to our standard DB categories if possible
                        if 'bedrijf' in pass_cat.lower(): pass_cat = 'Bedrijfstakpensioenfonds'
                        elif 'beroep' in pass_cat.lower(): pass_cat = 'Beroepspensioenfonds'
                        elif 'onderneming' in pass_cat.lower(): pass_cat = 'Ondernemingspensioenfonds'
                        elif 'algemeen' in pass_cat.lower(): pass_cat = 'APF'
                        
                        cursor.execute("UPDATE funds SET category = ? WHERE id = ?", (pass_cat, f_id))
                        updated_cat += 1
                
                # Update Uitvoerder
                if target_uit and target_uit.lower() != 'nan':
                    if not db_uit or str(db_uit).strip() == '' or str(db_uit).lower() == 'nan' or str(db_uit).lower() == 'none':
                        cursor.execute("UPDATE funds SET uitvoerder = ? WHERE id = ?", (target_uit, f_id))
                        updated_uit += 1
                
                break # Move to next WTP row

    conn.commit()
    print(f"Successfully populated {updated_cat} empty 'Category' fields and {updated_uit} empty 'Uitvoerder' allocations from the WTP Feed.")

except Exception as e:
    print("Error parsing WTP feed:", e)

conn.close()
