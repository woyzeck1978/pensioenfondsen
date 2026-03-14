import sqlite3

db_path = '../../data/processed/pension_funds.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute("SELECT id, name FROM funds WHERE category IS NULL OR category = ''")
missing_cats = c.fetchall()

success_count = 0

for fund_id, name in missing_cats:
    lower_name = name.lower()
    cat = None
    
    if 'kring' in lower_name or '(stap)' in lower_name or '(hnp' in lower_name or '(de nationale)' in lower_name or '(unilever)' in lower_name:
        cat = 'APF'
    elif 'bpf' in lower_name or 'bpl' in lower_name or 'levensmiddelen' in lower_name or 'binnenvaart' in lower_name:
        cat = 'Tak'
    elif 'iff' in lower_name:
        cat = 'Bedrijf'
        
    if cat:
        c.execute("UPDATE funds SET category = ? WHERE id = ?", (cat, fund_id))
        success_count += 1
        print(f"[{fund_id}] {name} -> {cat}")

conn.commit()
conn.close()
print(f"Successfully auto-categorized {success_count} / {len(missing_cats)} funds.")
