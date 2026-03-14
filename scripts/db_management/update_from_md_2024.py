import os
import sqlite3
import re

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
md_path = os.path.join(base_dir, "equity_strategies_2024.md")
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")

def parse_md():
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    funds_data = {}
    current_fund = None
    
    lines = content.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Match fund name
        if line.startswith('## '):
            current_fund = line[3:].strip()
            funds_data[current_fund] = {'allocation': None, 'notes': ""}
        
        elif current_fund:
            if line.startswith('- **Equity Allocation:**'):
                alloc_str = line.split(':**')[1].strip()
                if alloc_str != 'Unknown' and '%' in alloc_str:
                    try:
                        funds_data[current_fund]['allocation'] = float(alloc_str.replace('%', '').strip())
                    except ValueError:
                        pass
            
            elif line.startswith('- **Strategy Notes:**'):
                notes = []
                i += 1
                while i < len(lines) and lines[i].startswith('> '):
                    notes.append(lines[i][2:].strip())
                    i += 1
                funds_data[current_fund]['notes'] = " ".join(notes)
                continue # since we incremented i
                
        i += 1
        
    return funds_data

def get_best_match(db_cursor, fund_name):
    # Try exact match 
    db_cursor.execute("SELECT id, name FROM funds WHERE name = ?", (fund_name,))
    row = db_cursor.fetchone()
    if row: return row[0]
    
    # Try case insensitive match or LIKE
    db_cursor.execute("SELECT id, name FROM funds WHERE name LIKE ?", (f"%{fund_name}%",))
    rows = db_cursor.fetchall()
    
    if len(rows) == 1:
        return rows[0][0]
    elif len(rows) > 1:
        # If there are multiple, maybe it's tricky, we can try to find an exact substring
        for r in rows:
            if r[1].lower() == fund_name.lower():
                return r[0]
                
    # Try looking for parentheses
    idx = fund_name.find('(')
    if idx != -1:
        base = fund_name[:idx].strip()
        db_cursor.execute("SELECT id, name FROM funds WHERE name LIKE ?", (f"%{base}%",))
        rows = db_cursor.fetchall()
        if len(rows) == 1:
            return rows[0][0]
            
    # Try split by slash: "APF (AkzoNobel, Nouryon, Nobian, Salt)", etc.
    if '/' in fund_name:
        parts = fund_name.split('/')
        for p in parts:
            db_cursor.execute("SELECT id, name FROM funds WHERE name LIKE ?", (f"%{p.strip()}%",))
            rows = db_cursor.fetchall()
            if len(rows) == 1:
                return rows[0][0]
                
    return None

def update_db(funds_data):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    updated_allocs = 0
    updated_notes = 0
    not_found = []
    
    for fund_name, data in funds_data.items():
        fund_id = get_best_match(cursor, fund_name)
        if not fund_id:
            # Add some manual edge cases common in this dataset
            if fund_name == 'KLM Algemeen (Ground Staff)':
                fund_id = get_best_match(cursor, 'KLM') # Need to be careful. Let's find specific text.
                cursor.execute("SELECT id FROM funds WHERE name LIKE '%Algemeen%KLM%'")
                row = cursor.fetchone()
                if row: fund_id = row[0]
            elif fund_name == 'KLM Vliegend Personeel (Flight Crew)':
                cursor.execute("SELECT id FROM funds WHERE name LIKE '%Vliegend Personeel%'")
                row = cursor.fetchone()
                if row: fund_id = row[0]
            elif fund_name == 'KLM Cabinepersoneel (Cabin Crew)':
                cursor.execute("SELECT id FROM funds WHERE name LIKE '%Cabinepersoneel%'")
                row = cursor.fetchone()
                if row: fund_id = row[0]
                
        if fund_id:
            alloc = data.get('allocation')
            notes = data.get('notes')
            
            if alloc is not None:
                cursor.execute("UPDATE funds SET equity_allocation_pct = ? WHERE id = ?", (alloc, fund_id))
                updated_allocs += 1
            if notes:
                cursor.execute("UPDATE funds SET equity_strategy_notes = ? WHERE id = ?", (notes, fund_id))
                updated_notes += 1
        else:
            not_found.append(fund_name)
            
    conn.commit()
    conn.close()
    
    print(f"Updated {updated_allocs} allocation percentages and {updated_notes} strategy notes.")
    if not_found:
        print("\nCould not cleanly match the following funds in DB:")
        for nf in not_found:
            print(f" - {nf}")

if __name__ == "__main__":
    data = parse_md()
    print(f"Found {len(data)} funds in markdown.")
    update_db(data)
