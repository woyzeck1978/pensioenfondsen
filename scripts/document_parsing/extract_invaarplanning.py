import sqlite3
import os
import re
from pypdf import PdfReader

# The first PDF contains the specific transition dates: e.g. 1-apr-26, 1-jul-26
pdf_path = "../data/PensioenPro/Invaarplanning-van-april-tm-oktober-2026.pdf"
reader = PdfReader(pdf_path)
text = reader.pages[0].extract_text()

lines = text.split('\n')

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()

# Get all DB funds
cursor.execute("SELECT id, name FROM funds")
funds = cursor.fetchall()

def clean_name(n):
    return String(n).lower().replace("pensioenfonds", "").replace("stichting", "").replace("(gesloten)", "").replace(" ", "").strip()

current_date = None
date_pattern = re.compile(r'^(\d{1,2}-[a-z]{3}-\d{2})')

updates = 0

print("--- Parsing Specific Transition Dates ---")
for line in lines:
    line = line.strip()
    if not line: continue
    
    # Check if line starts with a date like 1-apr-26
    match = date_pattern.match(line.lower())
    if match:
        current_date = match.group(1)
        # The rest of the line contains the first fund (e.g. "1-apr-26 3 Mitt AZL")
        # Remove the date and the count number
        rest = line[len(current_date):].strip()
        rest = re.sub(r'^\d+\s+', '', rest) # drop the count
        fund_text = rest
    else:
        # If it doesn't start with a date, it inherits the current_date
        fund_text = line
        
    if current_date and fund_text:
        # Stop at "Totaal: 17"
        if "totaal" in fund_text.lower(): break
            
        print(f"[{current_date}] Hunting for: '{fund_text}'")
        
        # We need to extract just the fund name. Typical line: "PostNL TKP Ja"
        # We can just use the whole line for fuzzy DB search
        target_clean = fund_text.lower().replace("pensioenfonds", "").replace(" ", "")
        
        best_match_id = None
        for f_id, f_name in funds:
            db_clean = f_name.lower().replace("pensioenfonds", "").replace("stichting", "").replace(" ", "")
            # e.g "kringowase(hnpf)" in "kringowase(hnpf)dionja"
            if len(db_clean) > 4 and db_clean in target_clean:
                best_match_id = f_id
                break
        
        if not best_match_id:
            # Reverse Check: Is a significant part of the target in the db name?
            words = fund_text.split()
            if len(words) > 0:
                core_name = words[0].lower().replace("kring", "").strip()
                if len(core_name) > 3:
                    for f_id, f_name in funds:
                        db_clean = f_name.lower()
                        if core_name in db_clean:
                            best_match_id = f_id
                            break
                            
        if best_match_id:
            print(f"  => Matched with DB ID: {best_match_id}")
            # Format the date nicely to match the DB format (if desired)
            cursor.execute("UPDATE funds SET wtp_transitie_datum=? WHERE id=?", (current_date, best_match_id))
            updates += 1
        else:
            print("  => NO MATCH FOUND IN DB")

conn.commit()
conn.close()
print(f"\nSuccessfully assigned {updates} highly-specific transition dates to the database.")
