import sqlite3
import re
from pypdf import PdfReader

pdf_path = "../data/PensioenPro/Overzicht-pensioentransitie-per-pensioenfonds-11-2-2026.pdf"
reader = PdfReader(pdf_path)

conn = sqlite3.connect('../../data/processed/pension_funds.db')
cursor = conn.cursor()
cursor.execute("SELECT id, name FROM funds")
funds = cursor.fetchall()

# Regex to capture the row logic
# Example 1: BPL 2027 Van 1-1-2026 naar 1-1-2027solidair ja 699,106TKP
# Example 2: BpfBouw 2026 solidair ja 743,122APG
# Example 3: Foodservice 2027 (april) Van 1-1-2026 naar 1-4-2027solidair ja 62,493AZL
# Example 4: PNO Media 2027 Van 1-1-2026 naar 1-1-2027biedt beide aan ja 67,808Zelfadministrerend
pattern = re.compile(r'^(.+?)\s+(202[4-8](?:\s*\([a-z]+\))?|onbekend)\s+(?:Van.*?naar.*?|Pensioenen.*?)?(solidair|flexibel(?:\s*\+\s*rdr)?|biedt beide aan|onbekend)\s+(ja|nee|onbekend)\s+([\d,.]+)([A-Za-z].*)$', re.IGNORECASE)

updates = 0
found_lines = 0

print("--- Parsing Massive Overzicht PDF ---")
for page in reader.pages:
    text = page.extract_text()
    for line in text.split('\n'):
        line = line.strip()
        if not line or "Naam fonds" in line or "Deelnemers" in line: continue
        
        match = pattern.match(line)
        if match:
            found_lines += 1
            fund_name = match.group(1).strip()
            trans_date = match.group(2).strip()
            contract = match.group(3).strip()
            invaren = match.group(4).strip()
            deelnemers = match.group(5).strip().replace('.', '').replace(',', '') # convert 3.013.496 to 3013496
            uitvoerder = match.group(6).strip()
            
            # Map into database
            target_clean = fund_name.lower().replace("pensioenfonds", "").replace(" ", "")
            best_match_id = None
            
            for f_id, f_name in funds:
                db_clean = f_name.lower().replace("pensioenfonds", "").replace("stichting", "").replace(" ", "")
                if len(target_clean) > 3 and (target_clean in db_clean or db_clean in target_clean):
                    best_match_id = f_id
                    break
            
            if best_match_id:
                # Update the DB if we found a match
                # wtp_deelnemers_k needs to be in thousands (k)
                deelnemers_k = round(int(deelnemers) / 1000.0) if deelnemers.isdigit() else None
                
                cursor.execute("""
                    UPDATE funds SET 
                    wtp_transitie_datum=COALESCE(?, wtp_transitie_datum),
                    wtp_contract_type=?,
                    wtp_invaren=?,
                    wtp_deelnemers_k=COALESCE(?, wtp_deelnemers_k),
                    uitvoerder=COALESCE(?, uitvoerder)
                    WHERE id=?
                """, (trans_date, contract, invaren, deelnemers_k, uitvoerder, best_match_id))
                updates += 1

conn.commit()
conn.close()
print(f"Matched {found_lines} valid rows from the PDF.")
print(f"Successfully applied {updates} massive WTP updates to the database.")
