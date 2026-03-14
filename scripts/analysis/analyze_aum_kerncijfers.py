import os
import sqlite3
import fitz
import re

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

# Match "belegd vermogen", "voor pensioen beschikbaar", "totaal beleggingen"
AUM_PATTERN = re.compile(r'(belegd\\s+vermogen|beschikbaar\\s+vermogen|beleggingen[\\s\\w]*totaal|vermogen\\s+aan\\s+het\\s+eind)', re.IGNORECASE)

# Match currency amounts like 285.340, 2.345, 1,2 miljard, 345 mln
CURRENCY_PATTERN = re.compile(r'(?:€|eur)?\\s*(\\d{1,3}(?:[.,]\\d{3})*(?:[.,]\\d+)?)\\s*(miljard|mln|miljoen|mrd)?', re.IGNORECASE)

def parse_aum_from_text(text):
    lines = text.split('\\n')
    
    for i, line in enumerate(lines):
        if AUM_PATTERN.search(line):
            # Often the value is on the same line or the next few lines in a table
            context = " ".join(lines[i:i+3])
            
            matches = CURRENCY_PATTERN.findall(context)
            if matches:
                for match in matches:
                    val_str, multiplier = match
                    # Clean the number string
                    val_str = val_str.replace('.', '').replace(',', '.')
                    try:
                        val = float(val_str)
                    except ValueError:
                        continue
                        
                    # Handle multipliers
                    mult_lower = multiplier.lower() if multiplier else ""
                    if 'miljard' in mult_lower or 'mrd' in mult_lower:
                        return val  # already in billions
                    elif 'miljoen' in mult_lower or 'mln' in mult_lower:
                        return val / 1000.0  # convert to billions
                    else:
                        # If no multiplier is specified and the number is huge (e.g. 285000000), it's probably raw euros
                        if val > 1000000:
                            return val / 1000000000.0
                        elif val > 100:
                            # Could be millions specified in the table header, e.g., "bedragen in miljoenen". Assume millions.
                            return val / 1000.0
                        else:
                            # Could be billions specified in the table header, e.g., "bedragen in miljarden". Assume billions.
                            return val
    
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    pdf_files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    updates = []
    
    print(f"Scanning {len(pdf_files)} reports for precise Kerncijfers AUM...")
    
    for i, file in enumerate(pdf_files):
        fund_id_str = file.split('_')[0]
        try:
            fund_id = int(fund_id_str)
        except ValueError:
            continue
            
        filepath = os.path.join(REPORTS_DIR, file)
        
        try:
            doc = fitz.open(filepath)
            
            # Key figures are almost always in the first 10 pages
            num_pages = min(10, len(doc))
            
            found_aum = None
            
            for page_num in range(num_pages):
                page = doc.load_page(page_num)
                # Try layout-preserving text extraction first as it's better for tables
                text = page.get_text("text") 
                
                # If the page mentions 'kerncijfers' or 'overzicht', it's a prime target
                if 'kerncijfers' in text.lower() or 'in vogelvlucht' in text.lower():
                    found_aum = parse_aum_from_text(text)
                    if found_aum:
                        break
                        
                # Even if not explicitly a kerncijfer page, an AUM table could be anywhere
                if not found_aum:
                    found_aum = parse_aum_from_text(text)
                    if found_aum:
                        break

            if found_aum:
                # Sanity check: cap at 600B (ABP is largest around ~500B), flooring at 0.01B (10m)
                if 0.01 <= found_aum <= 600.0:
                    print(f"[{i+1}/{len(pdf_files)}] {file} -> Extracted AUM: €{found_aum:.3f} bn")
                    updates.append((found_aum, fund_id))
                else:
                    print(f"[{i+1}/{len(pdf_files)}] {file} -> Extracted AUM {found_aum} out of bounds, skipping")
            else:
                print(f"[{i+1}/{len(pdf_files)}] {file} -> No Kerncijfers AUM found")

        except Exception as e:
            print(f"[{i+1}/{len(pdf_files)}] Error analyzing {file}: {e}")

    if updates:
        c.executemany("UPDATE funds SET aum_euro_bn = ? WHERE id = ?", updates)
        conn.commit()
        print(f"\\nSuccessfully updated AUM for {len(updates)} funds based on Kerncijfers.")
    else:
        print("\\nNo precise AUMs could be parsed.")
        
    conn.close()

if __name__ == "__main__":
    main()
