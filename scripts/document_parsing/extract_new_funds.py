import os
import re
import fitz  # PyMuPDF
import sqlite3
import csv

# We got the list from the previous run
files = [
    (6, "6_Openbare Apothekers SPOA - Public Pharmacists.pdf"),
    (11, "11_Bakkersbedrijf Bakeries.pdf"),
    (14, "14_Beton  Betonproductenindustrie Concrete Industry.pdf"),
    (24, "24_Metaal  Techniek  PMT Metal  Technique.pdf"),
    (34, "34_Recreatie.pdf"),
    (37, "37_Waterbouw Hydraulic Engineering.pdf"),
    (45, "45_IBM (SPIN) Nederland.pdf"),
    (50, "50_Mediahuis Nederland Mhpf.pdf"),
    (79, "79_Avebe.pdf"),
    (91, "91_DSM (PDN) Nederland_full.pdf"), # just use the full one
    (152, "152_BPF Foodservice.pdf"),
    (165, "165_Kring Arcadis Hnp.pdf"),
    (167, "167_Kring CK1 Hnp.pdf"),
    (173, "173_Kring McCain De Nationale.pdf"),
    (177, "177_Kring TotalEnergies NL Stap.pdf"),
    (178, "178_Levensmiddelenbedrijf.pdf"),
    (187, "187_Pensioenkring CRH Hnp.pdf"),
    (193, "193_Pensioenkring Van Lanschot Hnp.pdf"),
]

reports_dir = "data/reports"

found_allocations = []

# Keywords that commonly denote equity allocation percentage in Dutch reports
# "aandelen" = equities. Let's look for "aandelen" followed by a percentage near it.
# e.g. "aandelen 35%" or "zakelijke waarden 40%"

for fund_id, filename in files:
    path = os.path.join(reports_dir, filename)
    try:
        doc = fitz.open(path)
        found_pct = None
        
        # We'll just extract all text and do regex, or page by page
        for i in range(len(doc)):
            page = doc.load_page(i)
            text = page.get_text()
            
            # Look for lines mentioning "Aandelen" and a "%"
            lines = text.split('\n')
            for j, line in enumerate(lines):
                if re.search(r'aandelen|zakelijk', line, re.IGNORECASE):
                    # Check if line itself has a percentage
                    pct_match = re.search(r'(\d+[\.,]?\d*)\s*%', line)
                    if pct_match:
                        val_str = pct_match.group(1).replace(',', '.')
                        try:
                            val = float(val_str)
                            if 5 <= val <= 95: # plausible equity allocation
                                found_pct = val
                                break
                        except:
                            pass
                    else:
                        # Check adjacent lines
                        start = max(0, j-2)
                        end = min(len(lines), j+3)
                        for k in range(start, end):
                            pct_match2 = re.search(r'^(\d+[\.,]?\d*)\s*%$', lines[k].strip())
                            if pct_match2:
                                val_str = pct_match2.group(1).replace(',', '.')
                                try:
                                    val = float(val_str)
                                    if 5 <= val <= 95:
                                        found_pct = val
                                        break
                                except:
                                    pass
                        if found_pct:
                            break
            if found_pct:
                break
                
        if found_pct is not None:
            print(f"[{fund_id}] {filename} -> {found_pct}%")
            found_allocations.append((fund_id, found_pct, filename))
        else:
            print(f"[{fund_id}] {filename} -> NOT FOUND")
            
    except Exception as e:
        print(f"Error reading {filename}: {e}")

print(f"\nFound {len(found_allocations)} allocations out of {len(files)}.")

if found_allocations:
    # Append to CSV
    with open("../../data/processed/extracted_allocations.csv", "a", newline="") as f:
        writer = csv.writer(f)
        for fid, pct, fname in found_allocations:
            writer.writerow([fid, f"{pct:.2f}", fname])
            
    # Update DB
    conn = sqlite3.connect("../../data/processed/pension_funds.db")
    cursor = conn.cursor()
    for fid, pct, fname in found_allocations:
        cursor.execute('''
            INSERT INTO equity_allocations_extracted (fund_id, allocation_pct, source_file) 
            VALUES (?, ?, ?) 
            ON CONFLICT(fund_id) DO UPDATE SET allocation_pct=?, source_file=?;
        ''', (fid, pct, fname, pct, fname))
    conn.commit()
    conn.close()
    print("Database and CSV updated.")
