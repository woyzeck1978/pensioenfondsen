import sqlite3
import fitz
import re
import os
import glob
from collections import defaultdict

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

# Connect to DB
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Make sure table exists (if script is run independently)
cursor.execute('''
CREATE TABLE IF NOT EXISTS historical_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fund_id INTEGER,
    year INTEGER,
    beleggingsrendement_pct REAL,
    economische_dekkingsgraad_pct REAL,
    nominale_dekkingsgraad_pct REAL,
    beleidsdekkingsgraad_pct REAL,
    reele_dekkingsgraad_pct REAL,
    indexatieverlening_pct REAL,
    cpi_pct REAL,
    FOREIGN KEY(fund_id) REFERENCES funds(id),
    UNIQUE(fund_id, year)
);
''')
conn.commit()

# Get mapping of Fund IDs to Names based on downloaded PDFs
files = glob.glob(f"{REPORTS_DIR}/*.pdf")
fund_files = {}
for f in files:
    basename = os.path.basename(f)
    if "Transitieplan" in basename:
        continue
    
    match = re.search(r'^(\d+)_', basename)
    if match:
        fund_id = int(match.group(1))
        fund_files[fund_id] = f

print(f"Found {len(fund_files)} annual report PDFs.")

def clean_percentage(text):
    if not text:
        return None
    # Extract just the number
    match = re.search(r'(-?\d+[,.]\d+|-?\d+)', str(text))
    if match:
        val = match.group(1)
        # Handle European decimal formatting
        val = val.replace(',', '.')
        try:
            return float(val)
        except ValueError:
            return None
    return None

def analyze_pdf_for_historical_metrics(pdf_path, fund_id):
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"Error opening {pdf_path}: {e}")
        return

    table_keywords = ["kerncijfers", "meerjarenoverzicht", "sleutelcijfers", "ontwikkeling"]
    target_years = ['2024', '2023', '2022', '2021', '2020']
    
    extracted_data = {year: {
        'beleggingsrendement_pct': None,
        'economische_dekkingsgraad_pct': None,
        'nominale_dekkingsgraad_pct': None,
        'beleidsdekkingsgraad_pct': None,
        'reele_dekkingsgraad_pct': None,
        'indexatieverlening_pct': None,
        'cpi_pct': None
    } for year in target_years}

    num_pages = min(20, len(doc))
    
    for i in range(num_pages):
        page = doc[i]
        raw_text = page.get_text().lower()
        
        # Check if page likely contains the key figures table
        if any(kw in raw_text for kw in table_keywords) and all(str(yr) in raw_text for yr in [2022, 2023]):
            
            blocks = page.get_text("dict")["blocks"]

            # Group text spans by their vertical block / Y-coordinate to reconstruct rows
            rows = defaultdict(list)
            for b in blocks:
                if "lines" in b:
                    for l in b["lines"]:
                        for s in l["spans"]:
                            # Round Y0 to nearest 5 pixels to group same-line items
                            y_coord = round(s["bbox"][1] / 5.0) * 5
                            rows[y_coord].append((s["bbox"][0], s["text"].strip()))

            sorted_y = sorted(rows.keys())

            # Find the row with years
            year_row = None
            year_positions = [] # list of (x_coord, year_str)
            for y in sorted_y:
                items = sorted(rows[y], key=lambda x: x[0])
                years = []
                for x_ptr, txt in items:
                    if re.match(r'^202[0-4]$', txt):
                        years.append((x_ptr, txt))
                if len(years) >= 3:
                    year_row = y
                    year_positions = years
                    break

            if not year_row:
                continue

            print(f"  -> Found 'Kerncijfers' table on page {i+1} for Fund {fund_id}")

            metrics_keywords = {
                'beleggingsrendement_pct': ['beleggingsrendement', 'rendement'],
                'economische_dekkingsgraad_pct': ['actuele dekkingsgraad', 'economische dekkingsgraad'],
                'nominale_dekkingsgraad_pct': ['nominale dekkingsgraad'],
                'beleidsdekkingsgraad_pct': ['beleidsdekkingsgraad'],
                'reele_dekkingsgraad_pct': ['reële dekkingsgraad', 'reele dekkingsgraad'],
                'indexatieverlening_pct': ['indexatie', 'toeslag', 'verhoging'],
                'cpi_pct': ['prijsinflatie', 'cpi', 'consumentenprijsindex', 'prijsstijging']
            }

            for y in sorted_y:
                if y <= year_row: continue # Skip headers and above
                
                items = sorted(rows[y], key=lambda x: x[0])
                row_text = " ".join([txt for _, txt in items]).lower()
                
                for metric, kws in metrics_keywords.items():
                    if any(kw in row_text for kw in kws):
                        
                        numbers = []
                        for x_ptr, txt in items:
                            cln_txt = txt.replace('%','').strip()
                            if re.match(r'^-?\d+[,.]?\d*$', cln_txt):
                                numbers.append((x_ptr, cln_txt))
                        
                        for nx, ntxt in numbers:
                            # Map to closest year horizontally
                            closest_year = min(year_positions, key=lambda yp: abs(yp[0] - nx))[1]
                            
                            if extracted_data[closest_year][metric] is None:
                                extracted_data[closest_year][metric] = clean_percentage(ntxt)
                                
            # If we found at least some data, we can stop searching pages
            if any(extracted_data['2023'].values()):
                break
                
    # Insert to DB
    records_inserted = 0
    for year, metrics in extracted_data.items():
        if any(metrics.values()):
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO historical_metrics 
                    (fund_id, year, beleggingsrendement_pct, economische_dekkingsgraad_pct, 
                    nominale_dekkingsgraad_pct, beleidsdekkingsgraad_pct, reele_dekkingsgraad_pct, 
                    indexatieverlening_pct, cpi_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    fund_id, int(year), 
                    metrics['beleggingsrendement_pct'],
                    metrics['economische_dekkingsgraad_pct'],
                    metrics['nominale_dekkingsgraad_pct'],
                    metrics['beleidsdekkingsgraad_pct'],
                    metrics['reele_dekkingsgraad_pct'],
                    metrics['indexatieverlening_pct'],
                    metrics['cpi_pct']
                ))
                records_inserted += 1
            except Exception as e:
                print(f"Error inserting for Fund {fund_id} Year {year}: {e}")
                
    if records_inserted > 0:
        print(f"  -> Inserted {records_inserted} historical records for Fund ID {fund_id}")

# Run analysis
processed_count = 0
for fund_id, filepath in fund_files.items():
    # Skip ABN Amro since we already manually verified and inserted its data perfectly.
    if fund_id == 71:
        continue
        
    processed_count += 1
    analyze_pdf_for_historical_metrics(filepath, fund_id)
    
    # Save every 5 funds
    if processed_count % 5 == 0:
        conn.commit()

conn.commit()
conn.close()
print("Historical metrics extraction complete.")
