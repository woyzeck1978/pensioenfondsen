import os
import sqlite3
import re
import fitz # PyMuPDF
from concurrent.futures import ProcessPoolExecutor, as_completed

# Database and directory setup
DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def parse_int(val_str):
    try:
        clean = val_str.replace('.', '').replace(' ', '')
        val = int(clean)
        # Avoid treating years as demographics
        if 2010 <= val <= 2030 or val < 10:
            return None
        return val
    except:
        return None

def parse_float(val_str):
    try:
        clean = val_str.replace('.', '').replace(',', '.')
        return float(clean)
    except:
        return None

def extract_text_from_pdf(pdf_path, max_pages=120):
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(min(max_pages, len(doc))):
            text += doc.load_page(page_num).get_text() + " "
    except Exception as e:
        pass
    return text.lower().replace('\\n', ' ')

def extract_metric(text, keyword_patterns):
    # Search for the keyword, then find the nearest number in a window
    # Try looking ahead up to 150 chars, and behind up to 150 chars
    num_pattern = re.compile(r'(?<!\S)(\d{1,3}(?:\.\d{3})+|\d{2,6})(?!\S)')
    
    best_val = None
    best_dist = 999
    
    for kp in keyword_patterns:
        matches = re.finditer(kp, text)
        for m in matches:
            # Lookahead window
            window_ahead = text[m.end():min(len(text), m.end()+150)]
            nums_ahead = num_pattern.findall(window_ahead)
            if nums_ahead:
                val = parse_int(nums_ahead[0])
                if val: return val
                
            # Lookbehind window (in case it's a table where numbers render first)
            window_behind = text[max(0, m.start()-150):m.start()]
            nums_behind = num_pattern.findall(window_behind)
            if nums_behind:
                # Take the closest one (the last in the list)
                val = parse_int(nums_behind[-1])
                if val: return val
                
    return best_val

def analyze_demographics(filename):
    pdf_path = os.path.join(REPORTS_DIR, filename)
    fund_name_from_file = filename.replace('.pdf', '')
    try:
        fund_id = int(fund_name_from_file.split('_')[0])
    except ValueError:
        return None

    text = extract_text_from_pdf(pdf_path)
    if not text: return None

    # Demographics
    actief = extract_metric(text, [r'(actieve deelnemers|actieven|werknemers)'])
    slapers = extract_metric(text, [r'(gewezen deelnemers|slapers|oud-werknemers)'])
    gepensioneerd = extract_metric(text, [r'(pensioengerechtigden|gepensioneerden)'])
    totaal = extract_metric(text, [r'(totaal aantal deelnemers|totaal deelnemers|totaal verzekerden)'])
    
    # Uitvoeringskosten
    # Often expressed in millions (1.4 or 1,4 or '1.483' in thousands)
    uk_miljoenen = None
    uk_per_deelnemer = None
    
    # Exec costs per participant
    ukd_matches = re.finditer(r'(uitvoeringskosten per deelnemer|kosten per deelnemer|uitvoeringskosten|kostprijs per deelnemer)(.{0,200})', text)
    best_ukd = []
    for m in ukd_matches:
        window = m.group(2)
        nums = re.findall(r'(?:€|eur)?\s*(\d{2,4})(?!\S)', window)
        for n in nums:
            val = float(n)
            if not (2010 <= val <= 2030):
                best_ukd.append(val)
    if best_ukd:
        uk_per_deelnemer = best_ukd[0]

    # Exec costs total
    ukt_matches = re.finditer(r'(pensioenuitvoeringskosten|totale uitvoeringskosten|uitvoeringskosten)(.{0,200})', text)
    best_ukm = []
    for m in ukt_matches:
        window = m.group(2)
        nums = re.findall(r'(?:€|eur)?\s*(\d{1,4}[,\.]\d{1,3}|\d{1,4})(?=\s|m|miljoen|duizend|$)', window)
        for n in nums:
            val = parse_float(n)
            if val and not (2010 <= val <= 2030) and not (2.010 <= val <= 2.030):
                if val < 100: 
                    best_ukm.append(val)
                elif val > 100000: 
                    best_ukm.append(val / 1000000.0)
                else: 
                    best_ukm.append(val / 1000.0)
    if best_ukm:
        uk_miljoenen = best_ukm[0]

    return fund_id, actief, slapers, gepensioneerd, totaal, uk_miljoenen, uk_per_deelnemer

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    pdf_files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    print(f"Analyzing {len(pdf_files)} reports for demographics and uitvoeringskosten...")

    results = []
    # Using ThreadPoolExecutor heavily to process PDFs concurrently without over-consuming RAM on large PDFs
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(analyze_demographics, f): f for f in pdf_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            filename = futures[future]
            try:
                res = future.result()
                if res is not None:
                    fund_id, act, slp, gep, tot, ukm, ukd = res
                    results.append((act, slp, gep, tot, ukm, ukd, fund_id))
                    print(f"[{i}/{len(pdf_files)}] {filename} -> Act: {act} | Slp: {slp} | Gep: {gep} | UVK Mln: {ukm} | UVK pD: {ukd}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    c.executemany("UPDATE funds SET deelnemers_actief = ?, deelnemers_slapers = ?, deelnemers_gepensioneerd = ?, deelnemers_totaal = ?, uitvoeringskosten_miljoenen = ?, uitvoeringskosten_per_deelnemer = ? WHERE id = ?", results)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
