import os
import sqlite3
import re
import fitz # PyMuPDF
from concurrent.futures import ProcessPoolExecutor, as_completed

# Database and directory setup
DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def extract_text_from_pdf(pdf_path):
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            text += page.get_text() + " "
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
    return text.lower()

def analyze_report(filename):
    pdf_path = os.path.join(REPORTS_DIR, filename)
    fund_name_from_file = filename.replace('.pdf', '')
    try:
        fund_id = int(fund_name_from_file.split('_')[0])
    except ValueError:
        return None

    text = extract_text_from_pdf(pdf_path)
    if not text:
        return None

    # Beheerkosten logic
    vermogensbeheerkosten = "Unknown"
    transactiekosten = "Unknown"
    
    # Vermogensbeheerkosten
    # Look for the word, then within ~400 chars (to jump across table columns), look for a percentage or bps
    matches = re.finditer(r'(vermogensbeheerkosten|kosten vermogensbeheer|beheerkosten|uitvoeringskosten)(.{0,400})', text)
    best_vcost = []
    for match in matches:
        window = match.group(2)
        pct_match = re.search(r'(\d+[,\.]\d+)\s*%', window)
        bps_match = re.search(r'(\d+[,\.]?\d*)\s*(bps|basispunten|basis points)', window)
        raw_float_match = re.search(r'(?<!\S)([0-4][,\.]\d{2,4})(?!\S)', window)
        if pct_match:
            best_vcost.append(pct_match.group(1).replace(',', '.') + "%")
        elif bps_match:
            # Convert bps to percentage (e.g. 35 bps -> 0.35%)
            bps_val = float(bps_match.group(1).replace(',', '.'))
            best_vcost.append(f"{bps_val / 100:.3f}%".rstrip('0').rstrip('.'))
        elif raw_float_match:
            best_vcost.append(raw_float_match.group(1).replace(',', '.') + "%")
            
    if best_vcost:
        # Default to first found that resembles a sensible fee (e.g. < 5%)
        for cost in best_vcost:
            try:
                if float(cost.replace('%', '')) < 5.0:
                    vermogensbeheerkosten = cost
                    break
            except: pass
        if vermogensbeheerkosten == "Unknown":
            vermogensbeheerkosten = best_vcost[0]

    # Transactiekosten
    matches_t = re.finditer(r'(transactiekosten|kosten transactie)(.{0,400})', text)
    best_tcost = []
    for match in matches_t:
        window = match.group(2)
        pct_match = re.search(r'(\d+[,\.]\d+)\s*%', window)
        bps_match = re.search(r'(\d+[,\.]?\d*)\s*(bps|basispunten|basis points)', window)
        raw_float_match = re.search(r'(?<!\S)([0-4][,\.]\d{2,4})(?!\S)', window)
        if pct_match:
            best_tcost.append(pct_match.group(1).replace(',', '.') + "%")
        elif bps_match:
            bps_val = float(bps_match.group(1).replace(',', '.'))
            best_tcost.append(f"{bps_val / 100:.3f}%".rstrip('0').rstrip('.'))
        elif raw_float_match:
            best_tcost.append(raw_float_match.group(1).replace(',', '.') + "%")

    if best_tcost:
        for cost in best_tcost:
            try:
                if float(cost.replace('%', '')) < 5.0:
                    transactiekosten = cost
                    break
            except: pass
        if transactiekosten == "Unknown":
            transactiekosten = best_tcost[0]

    # Beleggingsmix logic
    beleggingsmix = "Unknown"
    mix_matches = re.finditer(r'(beleggingsmix|asset allocatie|strategische allocatie|portefeuille)', text)
    equity_val = None
    fixed_val = None
    real_estate_val = None
    
    for match in mix_matches:
        start = max(0, match.start() - 50)
        end = min(len(text), match.end() + 200)
        window = text[start:end]
        
        if not equity_val:
            e = re.search(r'aandelen.*?(\d+[,\.]\d+)\s*%', window)
            if e: equity_val = e.group(1) + "%"
        if not fixed_val:
            f = re.search(r'(vastrentend|obligaties).*?(\d+[,\.]\d+)\s*%', window)
            if f: fixed_val = f.group(1) + "%"
        if not real_estate_val:
            r = re.search(r'(vastgoed|onroerend).*?(\d+[,\.]\d+)\s*%', window)
            if r: real_estate_val = r.group(1) + "%"
            
    mix_parts = []
    if equity_val: mix_parts.append(f"Aandelen: {equity_val}")
    if fixed_val: mix_parts.append(f"Vastrentend: {fixed_val}")
    if real_estate_val: mix_parts.append(f"Vastgoed: {real_estate_val}")
    
    if mix_parts:
        beleggingsmix = " | ".join(mix_parts)
    else:
        # Fallback to general mention
        beleggingsmix = "Mentions mix but exact % parsed failed" if 'beleggingsmix' in text else "Unknown"

    return fund_id, vermogensbeheerkosten, transactiekosten, beleggingsmix

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    pdf_files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    print(f"Found {len(pdf_files)} reports to analyze for costs and mix.")

    results = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(analyze_report, f): f for f in pdf_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            filename = futures[future]
            try:
                res = future.result()
                if res is not None:
                    fund_id, vermogensbeheerkosten, transactiekosten, beleggingsmix = res
                    results.append((vermogensbeheerkosten, transactiekosten, beleggingsmix, fund_id))
                    print(f"[{i}/{len(pdf_files)}] {filename} -> V-Costs: {vermogensbeheerkosten} | T-Costs: {transactiekosten} | Mix: {beleggingsmix}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    c.executemany("UPDATE funds SET vermogensbeheerkosten = ?, transactiekosten = ?, beleggingsmix = ? WHERE id = ?", results)
    conn.commit()
    conn.close()
    
    print("Done! Extracted costs and asset mixes for reports.")

if __name__ == "__main__":
    main()
