import sqlite3
import fitz  # PyMuPDF
import glob
import os
import re

DB_PATH = "../../data/processed/pension_funds.db"
REPORTS_DIR = "../data/reports"

def extract_management_entities(pdf_path):
    uitvoerder = None
    fiduciair = None
    intern_beheer = "Nee" # Default to No

    uitvoerder_keywords = ["pensioenuitvoerder", "uitvoeringsorganisatie", "uitvoering van de pensioenregeling", "administratie", "uitvoerder", "pensioenadministratie"]
    fiduciair_keywords = ["fiduciair", "vermogensbeheerder", "integraal vermogensbeheerder", "uitbesteding van het vermogensbeheer", "extern beheer"]
    intern_keywords = ["intern beheer", "eigen beheer", "zelfstandig beheer", "binnen het fonds"]

    common_uitvoerders = ["APG", "PGGM", "AZL", "TKP", "MN", "Appel", "Centric", "Achmea", "Syntrus", "Blue Sky Group", "DCP", "Visma", "AON"]
    common_fiduciairs = ["Achmea IM", "BlackRock", "Goldman Sachs", "MN", "APG", "PGGM", "Robeco", "Kempen", "Cardano", "Russell", "Willis Towers Watson", "Aegon", "NNIP", "BMO", "F&C", "Baudet"]

    try:
        doc = fitz.open(pdf_path)
        
        # Scan the first 30 pages where 'Organisatie', 'Kengetallen', or 'Uitbesteding' usually reside
        max_pages = min(30, len(doc))
        full_text = ""
        for i in range(max_pages):
            full_text += doc[i].get_text() + "\n"

        # Look for uitvoerder
        uitvoerder_match = re.search(r'(?:pensioenuitvoerder|uitvoeringsorganisatie|pensioenadministratie|administrateur).*?(?:is|bij)\s+([A-Z][A-Za-z\s&]+?)(?=\.|,| en| is| verzorgt)', full_text, re.IGNORECASE | re.DOTALL)
        if uitvoerder_match:
            candidate = uitvoerder_match.group(1).strip()
            # Clean up candidate
            candidate = candidate.replace('\n', ' ')
            if len(candidate) > 2 and len(candidate) < 50:
                 uitvoerder = candidate
        
        # Look for fiduciair
        fiduciair_match = re.search(r'(?:fiduciair(?: beheerder)?|integraal vermogensbeheerder|externe vermogensbeheerder).*?(?:is|bij)\s+([A-Z][A-Za-z\s&]+?)(?=\.|,| en| is| verzorgt)', full_text, re.IGNORECASE | re.DOTALL)
        if fiduciair_match:
            candidate = fiduciair_match.group(1).strip()
            candidate = candidate.replace('\n', ' ')
            if len(candidate) > 2 and len(candidate) < 50:
                fiduciair = candidate
                
        # Fallback to simple keyword matching if regex fails
        if not uitvoerder:
            for u in common_uitvoerders:
                if re.search(r'\b' + re.escape(u) + r'\b', full_text, re.IGNORECASE):
                    # Check context to ensure it's not just a passing mention
                    context = re.search(r'.{0,50}\b' + re.escape(u) + r'\b.{0,50}', full_text, re.IGNORECASE)
                    if context and any(k in context.group(0).lower() for k in uitvoerder_keywords):
                        uitvoerder = u
                        break
                        
        if not fiduciair:
            for f in common_fiduciairs:
                if re.search(r'\b' + re.escape(f) + r'\b', full_text, re.IGNORECASE):
                    context = re.search(r'.{0,50}\b' + re.escape(f) + r'\b.{0,50}', full_text, re.IGNORECASE)
                    if context and any(k in context.group(0).lower() for k in fiduciair_keywords):
                        fiduciair = f
                        break
        
        # Check for internal management
        for keyword in intern_keywords:
            if re.search(r'\b' + re.escape(keyword) + r'\b', full_text, re.IGNORECASE):
                # Ensure it's not "geen intern beheer" or similar
                context = re.search(r'.{0,40}\b' + re.escape(keyword) + r'\b.{0,40}', full_text, re.IGNORECASE)
                if context and not re.search(r'\b(geen|niet|uitbesteed)\b', context.group(0), re.IGNORECASE):
                    intern_beheer = "Ja"
                    break

        doc.close()
        return uitvoerder, fiduciair, intern_beheer
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        return None, None, "Onbekend"

def update_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    pdf_files = glob.glob(os.path.join(REPORTS_DIR, "*.pdf"))
    
    updates = 0
    for pdf_path in pdf_files:
        basename = os.path.basename(pdf_path)
        fund_id_match = re.match(r"(\d+)_", basename)
        
        if fund_id_match:
            fund_id = int(fund_id_match.group(1))
            
            # Skip transitiekplannen
            if "transitieplan" in basename.lower() or "update" in basename.lower() or "esg" in basename.lower():
                continue

            uitvoerder, fiduciair, intern_beheer = extract_management_entities(pdf_path)
            
            if uitvoerder or fiduciair or intern_beheer != "Nee":
                print(f"Fund {fund_id} ({basename}):")
                print(f"  Uitvoerder: {uitvoerder}")
                print(f"  Fiduciair: {fiduciair}")
                print(f"  Intern Beheer: {intern_beheer}")
                
            cursor.execute("""
                UPDATE funds 
                SET uitvoerder = ?, 
                    fiduciair_beheerder = ?,
                    intern_beheer = ?
                WHERE id = ?
            """, (uitvoerder, fiduciair, intern_beheer, fund_id))
            updates += 1

    conn.commit()
    conn.close()
    print(f"Updated {updates} records in the database.")

if __name__ == "__main__":
    update_database()
