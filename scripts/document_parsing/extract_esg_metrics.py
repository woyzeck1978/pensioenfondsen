import sqlite3
import pandas as pd
import fitz
import os
import re

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
db_path = os.path.join(base_dir, "data/processed/pension_funds.db")
reports_dir = os.path.join(base_dir, "data/annual_reports")

def extract_sfdr(text):
    # Looking for SFDR Article 8 or 9 classification
    sfdr_regex = re.compile(r'(?i)\b(artikel\s*[89]|art\.\s*[89])\s*(?:van de\s*)?(?:sfdr)?\b')
    match = sfdr_regex.search(text)
    if match:
        article = match.group(1).lower().replace('art.', 'artikel')
        if '8' in article: return "Artikel 8 (Lichtgroen)"
        if '9' in article: return "Artikel 9 (Donkergroen)"
    
    # Also look for explicit mention of "lichtgroen" or "donkergroen"
    if re.search(r'(?i)\blichtgroen', text): return "Artikel 8 (Lichtgroen)"
    if re.search(r'(?i)\bdonkergroen', text): return "Artikel 9 (Donkergroen)"
    
    return None

def extract_exclusions(text):
    # Look for common exclusion categories
    exclusions = []
    text_lower = text.lower()
    
    if re.search(r'\b(tabak|sigaretten)\b', text_lower): exclusions.append("Tabak")
    if re.search(r'\b(controversiële wapens|omstreden wapens|kernwapens|clustermunitie)\b', text_lower): exclusions.append("Controversiële Wapens")
    if re.search(r'\b(kolen|steenkolen|teerzand|fossiele brandstoffen)\b', text_lower): exclusions.append("Kolen / Fossiel")
    if re.search(r'\b(gokken|kansspelen)\b', text_lower): exclusions.append("Gokken")
    if re.search(r'\b(kinderarbeid|mensenrechten|global compact)\b', text_lower): exclusions.append("Mensenrechtenschendingen")
    
    if exclusions:
        return ", ".join(exclusions)
    return None

def extract_co2_goal(text):
    # Look for CO2 reduction percentages and years
    # Usually phrased like "50% CO2-reductie in 2030" or "reductie van 40% ten opzichte van 2019"
    
    co2_regex = re.compile(r'(?i)(?:co2|co₂|klimaat|broeikasgassen|emissie).{0,50}?(\d{1,3})\s*%\s*(?:reductie|vermindering|lager).{0,40}?(20[2-5][0-9])')
    match = co2_regex.search(text)
    if match:
        pct = match.group(1)
        year = match.group(2)
        return f"{pct}% reductie in {year}"
        
    co2_regex2 = re.compile(r'(?i)(?:reductie|vermindering).{0,30}?(\d{1,3})\s*%.{0,40}?(?:co2|co₂|broeikasgassen|emissie).{0,40}?(20[2-5][0-9])')
    match2 = co2_regex2.search(text)
    if match2:
        pct = match2.group(1)
        year = match2.group(2)
        return f"{pct}% reductie in {year}"
        
    return None

def get_target_funds():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT f.id, f.name, f.annual_report_year
        FROM funds f
        WHERE f.annual_report_downloaded = 1
    """, conn)
    conn.close()
    return df

def process_pdf(pdf_path):
    sfdr = None
    exclusions = None
    co2_goal = None
    
    try:
        doc = fitz.open(pdf_path)
        
        # We only really need to scan the ESG or sustainability chapters.
        # But to be safe, we'll scan the whole document, concatenating pages.
        # Alternatively, we can search page by page.
        
        full_text = ""
        for page_num in range(min(doc.page_count, 150)): # limit to first 150 pages for speed
            page = doc.load_page(page_num)
            text = page.get_text("text")
            full_text += text + " "
            
        sfdr = extract_sfdr(full_text)
        exclusions = extract_exclusions(full_text)
        co2_goal = extract_co2_goal(full_text)
        
        doc.close()
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")
        
    return {
        'sfdr_classification': sfdr,
        'esg_exclusions': exclusions,
        'co2_reduction_goal': co2_goal
    }

def main():
    df = get_target_funds()
    total = len(df)
    results = []
    
    print(f"Scanning {total} Annual Report PDFs for ESG & Sustainability Metrics...")
    
    for index, row in df.iterrows():
        f_id = row['id']
        name = row['name']
        year = row['annual_report_year']
        
        pdf_path = None
        for filename in os.listdir(reports_dir):
            if filename.startswith(f"{f_id}_") and filename.endswith(".pdf"):
                pdf_path = os.path.join(reports_dir, filename)
                break
        
        if pdf_path:
            print(f"Processing: {name}")
            data = process_pdf(pdf_path)
            if data['sfdr_classification'] or data['esg_exclusions'] or data['co2_reduction_goal']:
                results.append({
                    'fund_id': f_id,
                    'co2_reduction_goal': data['co2_reduction_goal'],
                    'esg_exclusions': data['esg_exclusions'],
                    'sfdr_classification': data['sfdr_classification']
                })
        else:
             print(f"File not found for ID: {f_id}")
             
    if results:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for res in results:
            cursor.execute('''
                INSERT INTO fund_esg_metrics (fund_id, co2_reduction_goal, esg_exclusions, sfdr_classification)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(fund_id) DO UPDATE SET
                    co2_reduction_goal=excluded.co2_reduction_goal,
                    esg_exclusions=excluded.esg_exclusions,
                    sfdr_classification=excluded.sfdr_classification
            ''', (res['fund_id'], res['co2_reduction_goal'], res['esg_exclusions'], res['sfdr_classification']))
            
        conn.commit()
        conn.close()
        print(f"Successfully extracted and saved ESG metrics for {len(results)} funds!")
    else:
        print("No ESG metrics found to save.")

if __name__ == '__main__':
    main()
