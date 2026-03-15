import os
import sqlite3
import pandas as pd
import pdfplumber
import fitz  # PyMuPDF
import google.generativeai as genai
import json
import time

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")
PDF_DIR = os.path.join(BASE_DIR, "data", "historical_reports")
MODEL_NAME = 'gemini-2.5-flash'  # Use flash for speed as task is relatively straightforward

genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Add new columns to the funds table for SFDR data
    columns_to_add = [
        ("sfdr_article", "INTEGER"),
        ("eu_taxonomy_pct", "REAL")
    ]
    for col_name, col_type in columns_to_add:
        try:
            cursor.execute(f"ALTER TABLE funds ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass # Column already exists
    conn.commit()
    conn.close()

def extract_sfdr_context(pdf_path, num_pages=30):
    """
    Search from the BACK of the document (because annexes are usually at the end).
    Look for keywords like 'SFDR', 'RTS', 'Artikel 8', 'Artikel 9', 'Taxonomie'.
    """
    context = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            # Scan the last 50 pages or the whole document if shorter
            start_page = max(0, total_pages - 50)
            
            for i in range(total_pages - 1, start_page - 1, -1):
                page = pdf.pages[i]
                text = page.extract_text()
                if text:
                    text_lower = text.lower()
                    if ('sfdr' in text_lower or 'regulatory technical standards' in text_lower or 
                        'artikel 8' in text_lower or 'artikel 9' in text_lower or 
                        'taxonomie' in text_lower or 'duurzame beleggingsdoelstelling' in text_lower):
                        context = f"--- PAGE {i} ---\n{text}\n" + context
                        
                        # Just grab a few pages of context around the hit to save tokens
                        if len(context) > 15000: 
                            break
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        
    # Fallback to PyMuPDF if pdfplumber fails or is empty
    if not context:
        try:
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            start_page = max(0, total_pages - 50)
            for i in range(total_pages - 1, start_page - 1, -1):
                page = doc[i]
                text = page.get_text()
                if text:
                    text_lower = text.lower()
                    if ('sfdr' in text_lower or 'regulatory technical standards' in text_lower or 
                        'artikel 8' in text_lower or 'artikel 9' in text_lower or 
                        'taxonomie' in text_lower or 'duurzame beleggingsdoelstelling' in text_lower):
                        context = f"--- PAGE {i} ---\n{text}\n" + context
                        if len(context) > 15000:
                            break
        except Exception as e:
            print(f"PyMuPDF Error reading {pdf_path}: {e}")
            
    return context

def process_sfdr_with_llm(context_text, fund_name):
    prompt = f"""
    You are a financial analyst specializing in European pension funds and the Sustainable Finance Disclosure Regulation (SFDR).
    
    Below is extracted text from the annexes/appendices of the 2024 annual report for the Dutch pension fund "{fund_name}".
    This text likely contains the mandatory SFDR classification (often called Regulatory Technical Standards or RTS annex).
    
    Your task is to extract two specific data points:
    1. SFDR Article Classification: Is the fund classified under Article 6, Article 8, or Article 9 of the SFDR? 
       (Look for phrases like "artikel 8 SFDR-classificatie", "Dit product promootte ecologische/sociale kenmerken", "Lichtgroen" -> 8)
       (Look for "donkergroen", "duurzame beleggingsdoelstelling" -> 9)
       (Look for "grijs", "geen duurzaamheidskenmerken" -> 6)
       Return the integer 6, 8, or 9. If not found, return null.
       
    2. EU Taxonomy Percentage: What percentage of the investments are aligned with the EU Taxonomy for environmentally sustainable activities?
       (Look for "minimum van X%", "EU-taxonomie als ecologisch duurzaam gelden", "Op de taxonomie afgestemd"). 
       Return this as a float number representing the percentage (e.g. 5.5 for 5.5%, 0.0 for 0%). If not found, return null.
       
    TEXT CONTEXT:
    ========================================================================
    {context_text}
    ========================================================================
    
    Output ONLY a pure JSON block containing these two keys exactly as shown. Do not include markdown formatting or explanations.
    {{
        "sfdr_article": 8,
        "eu_taxonomy_pct": 5.0
    }}
    """
    
    model = genai.GenerativeModel(MODEL_NAME)
    try:
        response = model.generate_content(prompt)
        result_text = response.text.strip()
        if result_text.startswith('```json'):
            result_text = result_text.replace('```json\n', '').replace('\n```', '')
        elif result_text.startswith('```'):
            result_text = result_text.replace('```\n', '').replace('\n```', '')
            
        return json.loads(result_text)
    except Exception as e:
        print(f"LLM Error: {e}")
        return {"sfdr_article": None, "eu_taxonomy_pct": None}

def main():
    setup_database()
    conn = sqlite3.connect(DB_PATH)
    
    # Get all funds that have a 2024 annual report downloaded BUT don't have an sfdr_article yet
    query = """
    SELECT f.id, f.name, f.sfdr_article, s.url
    FROM funds f
    JOIN scraped_documents s ON f.id = s.fund_id
    WHERE f.status = 'Open' 
      AND s.doc_type = 'document'
      AND s.year_extracted = 2024
      AND f.sfdr_article IS NULL
    """
    funds_to_process = pd.read_sql_query(query, conn)
    print(f"Found {len(funds_to_process)} funds missing SFDR metrics for 2024.")
    
    cursor = conn.cursor()
    success_count = 0
    
    for index, row in funds_to_process.iterrows():
        fund_id = row['id']
        fund_name = row['name']
        print(f"[{index+1}/{len(funds_to_process)}] Processing {fund_name}...")
        
        # Determine local filename convention as saved by scrape_historical_annual_reports.py
        import re
        clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', fund_name).strip()
        filename = f"{fund_id}_{clean_name}_2024.pdf"
        filepath = os.path.join(PDF_DIR, filename)
        
        if not os.path.exists(filepath):
            print(f"  -> Local PDF not found at {filepath}")
            continue
            
        print("  -> Extracting SFDR context from PDF annexes...")
        context = extract_sfdr_context(filepath)
        
        if not context:
            print("  -> No SFDR/RTS keywords found in the annexes of this PDF.")
            continue
            
        print("  -> Querying Gemini for classification...")
        result = process_sfdr_with_llm(context, fund_name)
        
        sfdr_article = result.get("sfdr_article")
        eu_taxonomy_pct = result.get("eu_taxonomy_pct")
        
        if sfdr_article:
            print(f"  -> SUCCESS! Article {sfdr_article} | Taxonomy: {eu_taxonomy_pct}%")
            cursor.execute("""
                UPDATE funds 
                SET sfdr_article = ?, eu_taxonomy_pct = ?
                WHERE id = ?
            """, (sfdr_article, eu_taxonomy_pct, fund_id))
            conn.commit()
            success_count += 1
        else:
            print("  -> SFDR classification could not be determined from the extracted context.")
            
        time.sleep(1) # Prevent API rate limits
        
    conn.close()
    print(f"\nFinished parsing. Successfully extracted SFDR metrics for {success_count} funds.")

if __name__ == "__main__":
    main()
