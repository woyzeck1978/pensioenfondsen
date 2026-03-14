import docx
import sqlite3
import os
import re

doc_path = r'c:\Users\WebkoWuite\DPS\VB Portefeuille Beheer - Documents\Zakelijke Waarden\Aandelen\EU equity\Research\Nederlandse-pensioenfondsen\The Comprehensive Directory of Dutch Pension Funds and Providers.docx'
db_path = r'c:\Users\WebkoWuite\DPS\VB Portefeuille Beheer - Documents\Zakelijke Waarden\Aandelen\EU equity\Research\Nederlandse-pensioenfondsen\data\pension_funds.db'

def extract_funds():
    if not os.path.exists(doc_path):
        print(f"Error: {doc_path} not found.")
        return

    doc = docx.Document(doc_path)
    funds = []
    current_category = "General"

    # Category patterns (headers usually found in such documents)
    category_keywords = [
        "Industry-wide", "Sector", "Corporate", "Company", 
        "Professional", "APF", "General Pension Fund", "Mandatory"
    ]

    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue

        # Check if it's a category header
        is_header = any(kw.lower() in text.lower() for kw in category_keywords) and len(text) < 50
        if is_header and ":" not in text:
            current_category = text
            continue

        # Logic for identifying a fund name
        # 1. Starts with 'Pensioenfonds' or 'Stichting Pensioenfonds'
        # 2. Ends with 'APF'
        # 3. Short line containing 'Pensioen'
        # 4. Specific known names from initial inspection
        
        is_fund = False
        if "Pensioenfonds" in text or "Pensioen" in text or text.endswith("APF"):
            is_fund = True
        elif "(" in text and ")" in text and len(text) < 60:
            is_fund = True # e.g. BPL Pensioen (Agriculture)
        
        if is_fund:
            funds.append((text, current_category))

    print(f"Extracted {len(funds)} potential funds.")
    save_to_db(funds)

def save_to_db(funds):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    count = 0
    for name, category in funds:
        try:
            cursor.execute("INSERT INTO funds (name, category, data_source) VALUES (?, ?, ?)", 
                           (name, category, "Local Directory DOCX"))
            count += 1
        except sqlite3.IntegrityError:
            # Duplicate name
            pass
    
    conn.commit()
    conn.close()
    print(f"Saved {count} unique funds to database.")

if __name__ == "__main__":
    extract_funds()
