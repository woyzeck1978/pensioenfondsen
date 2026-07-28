import requests
from io import BytesIO
import PyPDF2

url = "https://www.aholddelhaizepensioen.nl/-/media/Files/Aholddelhaize/adp-jaarverslag-2024.pdf"
print("Downloading Ahold Delhaize 2024 Annual Report...")

try:
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0'
    })
    
    response = s.get(url, verify=False, timeout=30)
    response.raise_for_status()
    
    pdf_file = BytesIO(response.content)
    reader = PyPDF2.PdfReader(pdf_file)
    
    num_pages = len(reader.pages)
    print(f"Total pages: {num_pages}")
    
    print("\\n--- EXTRACTING KEY FIGURES (First 15 Pages) ---")
    for p in range(0, min(15, num_pages)):
        try:
            text = reader.pages[p].extract_text()
            if text:
                print(f"\\n--- PAGE {p+1} ---\\n")
                print(text)
        except Exception as e:
            pass
            
    print("\\n--- EXTRACTING SFDR (Last 30 Pages) ---")
    start_sfdr = max(0, num_pages - 30)
    for p in range(start_sfdr, num_pages):
        try:
            text = reader.pages[p].extract_text()
            if text:
                for line in text.split('\\n'):
                    line_lower = line.lower()
                    if "artikel 8" in line_lower or "artikel 9" in line_lower or ("taxonomie" in line_lower and "%" in line):
                        print(f"[PAGE {p+1}] Match: {line}")
        except Exception as e:
            pass
            
except Exception as e:
    print(f"Error: {e}")
