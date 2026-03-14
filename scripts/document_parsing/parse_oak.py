import requests
from bs4 import BeautifulSoup
import urllib3
import re

urllib3.disable_warnings()

url = "https://verslagen.oakpensioenfonds.nl/jaarverslag-2024"

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

try:
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    
    print("\n================ Oak Pensioen Jaarverslag 2024 ================")
    
    text_content = soup.get_text(separator=' ', strip=True)
    lower_text = text_content.lower()
    
    # Simple regex to find surrounding context for AUM and Funding ratios
    print("\n--- Funding Ratio Hits ---")
    matches = re.finditer(r'.{0,80}(?:dekkingsgraad|invaren|minimaal|beleids).{0,80}(?:\d{2,3}(?:,\d{1,2})?%).{0,80}', lower_text, re.IGNORECASE)
    
    for i, match in enumerate(matches):
        if i > 10: break
        print(f">> {match.group(0).strip()}")
        
    print("\n--- AUM Hits ---")
    matches_aum = re.finditer(r'.{0,80}(?:vermogen|belegd|balans|mlj|mln|mld).{0,80}(?:miljard|miljoen).{0,80}', lower_text, re.IGNORECASE)
    
    for i, match in enumerate(matches_aum):
        if i > 10: break
        print(f">> {match.group(0).strip()}")

except Exception as e:
    print(f"Failed: {e}")
