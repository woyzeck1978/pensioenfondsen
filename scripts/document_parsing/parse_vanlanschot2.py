import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings()

url = "https://pensioenfondsvanlanschot.nl/nieuws/nieuwsbericht/"

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

print(f"\n================ Fetching nieuwsbericht ================")
try:
    response = requests.get(url, headers=headers, verify=False, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    
    paragraphs = soup.find_all('p')
    for p in paragraphs:
        text = p.get_text(strip=True)
        if text and len(text) > 20:
            print(text)
            
except Exception as e:
    print(f"Failed: {e}")
