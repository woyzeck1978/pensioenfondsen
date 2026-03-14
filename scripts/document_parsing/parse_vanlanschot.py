import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings()

urls = [
    "https://pensioenfondsvanlanschot.nl/nieuws/jaarverslag-2024/",
    "https://pensioenfondsvanlanschot.nl/nieuws/overdracht-naar-hnpf-heeft-plaatsgevonden/"
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

for url in urls:
    print(f"\n================ Fetching {url.split('/')[-2]} ================")
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Grab main text content paragraphs
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                print(text)
                
    except Exception as e:
        print(f"Failed: {e}")
        
