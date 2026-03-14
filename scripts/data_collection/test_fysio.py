import requests
from bs4 import BeautifulSoup
import re

url = "https://www.fysiopensioen.nl/over-ons/bestuur-en-verantwoordingsorgaan"
print(f"Testing {url}")
try:
    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(res.text, 'html.parser')
    for heading in soup.find_all(['h2', 'h3']):
        if 'bestuur' in heading.get_text().lower() or 'directie' in heading.get_text().lower():
            print(f"Found heading: {heading.get_text().strip()}")
            # Find next ul or table
            nxt = heading.find_next(['ul', 'table'])
            if nxt:
                print("Found list elements:")
                for item in nxt.find_all('li') + nxt.find_all('td'):
                    print(f" - {item.get_text().strip()}")
except Exception as e:
    print(e)
