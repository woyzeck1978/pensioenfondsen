import requests
import re

url = "https://www.dnb.nl/statistieken/data-zoeken/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

print(f"Fetching frontend at: {url}")
try:
    r = requests.get(url, headers=headers, timeout=10)
    print("Status:", r.status_code)
    html = r.text
    
    # Search for api domains
    api_urls = re.findall(r'https://[a-zA-Z0-9.-]*\api\.*?dnb\.nl[a-zA-Z0-9./-]*', html)
    print("Found API URLs:", set(api_urls))
    
    data_urls = re.findall(r'https://[a-zA-Z0-9.-]*\data[a-zA-Z0-9.-]*dnb\.nl[a-zA-Z0-9./-]*', html)
    print("Found Data URLs:", set(data_urls))
    
except Exception as e:
    print("Error:", e)
