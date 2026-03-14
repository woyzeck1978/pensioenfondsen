import requests

base_url = "https://data.dnb.nl/api/v1/OdataApi/Odata"

print("--- Testing Public DNB Open Data API ---")
try:
    root = requests.get(base_url)
    data = root.json()
    print("Top Level Collections:")
    if 'value' in data:
        for item in data['value'][:15]:
            print(f"- {item.get('name')}")
            
    # Also search specifically for pension fund datasets
    print("\nPension fund related collections:")
    for item in data.get('value', []):
        if "pensioen" in item.get('name', '').lower() or "pf" in item.get('name', '').lower():
            print(f"- {item.get('name')}")
            
except Exception as e:
    print(f"Error accessing API: {e}")
