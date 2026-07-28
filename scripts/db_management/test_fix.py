from fix_news_dates import fetch_date_from_html, extract_date_from_url
url = "https://www.fysiopensioen.nl/nieuws/20250625-jaarverslag-spf-2024-staat-online"
print("HTML:", fetch_date_from_html(url))
print("URL:", extract_date_from_url(url))
