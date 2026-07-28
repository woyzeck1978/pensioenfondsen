import sqlite3
import pandas as pd

def load_data(query):
    conn = sqlite3.connect("data/processed/pension_funds.db")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

query = """
SELECT year_extracted, title, url
FROM (
    SELECT 
        title, 
        url,
        CAST(SUBSTR(title, -4) AS INTEGER) as year_extracted
    FROM scraped_documents
    WHERE fund_id = 76 
        AND doc_type = 'document' 
        AND (lower(title) LIKE '%jaarverslag%' OR lower(title) LIKE '%jaarrapport%' OR lower(title) LIKE '%annual report%')
        AND lower(title) NOT LIKE '%maatschappelijk%'
        AND lower(title) NOT LIKE '%duurzaam%'
        AND lower(title) NOT LIKE '%esg%'
)
ORDER BY year_extracted DESC NULLS LAST, title DESC
LIMIT 5
"""
print(load_data(query))
