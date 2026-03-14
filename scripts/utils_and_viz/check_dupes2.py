import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
query = "SELECT id, name FROM funds WHERE data_source = 'DNB Appendix' ORDER BY id ASC"
df = pd.read_sql_query(query, conn)
conn.close()

for i, row in df.iterrows():
    print(f"ID: {row['id']} | Name: {row['name']}")
