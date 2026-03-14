import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
query = "SELECT id, name, data_source FROM funds ORDER BY id ASC"
df = pd.read_sql_query(query, conn)
conn.close()

original = df[df['data_source'] != 'DNB Appendix'].copy()
new_funds = df[df['data_source'] == 'DNB Appendix'].copy()

print(f"Original funds: {len(original)}")
print(f"New appended funds: {len(new_funds)}")

print("\n--- Suspicious New Funds List ---")
for i, row in new_funds.iterrows():
    print(f"ID: {row['id']} | Name: {row['name']}")
