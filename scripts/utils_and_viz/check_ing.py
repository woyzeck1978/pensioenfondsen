import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
query = "SELECT id, name, data_source FROM funds WHERE name LIKE '%ing%'"
df = pd.read_sql_query(query, conn)
conn.close()

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
print(df.to_string())
