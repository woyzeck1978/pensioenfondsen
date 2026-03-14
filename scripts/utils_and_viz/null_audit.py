import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
query = "SELECT id, name, category, website, aum_euro_bn, uitvoerder FROM funds WHERE data_source = 'DNB Appendix' ORDER BY id ASC"
df = pd.read_sql_query(query, conn)
conn.close()

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
print(df.to_string())
