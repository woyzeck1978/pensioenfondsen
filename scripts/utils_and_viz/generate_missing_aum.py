import sqlite3
import pandas as pd

conn = sqlite3.connect('../../data/processed/pension_funds.db')
query = """
    SELECT id, name, aum_euro_bn, equity_allocation_pct, website
    FROM funds
    WHERE status NOT LIKE '%Liquidat%' 
      AND status NOT LIKE '%Opgeheven%' 
      AND status NOT LIKE '%Buy-out%'
      AND status NOT LIKE '%Gesloten / Voorbereiding%'
      AND (aum_euro_bn IS NULL OR equity_allocation_pct IS NULL)
    ORDER BY name ASC
"""
df = pd.read_sql_query(query, conn)
conn.close()

md_content = '# Missing Metrics for Active Funds\n\n'
md_content += f'There are currently **{len(df)}** active funds missing either AUM or Equity Allocation Data.\n\n'

if not df.empty:
    headers = list(df.columns)
    md_content += '| ' + ' | '.join(headers) + ' |\n'
    md_content += '| ' + ' | '.join(['---'] * len(headers)) + ' |\n'
    
    for _, row in df.iterrows():
        id_str = str(row['id'])
        name = str(row['name']).replace('|', '')
        aum = f"{row['aum_euro_bn']:.2f}" if pd.notnull(row['aum_euro_bn']) else 'NaN'
        eq = f"{row['equity_allocation_pct']:.2f}%" if pd.notnull(row['equity_allocation_pct']) else 'NaN'
        website = str(row['website']) if pd.notnull(row['website']) else ''
        
        md_content += f'| {id_str} | {name} | {aum} | {eq} | {website} |\n'
else:
    md_content += 'All active funds have been fully populated!\n'

with open('../../data/processed/missing_aum_funds.md', 'w') as f:
    f.write(md_content)

print(f'Successfully generated missing_aum_funds.md with {len(df)} funds.')
