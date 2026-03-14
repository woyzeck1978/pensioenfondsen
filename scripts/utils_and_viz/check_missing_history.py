import sqlite3
import pandas as pd
import os
import re

def gather_missing_histories():
    conn = sqlite3.connect('../../data/processed/pension_funds.db')
    query = """
        SELECT id, name FROM funds 
        WHERE status NOT LIKE '%Liquidat%' 
          AND status NOT LIKE '%Opgeheven%' 
          AND status NOT LIKE '%Buy-out%'
          AND status NOT LIKE '%Gesloten / Voorbereiding%'
        ORDER BY name ASC
    """
    active_funds = pd.read_sql_query(query, conn)
    conn.close()

    hist_dir = '../../data/historical_reports'
    curr_dir = '../../data/annual_reports'
    
    hist_files = os.listdir(hist_dir) if os.path.exists(hist_dir) else []
    curr_files = os.listdir(curr_dir) if os.path.exists(curr_dir) else []
    all_files = hist_files + curr_files

    target_years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
    
    missing_data = []

    for _, row in active_funds.iterrows():
        f_id = row['id']
        f_name = str(row['name']).replace('|', '')
        
        # Find which years we have for this fund id
        # E.g. 59_OAK Furniture Organ Building Exhibition Building and Timber Trade_2015.pdf
        # Or current reports like: 104_Haskoning DHV.pdf (Assume 2024/latest)
        
        found_years = set()
        has_current = False
        
        for f in hist_files:
            if f.startswith(f"{f_id}_"):
                # extract year from end of filename before .pdf
                match = re.search(r'_(\d{4})\.pdf$', f)
                if match:
                    found_years.add(int(match.group(1)))
                    
        for f in curr_files:
            if f.startswith(f"{f_id}_"):
                has_current = True
                
        # We assume 'current' covers 2024 or 2023 at least. If not strictly named with year, we 
        # might still be missing explicit historical drops. Let's just track strictly by explicit year, 
        # and maybe flag if they have a 'current' report.
        if has_current:
            found_years.add(2024) # We'll conventionally say the base document is 2024
            
        missing_years = sorted(list(set(target_years) - found_years))
        
        if missing_years:
            missing_str = ", ".join(map(str, missing_years))
            missing_data.append((f_id, f_name, missing_str, len(missing_years)))

    # Sort by number of missing years descending
    missing_data.sort(key=lambda x: x[3], reverse=True)

    # Generate Markdown
    md = "# Missing Historical Annual Reports\n\n"
    md += f"Out of {len(active_funds)} active funds, **{len(missing_data)}** are missing one or more annual reports between 2018-2024.\n\n"
    md += "| ID | Fund Name | Missing Years |\n"
    md += "|---|---|---|\n"
    
    for item in missing_data:
        md += f"| {item[0]} | {item[1]} | {item[2]} |\n"
        
    with open('../../data/processed/missing_historical_reports.md', 'w') as f:
        f.write(md)
        
    print(f"Report generated: data/processed/missing_historical_reports.md")
    print(f"Total funds with missing history: {len(missing_data)}")

if __name__ == '__main__':
    gather_missing_histories()
