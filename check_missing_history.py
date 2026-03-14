import sqlite3
import pandas as pd
import os
import re

# Connect to database and get active funds
conn = sqlite3.connect('data/processed/pension_funds.db')
query = "SELECT id, name, website FROM funds WHERE status = 'Actief' ORDER BY name ASC"
active_funds = pd.read_sql_query(query, conn)
conn.close()

# Scan the historical_reports directory
reports_dir = 'data/historical_reports'
downloaded_files = os.listdir(reports_dir) if os.path.exists(reports_dir) else []

# Target years
target_years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]

# Also check annual_reports for 2024
current_reports_dir = 'data/annual_reports'
current_files = os.listdir(current_reports_dir) if os.path.exists(current_reports_dir) else []

# We need a robust way to match files to fund IDs.
# If the previous scraper named files with the fund ID, it makes it easier. 
# Let's check how the files are named.
print(f"Sample historical files: {downloaded_files[:5]}")
print(f"Sample current files: {current_files[:5]}")

