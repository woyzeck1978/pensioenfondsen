import sqlite3
import os

conn = sqlite3.connect("../../data/processed/pension_funds.db")
cursor = conn.cursor()

# Get funds that are NOT in equity_allocations_extracted
cursor.execute("SELECT id, name FROM funds WHERE id NOT IN (SELECT fund_id FROM equity_allocations_extracted);")
missing_funds = cursor.fetchall()
missing_dict = {f[0]: f[1] for f in missing_funds}

reports_dir = "data/reports"
available_reports = os.listdir(reports_dir)

new_funds_with_reports = []

for report in available_reports:
    if report.endswith(".pdf"):
        # e.g. "19_Horeca.pdf" or "65_Centraal_Beheer_APF.pdf"
        try:
            fund_id = int(report.split('_')[0])
            if fund_id in missing_dict:
                new_funds_with_reports.append((fund_id, missing_dict[fund_id], report))
        except ValueError:
            pass

print(f"Found {len(new_funds_with_reports)} newly added funds with missing metrics that HAVE a PDF report:")
for fid, fname, freport in sorted(new_funds_with_reports):
    print(f"[{fid}] {fname} -> {freport}")

conn.close()
