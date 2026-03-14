import csv
import sqlite3

# 1. Update CSV
lines = []
with open("../../data/processed/extracted_allocations.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
        if row and row[0] == "108":
            row[1] = "27.10"
        lines.append(row)

with open("../../data/processed/extracted_allocations.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(lines)

# 2. Update DB
conn = sqlite3.connect("../../data/processed/pension_funds.db")
cursor = conn.cursor()
cursor.execute("UPDATE equity_allocations_extracted SET allocation_pct=27.1 WHERE fund_id=108;")
conn.commit()
conn.close()

print("Updated CSV and DB for ING (108).")
