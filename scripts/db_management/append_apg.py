import csv

csv_path = "../../data/processed/extracted_allocations.csv"

with open(csv_path, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["76", "34.35", "76_APG.pdf"])

print("Appended APG to extracted_allocations.csv")
