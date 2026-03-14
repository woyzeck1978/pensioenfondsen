import csv

csv_path = "../../data/processed/extracted_allocations.csv"

with open(csv_path, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["126", "23.9", "126_SABIC.pdf"])

print("Appended SABIC to extracted_allocations.csv")
