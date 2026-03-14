import os
import fitz

REPORTS_DIR = "../data/reports"
FUNDS_TO_CHECK = [77, 81, 88, 92, 93, 94, 99, 103, 114, 115, 120, 122, 125, 126, 127, 128]

for f in os.listdir(REPORTS_DIR):
    if not f.endswith('.pdf'): continue
    try:
        fid = int(f.split('_')[0])
        if fid in FUNDS_TO_CHECK:
            doc = fitz.open(os.path.join(REPORTS_DIR, f))
            text = '\n'.join([p.get_text() for p in doc[:50]])
            lines = text.split('\n')
            print(f"\n--- {f} ---")
            found_count = 0
            for i, l in enumerate(lines):
                if 'balanstotaal' in l.lower() or 'belegd vermogen' in l.lower():
                    print(f"Match: {l}")
                    for j in range(1, 5):
                        if i+j < len(lines):
                            print(f" +{j}: {lines[i+j]}")
                    print("-" * 20)
                    found_count += 1
                    if found_count > 3: break
    except Exception as e:
        pass
