import fitz
import re

def find_balans(pdf_path, name):
    print(f'\n=== {name} BALANS SEARCH ===')
    try:
        doc = fitz.open(pdf_path)
        full_text = []
        for i in range(len(doc)):
            full_text.extend(doc[i].get_text().split('\n'))
            
        for i, line in enumerate(full_text):
            if 'balans per 31 december' in line.lower() or 'staat van baten' in line.lower() or re.match(r'^balans$', line.lower().strip()):
                print(f'- Found potential Balans section at line {i}: {line}')
                # Print the next 20 non-empty lines
                count = 0
                for j in range(i+1, min(len(full_text), i+50)):
                    l = full_text[j].strip()
                    if l:
                        print(f'  {l}')
                        count += 1
                        if count > 20: break
    except Exception as e:
        print(f'{name} Error: {e}')

find_balans('data/processed/jaarverslag-2023-vlakglas.pdf', 'VLAKGLAS')
find_balans('data/processed/bpl-pensioen-jaarverslag-2024.pdf', 'BPL')

