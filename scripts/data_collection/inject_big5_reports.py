import sqlite3
import os
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "pension_funds.db")

def find_fund_id(cursor, name_like):
    cursor.execute("SELECT id FROM funds WHERE name LIKE ?", ('%' + name_like + '%',))
    row = cursor.fetchone()
    if row:
        return row[0]
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Map fund identifiers to their target search terms
    fund_searches = {
        "ABP": "ABP (Government and Education)",
        "PFZW": "Zorg en Welzijn",
        "PMT": "Metaal & Techniek / PMT",
        "PME": "Metalektro / PME",
        "bpfBOUW": "BPFBouw",
        "ABN_AMRO": "ABN AMRO"
    }
    
    # Pre-compiled explicit links (2019-2023 generally covers the last 5 years)
    report_links = {
        "ABP": [
            ("Jaarverslag 2023", "https://cms.abp.nl/siteassets/over-abp/documenten/abp-jv-2023.pdf"),
            ("Jaarverslag 2022", "https://cms.abp.nl/siteassets/over-abp/documenten/abp-jaarverslag-2022.pdf"),
            ("Jaarverslag 2021", "https://cms.abp.nl/siteassets/over-abp/documenten/abp-jaarverslag-2021.pdf"),
            ("Jaarverslag 2020", "https://cms.abp.nl/siteassets/over-abp/documenten/abp-jaarverslag-2020.pdf"),
            ("Jaarverslag 2019", "https://cms.abp.nl/siteassets/over-abp/documenten/abp-jaarverslag-2019.pdf")
        ],
        "PFZW": [
            ("Jaarverslag 2023", "https://www.pfzw.nl/content/dam/pfzw/over-ons/jaarverslag/pdf/pfzw-jaarverslag-2023.pdf"),
            ("Jaarverslag 2022", "https://www.pfzw.nl/content/dam/pfzw/over-ons/jaarverslag/pdf/jaarverslag-pm-pfzw-2022.pdf"),
            ("Jaarverslag 2021", "https://www.pfzw.nl/content/dam/pfzw/over-ons/jaarverslag/pdf/jaarverslag-en-verantwoordingsorgaan-pfzw-2021.pdf"),
            ("Jaarverslag 2020", "https://www.pfzw.nl/content/dam/pfzw/over-ons/jaarverslag/pdf/jaarverslag-pm-pfzw-2020.pdf"),
            ("Jaarverslag 2019", "https://www.pfzw.nl/content/dam/pfzw/over-ons/jaarverslag/pdf/jaarverslag-pfzw-2019.pdf")
        ],
        "PMT": [
            ("Jaarverslag 2023", "https://www.pmt.nl/media/dhlg1y30/pmt-jaarverslag-2023.pdf"),
            ("Jaarverslag 2022", "https://www.pmt.nl/media/p5fpcoyp/pmt-jaarverslag-2022_b.pdf"),
            ("Jaarverslag 2021", "https://www.pmt.nl/media/yqlnjrdk/pmt-jaarverslag-2021.pdf"),
            ("Jaarverslag 2020", "https://www.pmt.nl/media/s25l2whg/pmt-jaarverslag-2020.pdf"),
            ("Jaarverslag 2019", "https://www.pmt.nl/media/1s3d2rht/pmt-jaarverslag-2019.pdf")
        ],
        "PME": [
            ("Jaarverslag 2023", "https://www.metalektropensioen.nl/media/klyftg2a/pme_jaarverslag_2023_online.pdf"),
            ("Jaarverslag 2022", "https://www.metalektropensioen.nl/media/z4bb053k/pme-jaarverslag-2022.pdf"),
            ("Jaarverslag 2021", "https://www.metalektropensioen.nl/media/wxhlnz1h/pme-jaarverslag-2021_getekend.pdf"),
            ("Jaarverslag 2020", "https://www.metalektropensioen.nl/media/v2gdr2d5/pme-jaarverslag-2020-volledig-herzien-7-oktober-2021.pdf"),
            ("Jaarverslag 2019", "https://www.metalektropensioen.nl/media/4gkhxczs/pme_jaarverslag-2019.pdf")
        ],
        "bpfBOUW": [
            ("Jaarverslag 2023", "https://www.bpfbouw.nl/binaries/content/assets/bpfbouw-documenten/over-bpfbouw/jaarverslag-bpfbouw-2023.pdf"),
            ("Jaarverslag 2022", "https://www.bpfbouw.nl/binaries/content/assets/bpfbouw-documenten/over-bpfbouw/jaarverslag-bpfbouw-2022.pdf"),
            ("Jaarverslag 2021", "https://www.bpfbouw.nl/binaries/content/assets/bpfbouw-documenten/over-bpfbouw/jaarverslag-bpfbouw-2021.pdf"),
            ("Jaarverslag 2020", "https://www.bpfbouw.nl/binaries/content/assets/bpfbouw-documenten/over-bpfbouw/jaarverslag-bpfbouw-2020.pdf"),
            ("Jaarverslag 2019", "https://www.bpfbouw.nl/binaries/content/assets/bpfbouw-documenten/over-bpfbouw/bpfbouw_jaarverslag-2019.pdf")
        ],
        "ABN_AMRO": [
            ("Jaarverslag 2023", "https://abnamropensioenfonds.nl/images/abn-amro-pensioenfonds-jaarverslag-2023.pdf"),
            ("Jaarverslag 2022", "https://abnamropensioenfonds.nl/images/abn-amro-pensioenfonds-jaarverslag-2022.pdf"),
            ("Jaarverslag 2021", "https://abnamropensioenfonds.nl/images/abn-amro-pensioenfonds-jaarverslag-2021.pdf"),
            ("Jaarverslag 2020", "https://abnamropensioenfonds.nl/images/abn-amro-pensioenfonds-jaarverslag-2020.pdf"),
            ("Jaarverslag 2019", "https://abnamropensioenfonds.nl/images/abn-amro-pensioenfonds-jaarverslag-2019.pdf")
        ]
    }

    inserted = 0
    now = datetime.now()

    for fund_key, search_str in fund_searches.items():
        fund_id = find_fund_id(cursor, search_str)
        if not fund_id:
            print(f"ERROR: Could not find fund_id for {fund_key} (Search: {search_str})")
            continue
        
        print(f"Found {fund_key} with ID {fund_id}")

        for title, url in report_links[fund_key]:
            try:
                cursor.execute("""
                    INSERT INTO scraped_documents (fund_id, url, title, doc_type, discovered_at)
                    VALUES (?, ?, ?, 'document', ?)
                    ON CONFLICT(url) DO UPDATE SET title=excluded.title, discovered_at=excluded.discovered_at
                """, (fund_id, url, title, now))
                inserted += 1
            except Exception as e:
                print(f"Failed to insert {title} for {fund_key}: {e}")
                
    conn.commit()
    conn.close()
    print(f"Successfully injected {inserted} 'Big 5' annual report links into the database!")

if __name__ == "__main__":
    main()
