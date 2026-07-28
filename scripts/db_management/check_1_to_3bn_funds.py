import sqlite3
import pandas as pd

db_path = "data/processed/pension_funds.db"

def analyze_completeness():
    conn = sqlite3.connect(db_path)
    
    # Select funds with AUM between 1.0 and 3.0 billion
    query = """
    SELECT 
        id, 
        name, 
        category,
        aum_euro_bn,
        deelnemers_totaal,
        beleidsdekkingsgraad_pct,
        beleggingsmix,
        sfdr_article,
        vermogensbeheerkosten_pct,
        transactiekosten_pct,
        uitvoeringskosten_per_deelnemer,
        wtp_transitie_datum,
        annual_report_downloaded,
        transitieplan_downloaded
    FROM funds
    WHERE aum_euro_bn >= 1.0 AND aum_euro_bn < 3.0
    ORDER BY aum_euro_bn DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    print(f"Found {len(df)} funds with AUM between 1 and 3 billion EUR.\n")
    
    for _, row in df.iterrows():
        missing_fields = []
        for col in df.columns:
            if pd.isna(row[col]) or row[col] == "" or row[col] == 0:
                if col not in ['annual_report_downloaded', 'transitieplan_downloaded']:
                    missing_fields.append(col)
        
        print(f"Fund: {row['name']} (ID: {row['id']}) | AUM: €{row['aum_euro_bn']}B")
        if missing_fields:
            print(f"  Missing: {', '.join(missing_fields)}")
        else:
            print(f"  Completeness: 100% core fields populated")
            
        print("-" * 50)
        
    conn.close()

if __name__ == "__main__":
    analyze_completeness()
