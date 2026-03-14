import sqlite3
import pandas as pd

DB_PATH = "../../data/processed/pension_funds.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # APG Data (pages 53-54)
    apg_fund_id = 76
    
    # Clean up any existing data for APG to avoid duplicates
    c.execute("DELETE FROM equity_strategies WHERE fund_id = ?", (apg_fund_id,))
    
    # We will aggregate the Active vs Passive values.
    # Total Active Equities
    # Developed Markets Equity: 35587
    # DME Focus Total: 15919
    # DME Fundamental Total: 3748
    # DME Quant Total: 2349
    # DME Small Cap & Midcap: 1258
    # DME Transition Portfolios: 12337
    # Emerging Markets Equity: 30907
    total_active = 35587 + 15919 + 3748 + 2349 + 1258 + 12337 + 30907
    
    # Total Index (Passive) Equities
    # DME RI Index: 113146
    # DME Minimum Volatility Total: 3757
    total_passive = 113146 + 3757
    
    total_equities = total_active + total_passive
    
    # Calculate weights
    weight_active = round((total_active / total_equities) * 100, 2)
    weight_passive = round((total_passive / total_equities) * 100, 2)
    
    # Also calc Regional weights
    # Emerging Markets = 30907 (Active)
    # Developed Markets = Total - Emerging
    total_emerging = 30907
    weight_emerging = round((total_emerging / total_equities) * 100, 2)
    weight_developed = round((100.0 - weight_emerging), 2)
    
    print(f"APG Total Equities: €{total_equities} mln")
    print(f"Active: {weight_active}% | Passive (Index): {weight_passive}%")
    print(f"Developed: {weight_developed}% | Emerging: {weight_emerging}%")
    
    # Insert Regional Strategies
    c.execute("INSERT INTO equity_strategies (fund_id, region, weight_pct) VALUES (?, ?, ?)",
              (apg_fund_id, 'Ontwikkelde Markten', weight_developed))
    c.execute("INSERT INTO equity_strategies (fund_id, region, weight_pct) VALUES (?, ?, ?)",
              (apg_fund_id, 'Opkomende Markten', weight_emerging))
              
    # Update Management Style in equity_strategy
    c.execute("UPDATE equity_strategy SET management_style = ? WHERE fund_id = ?", 
              (f"Actief ({weight_active}%), Passief ({weight_passive}%)", apg_fund_id))
              
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
