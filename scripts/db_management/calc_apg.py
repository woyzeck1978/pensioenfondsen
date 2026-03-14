lines = """
Long Duration Treasury Euro 2.101
Total Credits 56.631
Alternative Credits 3.718
Investment Grade Corporate Credits 13.016
Mortgages 10.035
Emerging Market Debt Active 16.617
Alternative Inflation 89

Developed Markets Equity (excluding DME RI Index and DME Min Vol Total) 35.587
DME Focus Total 15.919
DME Fundamental Total 3.748
DME Quant Total 2.349
DME Small Cap & Midcap 1.258
DME Transition Portfolios 12.337
Emerging Markets Equity 30.907

Strategic Real Estate 43.442
Tactical Real Estate 7.007
Liquid Commodities 18.913
Illiquid Commodities 3.170
Hedge Funds 8.272
Thematic Investments 342
Private Equity Combined Pools 56.005
Infrastructure Combined Active Strategies 32.237

Emerging Market Debt Index 15.663
DME RI Index (Developed Markets Equity) 113.146
DME Minimum Volatility Total (Developed Markets Equity) 3.757

Treasury Global 51.606
Long Duration Treasury Global 67.955
Fixed Income Liability Hedging 11.545
Index Linked Bonds 119
"""

import re

total_aum = 0
total_equity = 0
for line in lines.strip().split('\n'):
    if not line.strip(): continue
    
    parts = line.split()
    # the last part is the number in formatted string like "56.631"
    val_str = parts[-1].replace('.', '')
    try:
        val = int(val_str)
        total_aum += val
        if "Equity" in line or "DME " in line or "DME\t" in line:
            total_equity += val
    except:
        pass

print(f"Total AUM: {total_aum}")
print(f"Total Equity: {total_equity}")
print(f"Equity %: {total_equity / total_aum * 100:.2f}%")
