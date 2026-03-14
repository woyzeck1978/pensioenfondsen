# Dutch Pension Funds Equity Strategy Tracker

## 🎯 Project Objective
The primary goal of this project is to build a comprehensive dataset of Dutch pension funds, specifically focusing on extracting granular equity strategy information from their 2024 annual reports (PDFs). 

Key metrics being tracked include:
- `equity_allocation_pct`: The percentage of the fund allocated to equities.
- `equity_strategy_notes`: Qualitative strategy notes describing their equity approach.

## 🏗️ Architecture & Assets

### Data Storage
- **Database:** `data/pension_funds.db` (SQLite database containing the `funds` table with all extracted info)
- **Reports:** `data/pension_funds.xlsx` (Excel export for easy viewing)

### Data Sources & Raw Files
- **Annual Reports (PDFs):** Tracked and downloaded for each fund (stored in `data/annual_reports` or similar directories).
- **Web Scraping:** Playwright scripts used to scrape pension fund websites and directories for documents and metadata.

### Scripts
- Located in the `scripts/` directory.
- Include web scrapers (Playwright) and PDF parsers to extract financial metrics.

## ⚠️ Known Challenges & Quirks
1. **Dynamic Web Elements:** Some fund websites use custom HTML elements that break standard Playwright locators, requiring custom-tailored scraping logic for specific funds.
2. **PDF Parsing Variability:** Annual reports do not follow a standardized structure. 
   - Example: The Hoogovens 2024 annual report (`106_Hoogovens.pdf`) required specific logic to extract the investment mix and equity allocation from page 16.
3. **Missing Data:** A significant focus is filling in the missing `equity_strategy_notes` and `equity_allocation_pct` fields for funds that haven't been successfully parsed yet.

## 📝 Next Steps / To-Do
- [ ] Debug and refine Playwright scripts for remaining failing funds.
- [ ] Automate or manually review edge-case PDFs (like Hoogovens) that fail standard extraction.
- [ ] Ensure the `funds` database table is fully updated to reflect 2024 equity allocations for all tracked pension funds.
- [ ] Export final updated `pension_funds.xlsx` report.

---
*Note: This file is intended as a living document to track the state of the project and provide context for AI assistants and collaborators.*
