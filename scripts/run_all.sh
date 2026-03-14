#!/bin/bash
set -e
python3 analyze_reports.py
python3 analyze_geography.py
python3 analyze_geo_weights.py
python3 analyze_management_style.py
python3 analyze_esg_factors.py
python3 update_equity_strategies_table.py
python3 export_excel.py
python3 plot_all_strategies.py
