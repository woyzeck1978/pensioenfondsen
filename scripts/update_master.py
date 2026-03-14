import os
import subprocess
import time
from datetime import datetime

base_dir = "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
log_file = os.path.join(base_dir, "data/processed/update.log")

def write_log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}\\n"
    print(message)
    with open(log_file, "a") as f:
        f.write(log_entry)

def run_script(script_path):
    write_log(f"Starting {os.path.basename(script_path)}...")
    try:
        start_time = time.time()
        # Use subprocess to run the script and capture output if needed
        result = subprocess.run(['python3', script_path], capture_output=True, text=True, check=True)
        duration = time.time() - start_time
        write_log(f"Success ({duration:.1f}s): {os.path.basename(script_path)}")
        return True
    except subprocess.CalledProcessError as e:
        write_log(f"ERROR running {os.path.basename(script_path)}:")
        write_log(e.stdout)
        write_log(e.stderr)
        return False
    except Exception as e:
        write_log(f"UNEXPECTED ERROR running {os.path.basename(script_path)}: {e}")
        return False

def main():
    write_log("="*50)
    write_log("PENSION FUNDS DATABASE AUTOMATED UPDATE STARTED")
    write_log("="*50)

    # Core updates that are stable and run fairly quickly
    scripts_to_run = [
        os.path.join(base_dir, "scripts/data_collection/scrape_annual_reports.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_metrics.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_dekkingsgraad.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_asset_classes.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_benchmarks.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_uitvoerder.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_equity_funds.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_esg_metrics.py"),
        os.path.join(base_dir, "scripts/document_parsing/extract_costs.py"),
        os.path.join(base_dir, "scripts/data_collection/scrape_board_members.py"),
        os.path.join(base_dir, "scripts/data_collection/scrape_uitvoerder.py"),
        os.path.join(base_dir, "scripts/utils_and_viz/export_excel.py")
    ]

    for script in scripts_to_run:
        if os.path.exists(script):
            success = run_script(script)
            if not success:
                write_log(f"Aborting update pipeline due to failure in {os.path.basename(script)}.")
                break 
        else:
            write_log(f"WARNING: Script not found at {script}")

    write_log("="*50)
    write_log("PENSION FUNDS DATABASE AUTOMATED UPDATE FINISHED")
    write_log("="*50)

if __name__ == "__main__":
    main()
