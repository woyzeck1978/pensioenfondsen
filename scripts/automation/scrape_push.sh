#!/bin/zsh
# Bi-daily scrape + push for woyzeck1978/pensioenfondsen.
#
# Triggered by ~/Library/LaunchAgents/nl.wuite.pensioenfondsen.scrape.plist
# (StartInterval = 172800s = 2 dagen). Runs the website monitor + news
# parser, then commits and pushes only if the DB actually changed.
#
# Logs to logs/cron/<timestamp>.log AND to stdout/stderr (which launchd
# writes to logs/cron/launchd.{out,err}).
#
# Manual invocation (test):
#   scripts/automation/scrape_push.sh

set -u
set -o pipefail

PROJECT_DIR="/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/03_Reference/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen"
PYTHON="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3"
GIT="/usr/bin/git"
DB_REL="data/processed/pension_funds.db"

cd "$PROJECT_DIR" || { echo "[FATAL] kan project-dir niet vinden: $PROJECT_DIR" >&2; exit 2; }

mkdir -p logs/cron
TS=$(date '+%Y%m%d_%H%M%S')
LOG="logs/cron/${TS}.log"

# Tee everything to a per-run log file and also stdout (which launchd routes
# to launchd.out/launchd.err in the same dir).
exec > >(tee -a "$LOG") 2>&1

echo "=================================================================="
echo "scrape_push.sh — start $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "=================================================================="

baseline=$(sqlite3 "$DB_REL" "
    SELECT
      (SELECT COUNT(*) FROM scraped_documents),
      (SELECT COUNT(*) FROM news_articles)
")
echo "[pre]  scraped_documents=$(echo $baseline | cut -d'|' -f1)  news_articles=$(echo $baseline | cut -d'|' -f2)"

echo
echo "[1/3] monitor_websites_concurrent.py"
cd scripts/data_collection
if ! "$PYTHON" -u monitor_websites_concurrent.py; then
    echo "[FATAL] monitor exited non-zero — abort run" >&2
    exit 3
fi
cd "$PROJECT_DIR"

echo
echo "[2/3] parse_news_articles.py"
cd scripts/data_collection
if ! "$PYTHON" -u parse_news_articles.py --workers 12; then
    echo "[FATAL] parser exited non-zero — abort run" >&2
    exit 4
fi
cd "$PROJECT_DIR"

# NB: alerts worden NIET hier gegenereerd. De scraper draait op de MBP, maar
# de live dashboard + watchlist + app_state.db leven op de Mac mini. Alerts
# worden daarom op de mini gegenereerd, ná de pull-job (zie
# ~/bin/pensioenfondsen_pull.sh op de mini) — daar staat de echte watchlist en
# de net-gepullde data, en de feed die de live app leest.

after=$(sqlite3 "$DB_REL" "
    SELECT
      (SELECT COUNT(*) FROM scraped_documents),
      (SELECT COUNT(*) FROM news_articles)
")
echo
echo "[post] scraped_documents=$(echo $after | cut -d'|' -f1)  news_articles=$(echo $after | cut -d'|' -f2)"

# Skip commit if DB is byte-identical to HEAD (rare but possible)
if "$GIT" diff --quiet -- "$DB_REL"; then
    echo "[3/3] geen DB-verandering — niets te committen."
    echo "done $(date '+%Y-%m-%d %H:%M:%S %Z')"
    exit 0
fi

echo
echo "[3/3] commit + push"
"$GIT" add "$DB_REL"
"$GIT" commit -m "auto: bi-daily scrape ($(date '+%Y-%m-%d %H:%M'))" || { echo "[FATAL] commit faalde" >&2; exit 5; }

# Push with a short retry — Google Drive sync occasionally locks files
for attempt in 1 2 3; do
    if "$GIT" push origin main; then
        echo "[ok] push gelukt op poging $attempt"
        echo "done $(date '+%Y-%m-%d %H:%M:%S %Z')"
        exit 0
    fi
    echo "[warn] push poging $attempt mislukt, opnieuw over 30s..."
    sleep 30
done

echo "[FATAL] push faalde na 3 pogingen" >&2
exit 6
