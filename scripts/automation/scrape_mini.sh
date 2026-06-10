#!/bin/zsh
# Daily scrape orchestrator on the Mac mini (always-on). REPLACES the old MBP
# launchd + AppleScript-.app + Full-Disk-Access chain: the mini's repo is a
# plain clone (~/pensioenfondsen-app, NOT in Google Drive), so launchd can read
# it directly — no TCC hack, and the always-on mini removes the MBP-sleep
# fragility that left the news stale.
#
# Flow: drop any partial DB writes from a failed prior run, pull latest code,
# run the scrape worker (scrape_push.sh = monitor + news-parser + commit +
# push), and on a new commit restart the dashboard (clear its cache) and
# regenerate alerts on the fresh data.
#
# Wired via launchd nl.wuite.pensioenfondsen.scrape (StartCalendarInterval).

export PATH=/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin
APP="$HOME/pensioenfondsen-app"
PY="$APP/.venv/bin/python3"
LOG="$APP/logs/server/scrape.launchd.log"
DASH="gui/$(id -u)/nl.wuite.pensioenfondsen.dashboard"

{
  echo "=== $(date '+%F %T') scrape-orchestrator start ==="
  cd "$APP" || { echo "[FATAL] geen app-dir $APP"; exit 1; }
  # A failed run can leave pension_funds.db dirty (monitor writes mid-run);
  # discard so the ff-pull succeeds. Those findings get re-scraped now.
  git checkout -- data/processed/pension_funds.db 2>/dev/null
  git pull --ff-only origin main 2>&1 | tail -3
  before="$(git rev-parse HEAD)"
  PENSIOEN_PROJECT_DIR="$APP" PENSIOEN_PYTHON="$PY" \
    zsh "$APP/scripts/automation/scrape_push.sh"
  rc=$?
  after="$(git rev-parse HEAD)"
  if [ "$before" != "$after" ]; then
    echo "nieuwe data ($before -> $after) — dashboard herstart + alerts"
    launchctl kickstart -k "$DASH"
    "$PY" "$APP/scripts/automation/generate_alerts.py" --all-funds || true
  else
    echo "geen nieuwe commit — geen restart/alerts"
  fi
  echo "scrape rc=$rc — done $(date '+%F %T')"
} >> "$LOG" 2>&1
