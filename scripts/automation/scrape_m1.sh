#!/bin/zsh
# Dagelijkse scrape op de MacBook Pro M1. Variant op scrape_mini.sh.
#
# Verschil met de mini: die draait óók het live dashboard, de API en de
# watchlist, en herstart daarom na een nieuwe commit het dashboard en
# genereert alerts. Op de M1 staat niets van dat alles — hier hoeft alleen
# gescrapet en gepusht te worden. De mini pikt de nieuwe data een half uur
# later vanzelf op met zijn pull-job (nl.wuite.pensioenfondsen.pull, 06:30),
# die het dashboard herstart en de alerts genereert.
#
# Flow: partiële DB-schrijfacties van een mislukte vorige run weggooien, code
# ophalen, scrape_push.sh draaien (monitor + nieuwsparser + kwaliteitscontrole
# + commit + push).
#
# Aangestuurd door launchd nl.wuite.pensioenfondsen.scrape (StartCalendarInterval).

export PATH=/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin
APP="$HOME/pensioenfondsen-app"
PY="$APP/.venv/bin/python3"
LOG="$APP/logs/server/scrape.launchd.log"

mkdir -p "$APP/logs/server"

{
  echo "=== $(date '+%F %T') scrape-orchestrator (M1) start ==="
  cd "$APP" || { echo "[FATAL] geen app-dir $APP"; exit 1; }

  # Een mislukte run kan pension_funds.db vuil achterlaten (de monitor schrijft
  # tussentijds); weggooien zodat de fast-forward-pull slaagt. Die bevindingen
  # worden nu opnieuw gescrapet.
  git checkout -- data/processed/pension_funds.db 2>/dev/null
  git pull --ff-only origin main 2>&1 | tail -3

  before="$(git rev-parse HEAD)"
  PENSIOEN_PROJECT_DIR="$APP" PENSIOEN_PYTHON="$PY" \
    zsh "$APP/scripts/automation/scrape_push.sh"
  rc=$?
  after="$(git rev-parse HEAD)"

  if [ "$before" != "$after" ]; then
    echo "nieuwe data ($before -> $after) — de mini pikt dit om 06:30 op"
  else
    echo "geen nieuwe commit"
  fi
  echo "scrape rc=$rc — done $(date '+%F %T')"
} >> "$LOG" 2>&1
