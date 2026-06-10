#!/usr/bin/env bash
#
# setup_server.sh — zet het pensioenfondsen-dashboard op als always-on service
# op deze machine (bedoeld voor de Mac mini, herhaalbaar op de M1 MBP voor HA).
#
# Waarom een lokale clone i.p.v. direct uit Google Drive serveren:
#   launchd mag bestanden in de Google Drive CloudStorage-mount niet lezen
#   (macOS TCC -> "operation not permitted"). Daarom draait de service vanuit
#   een lokale git-clone buiten Drive; data ververst via `git pull` uit main,
#   waar de bi-daily scrape-job de verse pension_funds.db al naartoe pusht.
#
# Het script is IDEMPOTENT: opnieuw draaien = clone updaten, deps bijwerken,
# plists herschrijven. Draai het over SSH op de doelmachine:
#   ssh macmini 'zsh "/Users/webkowuite/Library/CloudStorage/GoogleDrive-webko@wuitepartners.nl/My Drive/03_Reference/Investing/Pensioenfondsen/Nederlandse-pensioenfondsen/scripts/automation/setup_server.sh"'
#
set -euo pipefail

REPO_URL="https://github.com/woyzeck1978/pensioenfondsen.git"
APP_DIR="$HOME/pensioenfondsen-app"
VENV="$APP_DIR/.venv"
DASHBOARD="$APP_DIR/scripts/utils_and_viz/dashboard.py"
LOG_DIR="$APP_DIR/logs/server"
LA_DIR="$HOME/Library/LaunchAgents"
CF_DIR="$HOME/.cloudflared"
PORT=8501
HOSTNAME_PUBLIC="pensioenfondsen.webkowuite.nl"
TUNNEL_NAME="pensioenfondsen"
UID_NUM="$(id -u)"

# --- homebrew / cloudflared pad bepalen (Apple Silicon) ---
if [ -x /opt/homebrew/bin/brew ]; then BREW=/opt/homebrew/bin/brew
elif [ -x /usr/local/bin/brew ]; then BREW=/usr/local/bin/brew
else echo "FOUT: homebrew niet gevonden. Installeer eerst brew."; exit 1; fi
CLOUDFLARED="$("$BREW" --prefix)/bin/cloudflared"

# Tailscale-IP: een dashboard-managed tunnel kan meerdere connectors hebben, dus
# de origin moet via het tailnet bereikbaar zijn (niet 127.0.0.1). Streamlit
# bindt daarom op 0.0.0.0 en de CF-ingress wijst naar dit IP.
TS_BIN=/Applications/Tailscale.app/Contents/MacOS/Tailscale
TS_IP="$({ [ -x "$TS_BIN" ] && "$TS_BIN" ip -4 2>/dev/null; command -v tailscale >/dev/null 2>&1 && tailscale ip -4 2>/dev/null; } | head -1)"
[ -z "$TS_IP" ] && TS_IP="127.0.0.1"

echo "==> Doelmachine: $(scutil --get ComputerName)  (user: $USER, uid: $UID_NUM)"
echo "==> App-dir:     $APP_DIR"

# --- 1. cloudflared aanwezig? ---
if [ ! -x "$CLOUDFLARED" ]; then
  echo "==> cloudflared installeren via brew..."
  "$BREW" install cloudflared
fi
echo "==> cloudflared: $("$CLOUDFLARED" --version)"

# --- 1b. vrije poort kiezen + bestaande tunnel detecteren ---
# Deze machine draait mogelijk al een ander Streamlit-dashboard (bv. 'energie'
# op 8501) en/of een token-based cloudflared-tunnel. Daar omheen werken:
while lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; do
  echo "==> poort $PORT bezet — probeer $((PORT+1))"; PORT=$((PORT+1))
done
echo "==> Dashboard-poort: $PORT"
if launchctl list 2>/dev/null | grep -qiE 'cloudflared'; then
  REUSE_TUNNEL=1
  echo "==> Bestaande cloudflared-tunnel gedetecteerd — die hergebruiken"
  echo "    (geen eigen config.yml/login; hostname voeg je toe in het CF-dashboard)."
else
  REUSE_TUNNEL=0
fi

# --- 2. repo clonen of bijwerken ---
if [ -d "$APP_DIR/.git" ]; then
  echo "==> Bestaande clone bijwerken (git pull)..."
  git -C "$APP_DIR" pull --ff-only
else
  echo "==> Repo clonen naar $APP_DIR..."
  git clone "$REPO_URL" "$APP_DIR"
fi

# --- 3. lokale venv + dashboard-deps ---
if [ ! -x "$VENV/bin/python3" ]; then
  echo "==> Lokale venv aanmaken..."
  python3 -m venv "$VENV"
fi
echo "==> Dashboard-deps installeren..."
"$VENV/bin/python3" -m pip install --upgrade pip -q
"$VENV/bin/python3" -m pip install -q -r "$APP_DIR/requirements.txt"

mkdir -p "$LOG_DIR" "$LA_DIR" "$CF_DIR"

# --- 4. cloudflared ingress-config (alleen bij eigen, lokaal beheerde tunnel) ---
if [ "$REUSE_TUNNEL" = "1" ]; then
  echo "==> Hergebruik bestaande tunnel — geen $CF_DIR/config.yml nodig."
elif [ ! -f "$CF_DIR/config.yml" ]; then
  echo "==> $CF_DIR/config.yml schrijven..."
  cat > "$CF_DIR/config.yml" <<EOF
tunnel: $TUNNEL_NAME
ingress:
  - hostname: $HOSTNAME_PUBLIC
    service: http://localhost:$PORT
  - service: http_status:404
EOF
else
  echo "==> $CF_DIR/config.yml bestaat al — ongemoeid gelaten."
fi

# --- 5. launchd: dashboard-service (Streamlit onder caffeinate) ---
DASH_PLIST="$LA_DIR/nl.wuite.pensioenfondsen.dashboard.plist"
echo "==> Dashboard-plist schrijven: $DASH_PLIST"
cat > "$DASH_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>nl.wuite.pensioenfondsen.dashboard</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/caffeinate</string>
    <string>-i</string>
    <string>$VENV/bin/python3</string>
    <string>-m</string><string>streamlit</string><string>run</string>
    <string>$DASHBOARD</string>
    <string>--server.port</string><string>$PORT</string>
    <string>--server.address</string><string>0.0.0.0</string>
    <string>--server.headless</string><string>true</string>
    <string>--server.enableCORS</string><string>false</string>
    <string>--server.enableXsrfProtection</string><string>false</string>
  </array>
  <key>WorkingDirectory</key><string>$APP_DIR</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>$LOG_DIR/dashboard.out.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/dashboard.err.log</string>
</dict>
</plist>
EOF

# --- 6. launchd: cloudflared-tunnel-service (alleen bij eigen lokale tunnel) ---
TUN_PLIST="$LA_DIR/nl.wuite.pensioenfondsen.tunnel.plist"
if [ "$REUSE_TUNNEL" = "1" ]; then
  echo "==> Hergebruik bestaande tunnel — geen eigen tunnel-plist."
else
  echo "==> Tunnel-plist schrijven: $TUN_PLIST"
  cat > "$TUN_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>nl.wuite.pensioenfondsen.tunnel</string>
  <key>ProgramArguments</key>
  <array>
    <string>$CLOUDFLARED</string>
    <string>tunnel</string>
    <string>--config</string><string>$CF_DIR/config.yml</string>
    <string>run</string><string>$TUNNEL_NAME</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>$LOG_DIR/tunnel.out.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/tunnel.err.log</string>
</dict>
</plist>
EOF
fi

# --- 6b. dagelijkse git-pull-timer (data verversen + dashboard herstarten) ---
PULL_WRAPPER="$HOME/bin/pensioenfondsen_pull.sh"
mkdir -p "$HOME/bin"
echo "==> Pull-wrapper schrijven: $PULL_WRAPPER"
# Quoted heredoc: niets expandeert nu; wrapper bepaalt paden op runtime via
# \$HOME/id -u, zodat hij identiek werkt op Mac mini en M1 MBP.
cat > "$PULL_WRAPPER" <<'PULLEOF'
#!/bin/zsh
# Ververst de lokale clone en herstart de dashboard-service alleen bij nieuwe data.
export PATH=/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin
APP_DIR="$HOME/pensioenfondsen-app"
LOG="$APP_DIR/logs/server/pull.log"
DASH="gui/$(id -u)/nl.wuite.pensioenfondsen.dashboard"
{
  echo "=== $(date '+%Y-%m-%d %H:%M:%S') pull ==="
  before="$(git -C "$APP_DIR" rev-parse HEAD 2>/dev/null)"
  git -C "$APP_DIR" pull --ff-only
  after="$(git -C "$APP_DIR" rev-parse HEAD 2>/dev/null)"
  if [ "$before" != "$after" ]; then
    echo "nieuwe data ($before -> $after) — dashboard herstarten"
    launchctl kickstart -k "$DASH"
  else
    echo "geen wijzigingen"
  fi
} >> "$LOG" 2>&1
PULLEOF
chmod +x "$PULL_WRAPPER"

PULL_PLIST="$LA_DIR/nl.wuite.pensioenfondsen.pull.plist"
echo "==> Pull-plist schrijven: $PULL_PLIST"
cat > "$PULL_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>nl.wuite.pensioenfondsen.pull</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>$PULL_WRAPPER</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict><key>Hour</key><integer>6</integer><key>Minute</key><integer>30</integer></dict>
  <key>RunAtLoad</key><false/>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>$LOG_DIR/pull.launchd.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/pull.launchd.log</string>
</dict>
</plist>
EOF

# --- 7. dashboard-service (her)laden + health-check ---
echo "==> Dashboard-service (her)laden..."
launchctl bootout   "gui/$UID_NUM" "$DASH_PLIST" 2>/dev/null || true
launchctl bootstrap "gui/$UID_NUM" "$DASH_PLIST"
echo "==> Pull-timer (her)laden..."
launchctl bootout   "gui/$UID_NUM" "$PULL_PLIST" 2>/dev/null || true
launchctl bootstrap "gui/$UID_NUM" "$PULL_PLIST"
echo "==> Wachten tot Streamlit luistert..."
for i in $(seq 1 30); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/_stcore/health" 2>/dev/null || true)"
  [ "$code" = "200" ] && break
  sleep 1
done
echo "==> Streamlit health: ${code:-geen antwoord}"

echo
echo "============================================================"
echo " Lokale dashboard-service draait nu op http://127.0.0.1:$PORT"
echo "============================================================"
echo
if [ "$REUSE_TUNNEL" = "1" ]; then
cat <<EOF
NOG TE DOEN (eenmalig) — bestaande tunnel hergebruiken:

  Open het Cloudflare Zero-Trust-dashboard
    one.dash.cloudflare.com -> Networks -> Tunnels -> (de bestaande tunnel)
    -> tab Public Hostname -> Add a public hostname:
        Subdomain : pensioenfondsen
        Domain    : webkowuite.nl
        Service   : HTTP  ->  $TS_IP:$PORT

  LET OP: gebruik het Tailscale-IP ($TS_IP), NIET localhost/127.0.0.1 — de
  tunnel heeft meerdere connectors en de origin moet via het tailnet
  bereikbaar zijn (zelfde patroon als de andere dashboards). Streamlit bindt
  hiervoor op 0.0.0.0.

  Cloudflare maakt zelf het DNS-record; de draaiende cloudflared pikt het
  live op (geen herstart nodig). Daarna is https://$HOSTNAME_PUBLIC live.
EOF
else
cat <<EOF
NOG TE DOEN (eenmalig, voor de publieke tunnel) — draai op DEZE machine:

  1. $CLOUDFLARED tunnel login
       -> opent/print een URL; autoriseer de zone 'webkowuite.nl'.
  2. $CLOUDFLARED tunnel create $TUNNEL_NAME
  3. $CLOUDFLARED tunnel route dns $TUNNEL_NAME $HOSTNAME_PUBLIC
  4. launchctl bootstrap gui/$UID_NUM "$TUN_PLIST"

Daarna is https://$HOSTNAME_PUBLIC live.
EOF
fi
cat <<EOF

Data-refresh: dagelijks om 06:30 ververst nl.wuite.pensioenfondsen.pull de
clone (git pull) en herstart het dashboard alleen bij nieuwe commits.

Beheer:
  launchctl list | grep pensioen
  tail -f "$LOG_DIR"/dashboard.err.log
  tail -f "$LOG_DIR"/pull.log
  launchctl kickstart gui/$UID_NUM/nl.wuite.pensioenfondsen.pull   # nu verversen
  git -C "$APP_DIR" pull --ff-only                                 # handmatig verversen
EOF
