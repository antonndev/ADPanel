#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PANEL_DIR_DEFAULT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PANEL_DIR="${PANEL_DIR:-$PANEL_DIR_DEFAULT}"
INTERVAL_SECONDS="${PM2_MONITOR_INTERVAL:-300}"
GENERATOR="${PANEL_DIR}/scripts/generate-ecosystem.sh"
ECOSYSTEM_FILE="${PANEL_DIR}/ecosystem.config.js"
PM2_BIN="${PANEL_DIR}/node_modules/.bin/pm2"

if [ ! -x "$PM2_BIN" ] && command -v pm2 >/dev/null 2>&1; then
  PM2_BIN="$(command -v pm2)"
fi

if [ ! -x "$GENERATOR" ]; then
  echo "Generator script not executable: $GENERATOR" >&2
  exit 1
fi

if [ ! -x "$PM2_BIN" ]; then
  echo "pm2 binary not found. Expected ${PANEL_DIR}/node_modules/.bin/pm2 or global pm2." >&2
  exit 1
fi

while true; do
  output="$($GENERATOR --quiet --panel-dir "$PANEL_DIR")"
  changed="$(echo "$output" | awk -F= '/^CHANGED=/{print $2}')"

  if [ "$changed" = "1" ]; then
    # Apply new worker count and memory limits without requiring full host reboot.
    "$PM2_BIN" start "$ECOSYSTEM_FILE" --only adpanel --update-env >/dev/null 2>&1 || true
    "$PM2_BIN" reload adpanel --update-env >/dev/null 2>&1 || "$PM2_BIN" restart adpanel --update-env >/dev/null 2>&1 || true
  fi

  sleep "$INTERVAL_SECONDS"
done
