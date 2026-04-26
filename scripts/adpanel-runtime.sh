#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PANEL_DIR_DEFAULT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PANEL_DIR="${PANEL_DIR:-$PANEL_DIR_DEFAULT}"
GENERATOR="${PANEL_DIR}/scripts/generate-ecosystem.sh"
MONITOR="${PANEL_DIR}/scripts/adpanel-autoscale-monitor.sh"
ECOSYSTEM_FILE="${PANEL_DIR}/ecosystem.config.js"
PM2_RUNTIME_BIN="${PANEL_DIR}/node_modules/.bin/pm2-runtime"

if [ ! -x "$PM2_RUNTIME_BIN" ] && command -v pm2-runtime >/dev/null 2>&1; then
  PM2_RUNTIME_BIN="$(command -v pm2-runtime)"
fi

if [ ! -x "$GENERATOR" ]; then
  echo "Generator script not found: $GENERATOR" >&2
  exit 1
fi

if [ ! -x "$MONITOR" ]; then
  echo "Monitor script not found: $MONITOR" >&2
  exit 1
fi

if [ ! -x "$PM2_RUNTIME_BIN" ]; then
  echo "pm2-runtime not found. Run npm install to install dependencies." >&2
  exit 1
fi

"$GENERATOR" --panel-dir "$PANEL_DIR" >/dev/null

"$MONITOR" &
MONITOR_PID=$!

cleanup() {
  if [ -n "${MONITOR_PID:-}" ] && kill -0 "$MONITOR_PID" >/dev/null 2>&1; then
    kill "$MONITOR_PID" >/dev/null 2>&1 || true
    wait "$MONITOR_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup INT TERM EXIT

exec "$PM2_RUNTIME_BIN" start "$ECOSYSTEM_FILE" --env production
