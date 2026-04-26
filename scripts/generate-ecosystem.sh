#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PANEL_DIR_DEFAULT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PANEL_DIR="${PANEL_DIR:-$PANEL_DIR_DEFAULT}"
TEMPLATE_FILE="${PANEL_DIR}/ecosystem.config.template.js"
OUTPUT_FILE="${PANEL_DIR}/ecosystem.config.js"
STATE_FILE="${PANEL_DIR}/data/runtime-scaling.env"
QUIET=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --panel-dir)
      PANEL_DIR="$2"
      TEMPLATE_FILE="${PANEL_DIR}/ecosystem.config.template.js"
      OUTPUT_FILE="${PANEL_DIR}/ecosystem.config.js"
      STATE_FILE="${PANEL_DIR}/data/runtime-scaling.env"
      shift 2
      ;;
    --quiet)
      QUIET=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [ ! -f "$TEMPLATE_FILE" ]; then
  echo "Template file not found: $TEMPLATE_FILE" >&2
  exit 1
fi

cpu_cores() {
  if command -v nproc >/dev/null 2>&1; then
    nproc
    return
  fi

  if command -v getconf >/dev/null 2>&1; then
    getconf _NPROCESSORS_ONLN
    return
  fi

  echo 1
}

total_ram_mb() {
  local mem_kb
  mem_kb="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || true)"
  if [ -z "$mem_kb" ]; then
    echo 1024
    return
  fi

  echo $((mem_kb / 1024))
}

TOTAL_CORES="$(cpu_cores)"
TOTAL_RAM_MB="$(total_ram_mb)"
SAFE_RAM_MB=$((TOTAL_RAM_MB * 80 / 100))

if [ "$TOTAL_CORES" -le 2 ]; then
  INSTANCES=1
else
  INSTANCES=$((TOTAL_CORES - 2))
fi

# Keep at least 192 MB target per worker by reducing workers on low-RAM hosts.
MIN_WORKER_TARGET_MB=192
if [ "$INSTANCES" -gt 1 ] && [ "$SAFE_RAM_MB" -gt 0 ]; then
  POSSIBLE_INSTANCES=$((SAFE_RAM_MB / MIN_WORKER_TARGET_MB))
  if [ "$POSSIBLE_INSTANCES" -lt 1 ]; then
    POSSIBLE_INSTANCES=1
  fi
  if [ "$POSSIBLE_INSTANCES" -lt "$INSTANCES" ]; then
    INSTANCES="$POSSIBLE_INSTANCES"
  fi
fi

if [ "$INSTANCES" -lt 1 ]; then
  INSTANCES=1
fi

WORKER_RAM_MB=$((SAFE_RAM_MB / INSTANCES))
if [ "$WORKER_RAM_MB" -lt 128 ]; then
  WORKER_RAM_MB=128
fi

MAX_OLD_SPACE_MB=$((WORKER_RAM_MB * 85 / 100))
if [ "$MAX_OLD_SPACE_MB" -lt 96 ]; then
  MAX_OLD_SPACE_MB=96
fi

APP_SCRIPT_PATH="${PANEL_DIR}/index.js"

tmp_ecosystem="$(mktemp)"
sed \
  -e "s|__SCRIPT_PATH__|${APP_SCRIPT_PATH}|g" \
  -e "s|__PANEL_DIR__|${PANEL_DIR}|g" \
  -e "s|__INSTANCES__|${INSTANCES}|g" \
  -e "s|__MAX_OLD_SPACE_MB__|${MAX_OLD_SPACE_MB}|g" \
  -e "s|__WORKER_RAM_MB__|${WORKER_RAM_MB}|g" \
  "$TEMPLATE_FILE" > "$tmp_ecosystem"

mkdir -p "$(dirname "$STATE_FILE")"
tmp_state="$(mktemp)"
cat > "$tmp_state" <<EOF
TOTAL_CORES=${TOTAL_CORES}
TOTAL_RAM_MB=${TOTAL_RAM_MB}
SAFE_RAM_MB=${SAFE_RAM_MB}
INSTANCES=${INSTANCES}
WORKER_RAM_MB=${WORKER_RAM_MB}
MAX_OLD_SPACE_MB=${MAX_OLD_SPACE_MB}
EOF

CHANGED=0
if [ ! -f "$OUTPUT_FILE" ] || ! cmp -s "$tmp_ecosystem" "$OUTPUT_FILE"; then
  mv "$tmp_ecosystem" "$OUTPUT_FILE"
  CHANGED=1
else
  rm -f "$tmp_ecosystem"
fi

if [ ! -f "$STATE_FILE" ] || ! cmp -s "$tmp_state" "$STATE_FILE"; then
  mv "$tmp_state" "$STATE_FILE"
  CHANGED=1
else
  rm -f "$tmp_state"
fi

if [ "$QUIET" -eq 0 ]; then
  echo "Generated ${OUTPUT_FILE} using ${TOTAL_CORES} cores and ${TOTAL_RAM_MB} MB RAM." >&2
fi

printf 'TOTAL_CORES=%s\n' "$TOTAL_CORES"
printf 'TOTAL_RAM_MB=%s\n' "$TOTAL_RAM_MB"
printf 'SAFE_RAM_MB=%s\n' "$SAFE_RAM_MB"
printf 'INSTANCES=%s\n' "$INSTANCES"
printf 'WORKER_RAM_MB=%s\n' "$WORKER_RAM_MB"
printf 'MAX_OLD_SPACE_MB=%s\n' "$MAX_OLD_SPACE_MB"
printf 'CHANGED=%s\n' "$CHANGED"
