#!/usr/bin/env bash
set -euo pipefail

echo "Preparing ADPanel runtime..."

if ! id -u adpanel &>/dev/null; then
    echo "[setup] Creating system user 'adpanel'..."
    useradd --system --no-create-home --shell /usr/sbin/nologin adpanel 2>/dev/null || true
fi

if [ -d /etc/nginx/snippets ]; then
    chgrp adpanel /etc/nginx/snippets 2>/dev/null || true
    chmod g+w /etc/nginx/snippets 2>/dev/null || true
fi

SUDOERS_SRC="$(dirname "$0")/adpanel-sudoers"
SUDOERS_DST="/etc/sudoers.d/adpanel-panel"
if [ -f "$SUDOERS_SRC" ]; then
    if [ "$(id -u)" -ne 0 ]; then
        echo "[WARN] Not running as root; skipping sudoers sync."
    else
        echo "[setup] Syncing sudoers rules for adpanel user..."
        mkdir -p /etc/sudoers.d
        tmp_sudoers="$(mktemp /etc/sudoers.d/adpanel-panel.tmp.XXXXXX)"
        cp "$SUDOERS_SRC" "$tmp_sudoers"
        chmod 0440 "$tmp_sudoers"

        validate_ok=0
        if command -v visudo >/dev/null 2>&1; then
            if visudo -cf "$tmp_sudoers" >/dev/null 2>&1; then
                validate_ok=1
            else
                echo "[WARN] Sudoers validation failed; keeping existing policy."
            fi
        else
            echo "[WARN] visudo not found; applying sudoers without validation."
            validate_ok=1
        fi

        if [ "$validate_ok" -eq 1 ]; then
            if [ ! -f "$SUDOERS_DST" ] || ! cmp -s "$tmp_sudoers" "$SUDOERS_DST"; then
                mv "$tmp_sudoers" "$SUDOERS_DST"
                chmod 0440 "$SUDOERS_DST"
                echo "[setup] Sudoers rules installed/updated."
            else
                rm -f "$tmp_sudoers"
                echo "[setup] Sudoers rules already up to date."
            fi
        else
            rm -f "$tmp_sudoers"
        fi
    fi
fi

PANEL_DIR="$(cd "$(dirname "$0")" && pwd)"
chown -R adpanel:adpanel "$PANEL_DIR" 2>/dev/null || true

echo "ADPanel runtime preparation complete."
