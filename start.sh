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
if [ -f "$SUDOERS_SRC" ] && [ ! -f /etc/sudoers.d/adpanel-panel ]; then
    echo "[setup] Installing sudoers rules for adpanel user..."
    cp "$SUDOERS_SRC" /etc/sudoers.d/adpanel-panel
    chmod 0440 /etc/sudoers.d/adpanel-panel
    if ! visudo -cf /etc/sudoers.d/adpanel-panel &>/dev/null; then
        echo "[WARN] Sudoers file validation failed, removing..."
        rm -f /etc/sudoers.d/adpanel-panel
    fi
fi

PANEL_DIR="$(cd "$(dirname "$0")" && pwd)"
chown -R adpanel:adpanel "$PANEL_DIR" 2>/dev/null || true

echo "ADPanel runtime preparation complete."
