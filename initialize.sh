#!/usr/bin/env bash

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  RED='\033[0;31m'
  GREEN='\033[0;32m'
  YELLOW='\033[1;33m'
  CYAN='\033[0;36m'
  MAGENTA='\033[1;35m'
  BLUE='\033[0;34m'
  WHITE='\033[1;37m'
  BOLD='\033[1m'
  DIM='\033[2m'
  NC='\033[0m'
else
  RED=''
  GREEN=''
  YELLOW=''
  CYAN=''
  MAGENTA=''
  BLUE=''
  WHITE=''
  BOLD=''
  DIM=''
  NC=''
fi

UI_RULE="======================================================================"
UI_SUBRULE="----------------------------------------------------------------------"

ui_rule() {
  local color="${1:-$DIM}"
  printf "%b%s%b\n" "${color}" "${UI_RULE}" "${NC}"
}

ui_subrule() {
  local color="${1:-$DIM}"
  printf "%b%s%b\n" "${color}" "${UI_SUBRULE}" "${NC}"
}

ui_banner() {
  printf '\n'
  ui_rule "${MAGENTA}${BOLD}"
  printf "%b%s%b\n" "${CYAN}${BOLD}" " ADPanel Initializer " "${NC}"
  ui_rule "${MAGENTA}${BOLD}"
  printf '\n'
}

ui_section() {
  printf '\n'
  ui_subrule "${BLUE}${DIM}"
  printf "%b%s%b\n" "${CYAN}${BOLD}" "$1" "${NC}"
  ui_subrule "${BLUE}${DIM}"
}

ui_info() {
  printf "%b%s%b\n" "${CYAN}${BOLD}" "$1" "${NC}"
}

ui_success() {
  printf "%b%s%b\n" "${GREEN}${BOLD}" "$1" "${NC}"
}

ui_warn() {
  printf "%b%s%b\n" "${YELLOW}${BOLD}" "$1" "${NC}"
}

ui_error() {
  printf "%b%s%b\n" "${RED}${BOLD}" "$1" "${NC}"
}

ui_menu_item() {
  printf "  %b%s%b\n" "${WHITE}${BOLD}" "$1" "${NC}"
}

ui_kv() {
  printf "%b%s%b %s\n" "${CYAN}${BOLD}" "$1" "${NC}" "$2"
}

ui_prompt() {
  printf "%b%s%b" "${MAGENTA}${BOLD}" "$1" "${NC}"
}

SUDO=""
if [ "$EUID" -ne 0 ]; then
  SUDO="sudo"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -n "$SCRIPT_DIR" ]; then
  cd "$SCRIPT_DIR" || exit 1
fi

ui_banner
ui_section "Choose an option:"
ui_menu_item "1) Initialize Panel"
ui_menu_item "2) Change an user password"
ui_menu_item "3) Delete an user"
ui_menu_item "4) Create User"

CHOICE="${ADPANEL_INIT_CHOICE:-}"
if [ -z "$CHOICE" ] && [ "${1:-}" == "--choice" ]; then
  CHOICE="${2:-}"
elif [ -z "$CHOICE" ] && [[ "${1:-}" == --choice=* ]]; then
  CHOICE="${1#--choice=}"
fi

if [ -n "$CHOICE" ]; then
  ui_info "Auto-selected option: ${CHOICE}"
else
  read -p "$(ui_prompt "Enter choice (1, 2, 3 or 4): ")" CHOICE
fi

CREATE_USER_SCRIPT=""
if [ -f "${SCRIPT_DIR}/scripts/create-user.js" ]; then
  CREATE_USER_SCRIPT="${SCRIPT_DIR}/scripts/create-user.js"
elif [ -f "${SCRIPT_DIR}/create-user.js" ]; then
  CREATE_USER_SCRIPT="${SCRIPT_DIR}/create-user.js"
fi

OS_ID="unknown"
OS_LIKE=""
PKG_MGR="unknown"
INIT_SYSTEM="unknown"
PKG_METADATA_READY="false"
PLATFORM_SUMMARY_SHOWN="false"

REDIS_SERVER_CMD="redis-server"
REDIS_CLI_CMD="redis-cli"

MYSQL_HOST=""
MYSQL_PORT=""
MYSQL_USER=""
MYSQL_PASSWORD=""
MYSQL_DATABASE=""
MYSQL_URL=""

ADMIN_DEFAULT_AVATAR="https://cdn.jsdelivr.net/gh/antonndev/ADCDn/admin-avatar.webp"
NORMAL_DEFAULT_AVATARS=(
  "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/normal-1.webp"
  "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/normal-2.webp"
  "https://cdn.jsdelivr.net/gh/antonndev/ADCDn/normal-3.webp"
)

cmd_exists() { command -v "$1" >/dev/null 2>&1; }

pick_default_avatar_url() {
  local role="${1:-user}"
  if [ "$role" = "admin" ]; then
    printf "%s" "$ADMIN_DEFAULT_AVATAR"
    return 0
  fi

  local index=0
  if [ "${#NORMAL_DEFAULT_AVATARS[@]}" -gt 1 ]; then
    index=$((RANDOM % ${#NORMAL_DEFAULT_AVATARS[@]}))
  fi

  printf "%s" "${NORMAL_DEFAULT_AVATARS[$index]}"
}

detect_platform() {
  if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_ID="${ID:-unknown}"
    OS_LIKE="${ID_LIKE:-}"
  fi

  if cmd_exists systemctl; then
    INIT_SYSTEM="systemd"
  elif cmd_exists rc-service; then
    INIT_SYSTEM="openrc"
  elif cmd_exists service; then
    INIT_SYSTEM="sysv"
  else
    INIT_SYSTEM="unknown"
  fi

  if cmd_exists apt-get; then
    PKG_MGR="apt"
  elif cmd_exists dnf; then
    PKG_MGR="dnf"
  elif cmd_exists yum; then
    PKG_MGR="yum"
  elif cmd_exists apk; then
    PKG_MGR="apk"
  elif cmd_exists pacman; then
    PKG_MGR="pacman"
  elif cmd_exists zypper; then
    PKG_MGR="zypper"
  else
    PKG_MGR="unknown"
  fi
}

platform_summary() {
  local summary="${OS_ID}"
  if [ -n "$OS_LIKE" ]; then
    summary="${summary} (like ${OS_LIKE})"
  fi
  printf "%s" "$summary"
}

log_platform_summary() {
  if [ "$PLATFORM_SUMMARY_SHOWN" = "true" ]; then
    return 0
  fi

  ui_info "Detected operating system and package manager:"
  ui_kv "  OS:" "$(platform_summary)"
  ui_kv "  Package manager:" "$PKG_MGR"
  ui_kv "  Init system:" "$INIT_SYSTEM"
  PLATFORM_SUMMARY_SHOWN="true"
}

require_sudo_if_needed() {
  if [ "$EUID" -ne 0 ] && ! cmd_exists sudo; then
    ui_error "sudo is required when running this script as a non-root user."
    exit 1
  fi
}

python_ready() {
  cmd_exists python3 || cmd_exists python
}

ensure_python_binary_alias() {
  if cmd_exists python3; then
    return 0
  fi

  if ! cmd_exists python; then
    return 1
  fi

  local python_bin
  python_bin="$(command -v python 2>/dev/null || true)"
  if [ -z "$python_bin" ]; then
    return 1
  fi

  if [ "$EUID" -eq 0 ] || [ -n "$SUDO" ]; then
    $SUDO mkdir -p /usr/local/bin >/dev/null 2>&1 || true
    $SUDO ln -sf "$python_bin" /usr/local/bin/python3 >/dev/null 2>&1 || true
    hash -r 2>/dev/null || true
  fi

  python_ready
}

ensure_supported_package_manager() {
  if [ "$PKG_MGR" != "unknown" ]; then
    return 0
  fi

  ui_error "Could not detect a supported package manager on this system."
  return 1
}

pkg_update() {
  case "$PKG_MGR" in
    apt)
      $SUDO apt-get update -y
      ;;
    dnf)
      $SUDO dnf -y makecache || $SUDO dnf -y check-update || true
      ;;
    yum)
      $SUDO yum -y makecache || $SUDO yum -y check-update || true
      ;;
    apk)
      $SUDO apk update
      ;;
    pacman)
      $SUDO pacman -Sy --noconfirm
      ;;
    zypper)
      $SUDO zypper --non-interactive refresh
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_pkg_metadata() {
  if [ "$PKG_METADATA_READY" = "true" ]; then
    return 0
  fi

  if ! ensure_supported_package_manager; then
    return 1
  fi

  ui_info "Refreshing package metadata for ${PKG_MGR}..."
  if pkg_update; then
    PKG_METADATA_READY="true"
    return 0
  fi

  ui_warn "Package metadata refresh failed; continuing with the existing cache."
  return 0
}

pkg_install() {
  case "$PKG_MGR" in
    apt)
      $SUDO apt-get install -y "$@"
      ;;
    dnf)
      $SUDO dnf install -y "$@"
      ;;
    yum)
      $SUDO yum install -y "$@"
      ;;
    apk)
      $SUDO apk add --no-cache "$@"
      ;;
    pacman)
      $SUDO pacman -S --noconfirm --needed "$@"
      ;;
    zypper)
      $SUDO zypper --non-interactive in -l "$@"
      ;;
    *)
      return 1
      ;;
  esac
}

pkg_install_try_sets() {
  local set
  for set in "$@"; do
    if pkg_install $set >/dev/null 2>&1; then
      return 0
    fi
  done
  set="${*: -1}"
  pkg_install $set
}

install_node_runtime_packages() {
  case "$PKG_MGR" in
    apt|apk|pacman)
      pkg_install_try_sets \
        "nodejs npm"
      ;;
    dnf|yum)
      pkg_install_try_sets \
        "nodejs npm" \
        "nodejs nodejs-npm" \
        "nodejs22 npm" \
        "nodejs22 nodejs22-npm" \
        "nodejs22 nodejs-npm" \
        "nodejs20 npm" \
        "nodejs20 nodejs20-npm" \
        "nodejs20 nodejs-npm" \
        "nodejs18 npm" \
        "nodejs18 nodejs18-npm" \
        "nodejs18 nodejs-npm" \
        "nodejs"
      ;;
    zypper)
      pkg_install_try_sets \
        "nodejs npm" \
        "nodejs22 npm22" \
        "nodejs20 npm20" \
        "nodejs18 npm18" \
        "nodejs16 npm16" \
        "nodejs22" \
        "nodejs20" \
        "nodejs18" \
        "nodejs16" \
        "nodejs"
      ;;
    *)
      return 1
      ;;
  esac
}

install_npm_package_only() {
  case "$PKG_MGR" in
    apt|apk|pacman)
      pkg_install npm
      ;;
    dnf|yum)
      pkg_install_try_sets \
        "npm" \
        "nodejs22-npm" \
        "nodejs20-npm" \
        "nodejs18-npm" \
        "nodejs-npm"
      ;;
    zypper)
      pkg_install_try_sets \
        "npm" \
        "npm22" \
        "npm20" \
        "npm18" \
        "npm16"
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_node_binary_alias() {
  if cmd_exists node; then
    return 0
  fi

  if ! cmd_exists nodejs; then
    return 1
  fi

  local nodejs_bin
  nodejs_bin="$(command -v nodejs 2>/dev/null || true)"
  if [ -z "$nodejs_bin" ]; then
    return 1
  fi

  if [ "$EUID" -eq 0 ] || [ -n "$SUDO" ]; then
    $SUDO mkdir -p /usr/local/bin >/dev/null 2>&1 || true
    $SUDO ln -sf "$nodejs_bin" /usr/local/bin/node >/dev/null 2>&1 || true
    hash -r 2>/dev/null || true
  fi

  cmd_exists node
}

compiler_ready() {
  cmd_exists gcc || cmd_exists cc || cmd_exists clang
}

cpp_compiler_ready() {
  cmd_exists g++ || cmd_exists c++ || cmd_exists clang++
}

install_base_system_packages() {
  local need_base="false"

  if ! cmd_exists curl || ! cmd_exists openssl || ! cmd_exists tar || ! python_ready || ! cmd_exists make; then
    need_base="true"
  fi
  if ! compiler_ready || ! cpp_compiler_ready; then
    need_base="true"
  fi

  if [ "$need_base" != "true" ]; then
    ui_success "Base system packages are already installed."
    return 0
  fi

  if ! ensure_supported_package_manager; then
    return 1
  fi

  log_platform_summary
  ui_info "Installing base system packages for ${OS_ID}..."
  ensure_pkg_metadata >/dev/null 2>&1 || true

  case "$PKG_MGR" in
    apt)
      pkg_install ca-certificates curl openssl tar python3 make g++
      ;;
    dnf|yum)
      pkg_install ca-certificates curl openssl tar python3 make gcc gcc-c++
      ;;
    apk)
      pkg_install ca-certificates curl openssl tar python3 make g++
      ;;
    pacman)
      pkg_install ca-certificates curl openssl tar python make gcc
      ;;
    zypper)
      pkg_install ca-certificates curl openssl tar python3 make gcc-c++
      ;;
    *)
      ui_error "Unsupported package manager for base system packages."
      return 1
      ;;
  esac

  if cmd_exists update-ca-certificates; then
    $SUDO update-ca-certificates >/dev/null 2>&1 || true
  fi
  ensure_python_binary_alias >/dev/null 2>&1 || true

  if ! cmd_exists curl || ! cmd_exists openssl || ! cmd_exists tar || ! python_ready || ! cmd_exists make; then
    ui_error "Failed to install one or more required base system packages."
    return 1
  fi
  if ! compiler_ready || ! cpp_compiler_ready; then
    ui_error "A C/C++ compiler is still missing after package installation."
    return 1
  fi

  ui_success "Base system packages installed successfully."
  return 0
}

ensure_node_runtime() {
  if ! cmd_exists node; then
    ensure_node_binary_alias >/dev/null 2>&1 || true
  fi

  if cmd_exists node && cmd_exists npm; then
    ui_success "Node.js and npm are already installed."
    ui_kv "  node:" "$(node -v 2>/dev/null)"
    ui_kv "  npm:" "$(npm -v 2>/dev/null)"
    return 0
  fi

  if ! ensure_supported_package_manager; then
    return 1
  fi

  log_platform_summary
  ui_info "Installing Node.js and npm..."
  ensure_pkg_metadata >/dev/null 2>&1 || true

  if { ! cmd_exists node || ! cmd_exists npm; } && ! install_node_runtime_packages; then
    ui_warn "Combined Node.js/npm installation attempt failed; trying a smaller npm-only fallback if possible..."
  fi

  if ! cmd_exists node && ! ensure_node_binary_alias >/dev/null 2>&1; then
    ui_error "Node.js is required but could not be installed automatically."
    return 1
  fi

  ensure_node_binary_alias >/dev/null 2>&1 || true

  if ! cmd_exists npm && ! install_npm_package_only; then
    ui_warn "Separate npm package installation failed; checking if npm shipped with Node.js..."
  fi

  ensure_node_binary_alias >/dev/null 2>&1 || true

  if ! cmd_exists node || ! cmd_exists npm; then
    ui_error "Node.js and npm are required but could not be installed automatically."
    return 1
  fi

  ui_success "Node.js and npm installed successfully."
  ui_kv "  node:" "$(node -v 2>/dev/null)"
  ui_kv "  npm:" "$(npm -v 2>/dev/null)"
  return 0
}

project_extensions_ready() {
  [ -d "${SCRIPT_DIR}/node_modules" ] || return 1

  npm ls --omit=dev --depth=0 >/dev/null 2>&1
}

install_project_extensions() {
  if ! ensure_node_runtime; then
    return 1
  fi

  if project_extensions_ready; then
    ui_success "Project extensions are already installed."
    return 0
  fi

  ui_info "Installing extensions..."
  if ! npm install --omit=dev --no-fund --no-audit; then
    ui_error "npm install failed."
    return 1
  fi

  if ! project_extensions_ready; then
    ui_error "Extensions were installed, but required Node modules are still missing."
    return 1
  fi

  ui_success "Project extensions installed successfully."
  return 0
}

ensure_node_prerequisites() {
  log_platform_summary

  if ! install_base_system_packages; then
    return 1
  fi

  if ! ensure_node_runtime; then
    return 1
  fi

  install_project_extensions
}

service_enable() {
  local svc="$1"
  case "$INIT_SYSTEM" in
    systemd)
      $SUDO systemctl enable "$svc" >/dev/null 2>&1 || true
      ;;
    openrc)
      $SUDO rc-update add "$svc" default >/dev/null 2>&1 || true
      ;;
    sysv)
      if cmd_exists update-rc.d; then
        $SUDO update-rc.d "$svc" defaults >/dev/null 2>&1 || true
      elif cmd_exists chkconfig; then
        $SUDO chkconfig "$svc" on >/dev/null 2>&1 || true
      fi
      ;;
  esac
}

service_start() {
  local svc="$1"
  case "$INIT_SYSTEM" in
    systemd)
      $SUDO systemctl start "$svc" >/dev/null 2>&1 || true
      ;;
    openrc)
      $SUDO rc-service "$svc" start >/dev/null 2>&1 || true
      ;;
    sysv)
      $SUDO service "$svc" start >/dev/null 2>&1 || true
      ;;
  esac
}

service_stop() {
  local svc="$1"
  case "$INIT_SYSTEM" in
    systemd)
      $SUDO systemctl stop "$svc" >/dev/null 2>&1 || true
      ;;
    openrc)
      $SUDO rc-service "$svc" stop >/dev/null 2>&1 || true
      ;;
    sysv)
      $SUDO service "$svc" stop >/dev/null 2>&1 || true
      ;;
  esac
}

service_restart() {
  local svc="$1"
  case "$INIT_SYSTEM" in
    systemd)
      $SUDO systemctl restart "$svc" >/dev/null 2>&1 || true
      ;;
    openrc)
      $SUDO rc-service "$svc" restart >/dev/null 2>&1 || true
      ;;
    sysv)
      $SUDO service "$svc" restart >/dev/null 2>&1 || true
      ;;
  esac
}

service_reload() {
  local svc="$1"
  case "$INIT_SYSTEM" in
    systemd)
      $SUDO systemctl reload "$svc" >/dev/null 2>&1 || true
      ;;
    openrc)
      $SUDO rc-service "$svc" reload >/dev/null 2>&1 || service_restart "$svc"
      ;;
    sysv)
      $SUDO service "$svc" reload >/dev/null 2>&1 || service_restart "$svc"
      ;;
  esac
}

detect_redis_commands() {
  if cmd_exists redis-server; then
    REDIS_SERVER_CMD="redis-server"
  elif cmd_exists valkey-server; then
    REDIS_SERVER_CMD="valkey-server"
  elif cmd_exists valkey; then
    REDIS_SERVER_CMD="valkey"
  fi

  if cmd_exists redis-cli; then
    REDIS_CLI_CMD="redis-cli"
  elif cmd_exists valkey-cli; then
    REDIS_CLI_CMD="valkey-cli"
  elif cmd_exists valkey; then
    REDIS_CLI_CMD="redis-cli"
  fi
}

detect_platform
require_sudo_if_needed
detect_redis_commands

setup_adpanel_systemd_service() {
  local service_name="adpanel"
  local service_file="/etc/systemd/system/${service_name}.service"
  local panel_root="$SCRIPT_DIR"
  local runtime_script="${panel_root}/scripts/adpanel-runtime.sh"
  local monitor_script="${panel_root}/scripts/adpanel-autoscale-monitor.sh"
  local ecosystem_generator_script="${panel_root}/scripts/generate-ecosystem.sh"
  local ecosystem_template_file="${panel_root}/ecosystem.config.template.js"
  local ecosystem_file="${panel_root}/ecosystem.config.js"
  local prep_script="${panel_root}/start.sh"
  local pm2_runtime_bin="${panel_root}/node_modules/.bin/pm2-runtime"
  local pm2_home_dir="${panel_root}/.pm2"
  local exec_start_pre_line=""
  local service_user="root"
  local service_group="root"

  if ! cmd_exists systemctl; then
    ui_warn "systemctl not found; skipping ${service_name} systemd service setup."
    return 1
  fi

  if [ ! -f "$runtime_script" ] || [ ! -f "$monitor_script" ] || [ ! -f "$ecosystem_generator_script" ]; then
    ui_error "Required PM2 runtime scripts are missing in ${panel_root}/scripts."
    return 1
  fi

  if [ ! -f "$ecosystem_template_file" ]; then
    ui_error "Expected ecosystem template not found at ${ecosystem_template_file}."
    return 1
  fi

  if [ ! -x "$pm2_runtime_bin" ] && ! cmd_exists pm2-runtime; then
    ui_error "pm2-runtime not found. Ensure dependencies are installed with npm install."
    return 1
  fi

  $SUDO chmod +x "$runtime_script" "$monitor_script" "$ecosystem_generator_script" >/dev/null 2>&1 || true

  if [ ! -f "$prep_script" ]; then
    ui_warn "Expected preparation script not found at ${prep_script}."
    ui_warn "The service will run as root without start.sh integration."
  else
    if ! $SUDO bash "$prep_script"; then
      ui_error "Failed to run start.sh preparation before creating the service."
      return 1
    fi

    if id -u adpanel >/dev/null 2>&1; then
      service_user="adpanel"
      service_group="adpanel"
      exec_start_pre_line="ExecStartPre=/bin/bash ${prep_script}"
    else
      ui_warn "start.sh completed but user 'adpanel' does not exist; the service will run as root."
    fi
  fi

  $SUDO mkdir -p "$pm2_home_dir" >/dev/null 2>&1 || true
  if id -u "$service_user" >/dev/null 2>&1; then
    $SUDO chown -R "$service_user":"$service_group" "$pm2_home_dir" >/dev/null 2>&1 || true
  fi

  if [ "$service_user" = "root" ]; then
    if ! "$ecosystem_generator_script" --panel-dir "$panel_root" >/dev/null; then
      ui_error "Failed to generate PM2 ecosystem configuration."
      return 1
    fi
  else
    if [ "$EUID" -eq 0 ]; then
      if cmd_exists runuser; then
        if ! runuser -u "$service_user" -- "$ecosystem_generator_script" --panel-dir "$panel_root" >/dev/null; then
          ui_error "Failed to generate PM2 ecosystem configuration for user ${service_user}."
          return 1
        fi
      else
        if ! su -s /bin/bash -c "\"$ecosystem_generator_script\" --panel-dir \"$panel_root\" >/dev/null" "$service_user"; then
          ui_error "Failed to generate PM2 ecosystem configuration for user ${service_user}."
          return 1
        fi
      fi
    else
      if ! $SUDO -u "$service_user" "$ecosystem_generator_script" --panel-dir "$panel_root" >/dev/null; then
        ui_error "Failed to generate PM2 ecosystem configuration for user ${service_user}."
        return 1
      fi
    fi
  fi

  cat <<EOF | $SUDO tee "$service_file" >/dev/null
[Unit]
Description=ADPanel PM2 Cluster Service
After=network.target

[Service]
Type=simple
PermissionsStartOnly=true
WorkingDirectory=${panel_root}
User=${service_user}
Group=${service_group}
${exec_start_pre_line}
Environment=NODE_ENV=production
Environment=PANEL_DIR=${panel_root}
Environment=PM2_MONITOR_INTERVAL=300
Environment=PM2_HOME=${pm2_home_dir}
ExecStart=${runtime_script}
Restart=always
RestartSec=10
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=8
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

  if ! $SUDO systemctl daemon-reload; then
    ui_error "Failed to reload systemd daemon after writing ${service_file}."
    return 1
  fi

  if ! $SUDO systemctl enable "$service_name"; then
    ui_error "Failed to enable ${service_name}.service."
    return 1
  fi

  if $SUDO systemctl is-active --quiet "$service_name"; then
    if ! $SUDO systemctl restart "$service_name"; then
      ui_error "Failed to restart ${service_name}.service."
      return 1
    fi
  else
    if ! $SUDO systemctl start "$service_name"; then
      ui_error "Failed to start ${service_name}.service."
      return 1
    fi
  fi

  if $SUDO systemctl is-active --quiet "$service_name"; then
    ui_success "${service_name}.service is active and enabled."
    if [ -f "$ecosystem_file" ]; then
      ui_info "PM2 ecosystem generated at: ${ecosystem_file}"
    fi
  else
    ui_warn "${service_name}.service was created and enabled but is not active."
    ui_warn "Check status with: systemctl status ${service_name}"
  fi

  return 0
}

setup_adpanel_sshterm_systemd_service() {
  local service_name="adpanel-sshterm"
  local service_file="/etc/systemd/system/${service_name}.service"
  local panel_root="$SCRIPT_DIR"
  local sshterm_main="${panel_root}/cmd/sshterm/main.go"
  local go_cache_dir="${panel_root}/.cache-go"
  local go_modcache_dir="${panel_root}/.gomodcache"
  local service_user="root"
  local service_group="root"

  if ! cmd_exists systemctl; then
    ui_warn "systemctl not found; skipping ${service_name} systemd service setup."
    return 1
  fi

  if ! cmd_exists go; then
    ui_warn "Go runtime not found; skipping ${service_name} service setup."
    return 1
  fi

  if [ ! -f "$sshterm_main" ]; then
    ui_warn "SSH terminal source not found at ${sshterm_main}; skipping ${service_name} setup."
    return 1
  fi

  if id -u adpanel >/dev/null 2>&1; then
    service_user="adpanel"
    service_group="adpanel"
  fi

  $SUDO mkdir -p "$go_cache_dir" "$go_modcache_dir" >/dev/null 2>&1 || true
  $SUDO chown -R "${service_user}:${service_group}" "$go_cache_dir" "$go_modcache_dir" >/dev/null 2>&1 || true

  cat <<EOF | $SUDO tee "$service_file" >/dev/null
[Unit]
Description=ADPanel SSH Terminal Service
After=network.target

[Service]
Type=simple
WorkingDirectory=${panel_root}
User=${service_user}
Group=${service_group}
EnvironmentFile=-${panel_root}/.env
Environment=SSH_TERM_PANEL_ROOT=${panel_root}
Environment=SSH_TERM_BIND=0.0.0.0:9393
Environment=HOME=${panel_root}
Environment=GOCACHE=${go_cache_dir}
Environment=GOMODCACHE=${go_modcache_dir}
Environment=GOMAXPROCS=1
Environment=GOMEMLIMIT=10MiB
ExecStart=/usr/bin/env bash -lc 'cd "${panel_root}" && exec go run ./cmd/sshterm/main.go'
NoNewPrivileges=true
PrivateTmp=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

  if ! $SUDO systemctl daemon-reload; then
    ui_warn "Failed to reload systemd daemon for ${service_name}."
    return 1
  fi

  $SUDO systemctl enable "$service_name" >/dev/null 2>&1 || true
  if $SUDO systemctl is-active --quiet "$service_name"; then
    $SUDO systemctl restart "$service_name" >/dev/null 2>&1 || true
  else
    $SUDO systemctl start "$service_name" >/dev/null 2>&1 || true
  fi

  if $SUDO systemctl is-active --quiet "$service_name"; then
    ui_success "${service_name}.service is active and enabled."
  else
    ui_warn "${service_name}.service was created but is not active."
    ui_warn "Check status with: systemctl status ${service_name}"
  fi

  return 0
}

install_certbot() {
  ui_info "Installing certbot..."
  if command -v certbot >/dev/null 2>&1; then
    ui_success "certbot is already installed."
    return 0
  fi

  if ! ensure_supported_package_manager; then
    ui_error "Could not detect a supported package manager to install certbot."
    ui_warn "Please install certbot manually and re-run initialization."
    return 1
  fi

  ensure_pkg_metadata >/dev/null 2>&1 || true

  if ! pkg_install_try_sets "certbot" "python3-certbot"; then
    ui_error "certbot installation failed."
    return 1
  fi

  if command -v certbot >/dev/null 2>&1; then
    ui_success "certbot installed successfully."
    return 0
  fi

  ui_error "certbot installation failed."
  return 1
}

ensure_certbot_nginx_plugin() {
  if command -v certbot >/dev/null 2>&1; then
    if certbot plugins 2>/dev/null | grep -qi "nginx"; then
      ui_success "certbot nginx plugin is already available."
      return 0
    fi
  fi

  ui_info "Installing certbot nginx plugin..."
  if ! ensure_supported_package_manager; then
    ui_warn "No supported package manager found for certbot nginx plugin."
    return 1
  fi

  ensure_pkg_metadata >/dev/null 2>&1 || true

  if ! pkg_install_try_sets \
      "python3-certbot-nginx" \
      "certbot-nginx" \
      "python-certbot-nginx" \
      "py3-certbot-nginx"; then
    ui_warn "Could not install certbot nginx plugin automatically."
    return 1
  fi

  if command -v certbot >/dev/null 2>&1 && certbot plugins 2>/dev/null | grep -qi "nginx"; then
    ui_success "certbot nginx plugin installed."
    return 0
  fi

  ui_warn "certbot nginx plugin might not be available on this system."
  return 1
}

install_nginx() {
  ui_info "Installing nginx..."
  if command -v nginx >/dev/null 2>&1; then
    ui_success "nginx is already installed."
    ensure_certbot_nginx_plugin >/dev/null 2>&1 || true
    return 0
  fi

  if ! ensure_supported_package_manager; then
    ui_error "Could not detect a supported package manager to install nginx."
    ui_warn "Please install nginx manually and re-run initialization."
    return 1
  fi

  ensure_pkg_metadata >/dev/null 2>&1 || true
  if ! pkg_install nginx; then
    ui_error "nginx installation failed."
    return 1
  fi

  ensure_certbot_nginx_plugin >/dev/null 2>&1 || true
  if command -v nginx >/dev/null 2>&1; then
    ui_success "nginx installed successfully."
    return 0
  fi

  ui_error "nginx installation failed."
  return 1
}

install_optional_nginx_brotli_module() {
  if ! ensure_supported_package_manager; then
    return 1
  fi

  ensure_pkg_metadata >/dev/null 2>&1 || true

  case "$PKG_MGR" in
    apt)
      pkg_install_try_sets \
        "libnginx-mod-brotli" \
        "nginx-mod-http-brotli"
      ;;
    dnf|yum)
      pkg_install_try_sets \
        "nginx-mod-brotli" \
        "nginx-module-brotli" \
        "nginx-mod-http-brotli"
      ;;
    zypper)
      pkg_install_try_sets \
        "nginx-module-brotli" \
        "nginx-mod-brotli"
      ;;
    pacman)
      pkg_install_try_sets \
        "nginx-mod-brotli" \
        "nginx-mainline-mod-brotli"
      ;;
    apk)
      pkg_install_try_sets \
        "nginx-mod-http-brotli" \
        "nginx-brotli"
      ;;
    *)
      return 1
      ;;
  esac
}

is_redis_installed() {
  command -v redis-server >/dev/null 2>&1 || command -v redis-cli >/dev/null 2>&1 || \
  command -v valkey-server >/dev/null 2>&1 || command -v valkey-cli >/dev/null 2>&1
}


is_mysql_installed() {
  command -v mysql >/dev/null 2>&1 || command -v mariadb >/dev/null 2>&1 || \
  command -v mysqld >/dev/null 2>&1 || command -v mariadbd >/dev/null 2>&1
}

mysql_detect_cli() {
  if cmd_exists mysql; then
    echo "mysql"
  elif cmd_exists mariadb; then
    echo "mariadb"
  else
    echo "mysql"
  fi
}

mysql_detect_service_candidates() {
  echo "mysql"
  echo "mysqld"
  echo "mariadb"
  echo "mariadb-server"
}

mysql_find_service() {
  local svc=""
  if [ "$INIT_SYSTEM" == "systemd" ]; then
    local cand
    while read -r cand; do
      if systemctl list-unit-files 2>/dev/null | grep -qE "^${cand}\.service"; then
        svc="$cand"
        break
      fi
    done < <(mysql_detect_service_candidates)
    echo "$svc"
    return 0
  fi

  echo "mariadb"
  return 0
}

mysql_init_datadir_if_needed() {
  if [ -d /var/lib/mysql/mysql ]; then
    return 0
  fi

  if cmd_exists rc-service && cmd_exists service; then
    :
  fi

  if cmd_exists mariadb-install-db; then
    $SUDO mariadb-install-db --user=mysql --datadir=/var/lib/mysql >/dev/null 2>&1 || true
    return 0
  fi

  if cmd_exists mysql_install_db; then
    $SUDO mysql_install_db --user=mysql --datadir=/var/lib/mysql >/dev/null 2>&1 || true
    return 0
  fi

  if cmd_exists rc-service; then
    $SUDO rc-service mariadb setup >/dev/null 2>&1 || true
  fi

  return 0
}

install_mysql() {
  if is_mysql_installed; then
    ui_success "MySQL/MariaDB is already installed."
    return 0
  fi

  ui_info "Installing MySQL/MariaDB..."
  if ! ensure_supported_package_manager; then
    ui_error "Could not detect a supported package manager to install MySQL/MariaDB."
    return 1
  fi

  ensure_pkg_metadata >/dev/null 2>&1 || true

  case "$PKG_MGR" in
    apt)
      pkg_install_try_sets \
        "mysql-server mysql-client" \
        "default-mysql-server default-mysql-client" \
        "mariadb-server mariadb-client" || return 1
      ;;
    dnf|yum)
      pkg_install_try_sets \
        "mariadb-server mariadb" \
        "mysql-server mysql" \
        "community-mysql-server community-mysql" || return 1
      ;;
    zypper)
      pkg_install_try_sets \
        "mariadb mariadb-client" \
        "mysql mysql-client" || return 1
      ;;
    pacman)
      pkg_install_try_sets \
        "mariadb" || return 1
      ;;
    apk)
      pkg_install_try_sets \
        "mariadb mariadb-client" || return 1
      ;;
    *)
      ui_error "Unsupported package manager for MySQL/MariaDB install."
      return 1
      ;;
  esac

  mysql_init_datadir_if_needed

  local svc
  svc="$(mysql_find_service)"
  if [ -n "$svc" ]; then
    service_enable "$svc"
    service_start "$svc"
  else
    service_enable mariadb
    service_start mariadb
    service_enable mysql
    service_start mysql
  fi

  if is_mysql_installed; then
    ui_success "MySQL/MariaDB installed successfully."
    return 0
  fi

  ui_error "MySQL/MariaDB installation failed."
  return 1
}

secure_mariadb_binding() {
  ui_info "Securing MariaDB/MySQL to listen on localhost only..."

  local conf_paths=(
    "/etc/mysql/mariadb.conf.d/50-server.cnf"
    "/etc/mysql/mysql.conf.d/mysqld.cnf"
    "/etc/my.cnf.d/server.cnf"
    "/etc/my.cnf.d/mariadb-server.cnf"
    "/etc/my.cnf"
  )

  local conf_found=""
  for cpath in "${conf_paths[@]}"; do
    if [ -f "$cpath" ]; then
      conf_found="$cpath"
      break
    fi
  done

  if [ -z "$conf_found" ]; then
    # Create a drop-in config if no existing config found
    if [ -d "/etc/mysql/mariadb.conf.d" ]; then
      conf_found="/etc/mysql/mariadb.conf.d/99-adpanel-security.cnf"
    elif [ -d "/etc/mysql/mysql.conf.d" ]; then
      conf_found="/etc/mysql/mysql.conf.d/99-adpanel-security.cnf"
    elif [ -d "/etc/my.cnf.d" ]; then
      conf_found="/etc/my.cnf.d/99-adpanel-security.cnf"
    else
      ui_warn "Could not find MariaDB/MySQL config directory. Skipping bind-address configuration."
      return 1
    fi
    $SUDO tee "$conf_found" >/dev/null <<'BINDEOF'
[mysqld]
bind-address = 127.0.0.1
skip-networking = 0
BINDEOF
    ui_success "Created MariaDB security config at ${conf_found}"
  else
    # Update existing config
    if grep -q "^bind-address" "$conf_found" 2>/dev/null; then
      $SUDO sed -i 's/^bind-address\s*=.*/bind-address = 127.0.0.1/' "$conf_found"
    elif grep -q "^#.*bind-address" "$conf_found" 2>/dev/null; then
      $SUDO sed -i 's/^#.*bind-address\s*=.*/bind-address = 127.0.0.1/' "$conf_found"
    elif grep -q "^\[mysqld\]" "$conf_found" 2>/dev/null; then
      $SUDO sed -i '/^\[mysqld\]/a bind-address = 127.0.0.1' "$conf_found"
    else
      echo -e "\n[mysqld]\nbind-address = 127.0.0.1" | $SUDO tee -a "$conf_found" >/dev/null
    fi
    ui_success "MariaDB bind-address set to 127.0.0.1 in ${conf_found}"
  fi

  # Restart MariaDB/MySQL to apply
  local svc
  svc="$(mysql_find_service)"
  if [ -n "$svc" ]; then
    service_restart "$svc"
  else
    service_restart mariadb
    service_restart mysql
  fi

  ui_success "MariaDB/MySQL is now listening on localhost only."
  return 0
}

harden_firewall() {
  ui_info "Hardening firewall rules..."

  local backend=""

  if cmd_exists ufw; then
    backend="ufw"
  elif cmd_exists firewall-cmd; then
    backend="firewalld"
  else
    ensure_pkg_metadata >/dev/null 2>&1 || true
    case "$PKG_MGR" in
      apt|pacman)
        if pkg_install ufw >/dev/null 2>&1; then
          backend="ufw"
        fi
        ;;
      dnf|yum|zypper)
        if pkg_install firewalld >/dev/null 2>&1; then
          backend="firewalld"
        fi
        ;;
    esac
  fi

  case "$backend" in
    ufw)
      $SUDO ufw deny 445/tcp >/dev/null 2>&1 || true
      $SUDO ufw deny 445/udp >/dev/null 2>&1 || true
      ui_success "Port 445 (SMB) blocked."

      $SUDO ufw deny 3306/tcp >/dev/null 2>&1 || true
      ui_success "Port 3306 (MySQL) blocked from external access."

      if ! $SUDO ufw status | grep -q "Status: active"; then
        $SUDO ufw --force enable >/dev/null 2>&1 || true
      fi
      $SUDO ufw reload >/dev/null 2>&1 || true
      ui_success "Firewall hardened with ufw."
      return 0
      ;;
    firewalld)
      service_enable firewalld
      service_start firewalld
      $SUDO firewall-cmd --permanent --add-rich-rule='rule family="ipv4" port port="445" protocol="tcp" reject' >/dev/null 2>&1 || true
      $SUDO firewall-cmd --permanent --add-rich-rule='rule family="ipv4" port port="445" protocol="udp" reject' >/dev/null 2>&1 || true
      ui_success "Port 445 (SMB) blocked."

      $SUDO firewall-cmd --permanent --add-rich-rule='rule family="ipv4" port port="3306" protocol="tcp" reject' >/dev/null 2>&1 || true
      ui_success "Port 3306 (MySQL) blocked from external access."

      $SUDO firewall-cmd --reload >/dev/null 2>&1 || true
      ui_success "Firewall hardened with firewalld."
      return 0
      ;;
    *)
      if command -v iptables >/dev/null 2>&1; then
        $SUDO iptables -C INPUT -p tcp --dport 445 -j DROP >/dev/null 2>&1 || \
          $SUDO iptables -A INPUT -p tcp --dport 445 -j DROP >/dev/null 2>&1 || true
        $SUDO iptables -C INPUT -p udp --dport 445 -j DROP >/dev/null 2>&1 || \
          $SUDO iptables -A INPUT -p udp --dport 445 -j DROP >/dev/null 2>&1 || true
        ui_success "Port 445 (SMB) blocked."

        $SUDO iptables -C INPUT -p tcp --dport 3306 -j DROP >/dev/null 2>&1 || \
          $SUDO iptables -A INPUT -p tcp --dport 3306 -j DROP >/dev/null 2>&1 || true
        ui_success "Port 3306 (MySQL) blocked from external access."

        if command -v systemctl >/dev/null 2>&1; then
          if systemctl list-unit-files | grep -q "^netfilter-persistent.service"; then
            $SUDO systemctl reload netfilter-persistent >/dev/null 2>&1 || true
          elif systemctl list-unit-files | grep -q "^iptables.service"; then
            $SUDO systemctl reload iptables >/dev/null 2>&1 || true
          fi
        fi
        if command -v service >/dev/null 2>&1; then
          $SUDO service iptables reload >/dev/null 2>&1 || true
        fi
        ui_success "Firewall hardened with iptables."
        return 0
      fi
      ;;
  esac

  ui_warn "No supported firewall manager was found or installed. Skipping firewall hardening."
  return 1
}

mask_service_versions() {
  ui_info "Masking service version fingerprints..."

  # --- Nginx: hide version in Server header globally ---
  if [ -f /etc/nginx/nginx.conf ]; then
    if ! grep -q "^\s*server_tokens\s\+off" /etc/nginx/nginx.conf 2>/dev/null; then
      if grep -q "^\s*http\s*{" /etc/nginx/nginx.conf 2>/dev/null; then
        $SUDO sed -i '/^\s*http\s*{/a\    server_tokens off;' /etc/nginx/nginx.conf
      else
        # Fallback: append in http block area
        $SUDO sed -i '/^\s*include.*sites-enabled/i\    server_tokens off;' /etc/nginx/nginx.conf 2>/dev/null || true
      fi
      ui_success "Nginx server_tokens off added."
    else
      ui_info "Nginx server_tokens already disabled."
    fi
  fi

  # --- MariaDB/MySQL: remove version comment ---
  local mysql_conf_dirs=(
    "/etc/mysql/mariadb.conf.d"
    "/etc/mysql/mysql.conf.d"
    "/etc/my.cnf.d"
  )
  local mysql_hardened=false
  for confdir in "${mysql_conf_dirs[@]}"; do
    if [ -d "$confdir" ]; then
      local target="${confdir}/99-adpanel-version-mask.cnf"
      $SUDO tee "$target" >/dev/null <<'MYSQLEOF'
[mysqld]
performance_schema = OFF
MYSQLEOF
      ui_success "MariaDB/MySQL hardened in ${target}"
      mysql_hardened=true
      break
    fi
  done
  if [ "$mysql_hardened" = false ] && [ -f /etc/my.cnf ]; then
    if ! grep -q "performance_schema" /etc/my.cnf 2>/dev/null; then
      if grep -q "^\[mysqld\]" /etc/my.cnf 2>/dev/null; then
        $SUDO sed -i '/^\[mysqld\]/a performance_schema = OFF' /etc/my.cnf
      else
        echo -e "\n[mysqld]\nperformance_schema = OFF" | $SUDO tee -a /etc/my.cnf >/dev/null
      fi
      ui_success "MariaDB/MySQL hardened in /etc/my.cnf"
    fi
  fi

  # --- OpenSSH: minimize banner exposure ---
  local sshd_conf="/etc/ssh/sshd_config"
  if [ -f "$sshd_conf" ]; then
    # Disable Debian/Ubuntu-specific banner that leaks OS info
    if ! grep -q "^DebianBanner\s\+no" "$sshd_conf" 2>/dev/null; then
      if grep -q "^#\?DebianBanner" "$sshd_conf" 2>/dev/null; then
        $SUDO sed -i 's/^#\?DebianBanner.*/DebianBanner no/' "$sshd_conf"
      else
        echo "DebianBanner no" | $SUDO tee -a "$sshd_conf" >/dev/null
      fi
    fi
    # Disable custom banner file
    if grep -q "^Banner\s" "$sshd_conf" 2>/dev/null; then
      $SUDO sed -i 's/^Banner\s.*/Banner none/' "$sshd_conf"
    elif ! grep -q "^Banner\s\+none" "$sshd_conf" 2>/dev/null; then
      echo "Banner none" | $SUDO tee -a "$sshd_conf" >/dev/null
    fi
    # Restart sshd to apply
    if command -v systemctl >/dev/null 2>&1; then
      $SUDO systemctl restart sshd 2>/dev/null || $SUDO systemctl restart ssh 2>/dev/null || true
    else
      service_restart sshd 2>/dev/null || service_restart ssh 2>/dev/null || true
    fi
    ui_success "OpenSSH banner exposure minimized."
  fi

  # --- PHP: hide version from HTTP headers (for phpMyAdmin) ---
  local php_ini_paths=(
    "/etc/php/8.3/fpm/php.ini"
    "/etc/php/8.2/fpm/php.ini"
    "/etc/php/8.1/fpm/php.ini"
    "/etc/php/8.0/fpm/php.ini"
    "/etc/php/7.4/fpm/php.ini"
  )
  for pini in "${php_ini_paths[@]}"; do
    if [ -f "$pini" ]; then
      if grep -q "^expose_php\s*=\s*On" "$pini" 2>/dev/null; then
        $SUDO sed -i 's/^expose_php\s*=\s*On/expose_php = Off/' "$pini"
        ui_success "PHP expose_php disabled in ${pini}"
      elif ! grep -q "^expose_php" "$pini" 2>/dev/null; then
        echo "expose_php = Off" | $SUDO tee -a "$pini" >/dev/null
        ui_success "PHP expose_php disabled in ${pini}"
      fi
    fi
  done
  # Restart PHP-FPM if running
  local php_fpm_svc=""
  if command -v systemctl >/dev/null 2>&1; then
    php_fpm_svc=$(systemctl list-units --type=service --state=running --no-legend 2>/dev/null | grep -oP 'php[\d.]+-fpm\.service' | sort -rV | head -1)
    if [ -n "$php_fpm_svc" ]; then
      $SUDO systemctl restart "$php_fpm_svc" 2>/dev/null || true
    fi
  fi

  # --- Apache: hide version (for pgAdmin4) ---
  if [ -f /etc/apache2/apache2.conf ]; then
    local apache_hardened=false
    # ServerTokens Prod
    if ! grep -q "^ServerTokens\s\+Prod" /etc/apache2/apache2.conf 2>/dev/null; then
      if grep -q "^ServerTokens" /etc/apache2/apache2.conf 2>/dev/null; then
        $SUDO sed -i 's/^ServerTokens.*/ServerTokens Prod/' /etc/apache2/apache2.conf
      else
        echo "ServerTokens Prod" | $SUDO tee -a /etc/apache2/apache2.conf >/dev/null
      fi
      apache_hardened=true
    fi
    # ServerSignature Off
    if ! grep -q "^ServerSignature\s\+Off" /etc/apache2/apache2.conf 2>/dev/null; then
      if grep -q "^ServerSignature" /etc/apache2/apache2.conf 2>/dev/null; then
        $SUDO sed -i 's/^ServerSignature.*/ServerSignature Off/' /etc/apache2/apache2.conf
      else
        echo "ServerSignature Off" | $SUDO tee -a /etc/apache2/apache2.conf >/dev/null
      fi
      apache_hardened=true
    fi
    # Ensure headers module is enabled for Header directives
    $SUDO a2enmod headers 2>/dev/null || true
    if [ "$apache_hardened" = true ]; then
      $SUDO systemctl restart apache2 2>/dev/null || true
      ui_success "Apache version fingerprint masked."
    else
      ui_info "Apache version already masked."
    fi
  fi

  # --- PostgreSQL: restrict version exposure ---
  local pg_conf_found=false
  for pgconf in $(find /etc/postgresql -name postgresql.conf 2>/dev/null); do
    if [ -f "$pgconf" ]; then
      # Disable server version in error messages to unauthenticated clients
      if ! grep -q "^password_encryption\s*=\s*scram-sha-256" "$pgconf" 2>/dev/null; then
        if grep -q "^#\?password_encryption" "$pgconf" 2>/dev/null; then
          $SUDO sed -i "s/^#\?password_encryption.*/password_encryption = scram-sha-256/" "$pgconf"
        else
          echo "password_encryption = scram-sha-256" | $SUDO tee -a "$pgconf" >/dev/null
        fi
      fi
      if ! grep -q "^log_hostname\s*=\s*off" "$pgconf" 2>/dev/null; then
        if grep -q "^#\?log_hostname" "$pgconf" 2>/dev/null; then
          $SUDO sed -i "s/^#\?log_hostname.*/log_hostname = off/" "$pgconf"
        else
          echo "log_hostname = off" | $SUDO tee -a "$pgconf" >/dev/null
        fi
      fi
      pg_conf_found=true
    fi
  done
  if [ "$pg_conf_found" = true ]; then
    $SUDO systemctl reload postgresql 2>/dev/null || true
    ui_success "PostgreSQL hardened."
  fi

  # --- MongoDB: reduce fingerprint exposure ---
  if [ -f /etc/mongod.conf ]; then
    # Ensure quiet mode is enabled to reduce protocol-level information leakage
    if ! grep -q "^\s*quiet:\s*true" /etc/mongod.conf 2>/dev/null; then
      if grep -q "^systemLog:" /etc/mongod.conf 2>/dev/null; then
        $SUDO sed -i '/^systemLog:/a\  quiet: true' /etc/mongod.conf
      fi
      $SUDO systemctl restart mongod 2>/dev/null || true
      ui_success "MongoDB fingerprint exposure reduced."
    else
      ui_info "MongoDB quiet mode already enabled."
    fi
  fi

  ui_success "Service version fingerprints masked."
  return 0
}

mysql_sql_escape() {
  printf "%s" "$1" | sed "s/\\\\/\\\\\\\\/g; s/'/''/g"
}

mysql_exec_as_root() {
  local sql="$1"
  local root_pass="${2:-}"
  local cli
  cli="$(mysql_detect_cli)"

  if ! cmd_exists "$cli"; then
    return 1
  fi

  if $SUDO "$cli" -e "$sql" >/dev/null 2>&1; then
    return 0
  fi

  if [ -n "$root_pass" ]; then
    if "$cli" -uroot -p"$root_pass" -h 127.0.0.1 -e "$sql" >/dev/null 2>&1; then
      return 0
    fi
  fi

  return 1
}

prompt_mysql_password() {
  local p=""
  while true; do
    read -r -s -p "$(ui_prompt "Enter MySQL password for the new user: ")" p
    printf '\n' >&2
    if [ -z "$p" ]; then
      ui_error "MySQL password cannot be empty." >&2
      continue
    fi
    if printf '%s' "$p" | grep -qE '[[:space:]]'; then
      ui_error "MySQL password cannot contain spaces or newlines." >&2
      continue
    fi
    printf '%s' "$p"
    return 0
  done
}

setup_mysql() {
  MYSQL_HOST="127.0.0.1"
  MYSQL_PORT="3306"
  MYSQL_USER="adpanel"
  MYSQL_PASSWORD=""
  MYSQL_DATABASE="adpanel"
  MYSQL_URL=""

  ui_section "=== MySQL/MariaDB configuration (users/storage) ==="
  read -p "$(ui_prompt "Do you want to configure MySQL/MariaDB now? (yes/no, default yes): ")" USE_MYSQL
  USE_MYSQL=$(echo "$USE_MYSQL" | tr '[:upper:]' '[:lower:]' | xargs)
  if [ -n "$USE_MYSQL" ] && [ "$USE_MYSQL" != "yes" ] && [ "$USE_MYSQL" != "y" ]; then
    ui_warn "Skipping MySQL/MariaDB setup; leaving MYSQL_* empty in .env."
    MYSQL_HOST=""
    MYSQL_PORT=""
    MYSQL_USER=""
    MYSQL_PASSWORD=""
    MYSQL_DATABASE=""
    MYSQL_URL=""
    return 0
  fi

  if ! install_mysql; then
    ui_warn "MySQL/MariaDB install failed; leaving MYSQL_* empty in .env."
    MYSQL_HOST=""
    MYSQL_PORT=""
    MYSQL_USER=""
    MYSQL_PASSWORD=""
    MYSQL_DATABASE=""
    MYSQL_URL=""
    return 1
  fi

  read -p "$(ui_prompt "MySQL host (default 127.0.0.1): ")" MYSQL_HOST_IN
  MYSQL_HOST_IN=$(echo "$MYSQL_HOST_IN" | xargs)
  if [ -n "$MYSQL_HOST_IN" ]; then
    MYSQL_HOST="$MYSQL_HOST_IN"
  fi

  read -p "$(ui_prompt "MySQL port (default 3306): ")" MYSQL_PORT_IN
  MYSQL_PORT_IN=$(echo "$MYSQL_PORT_IN" | xargs)
  if [ -n "$MYSQL_PORT_IN" ]; then
    MYSQL_PORT="$MYSQL_PORT_IN"
  fi

  read -p "$(ui_prompt "MySQL database name (default adpanel): ")" MYSQL_DB_IN
  MYSQL_DB_IN=$(echo "$MYSQL_DB_IN" | xargs)
  if [ -n "$MYSQL_DB_IN" ]; then
    MYSQL_DATABASE="$MYSQL_DB_IN"
  fi

  read -p "$(ui_prompt "MySQL username (default adpanel): ")" MYSQL_USER_IN
  MYSQL_USER_IN=$(echo "$MYSQL_USER_IN" | xargs)
  if [ -n "$MYSQL_USER_IN" ]; then
    MYSQL_USER="$MYSQL_USER_IN"
  fi

  MYSQL_PASSWORD="$(prompt_mysql_password)"

  ui_info "MySQL settings chosen:"
  ui_kv "  USERNAME:" "${MYSQL_USER}"
  ui_kv "  HOST:" "${MYSQL_HOST}"
  ui_kv "  PORT:" "${MYSQL_PORT}"
  ui_kv "  PASSWORD:" "${MYSQL_PASSWORD}"
  ui_kv "  DATABASE:" "${MYSQL_DATABASE}"

  local esc_user esc_pass esc_db
  esc_user="$(mysql_sql_escape "$MYSQL_USER")"
  esc_pass="$(mysql_sql_escape "$MYSQL_PASSWORD")"
  esc_db="$(printf "%s" "$MYSQL_DATABASE" | sed "s/\`//g")"

  if ! printf "%s" "$esc_user" | grep -qE '^[A-Za-z0-9_@.-]+$'; then
    ui_warn "MySQL username contains unsupported characters; skipping automatic user creation."
  elif ! printf "%s" "$esc_db" | grep -qE '^[A-Za-z0-9_]+$'; then
    ui_warn "MySQL database name contains unsupported characters; skipping automatic DB creation."
  else
    local sql
    sql=$(cat <<EOF
CREATE DATABASE IF NOT EXISTS \`$esc_db\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS '$esc_user'@'localhost' IDENTIFIED BY '$esc_pass';
CREATE USER IF NOT EXISTS '$esc_user'@'127.0.0.1' IDENTIFIED BY '$esc_pass';

-- Schema permissions kept local-only; REFERENCES is needed for foreign keys.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES
ON \`$esc_db\`.*
TO '$esc_user'@'localhost';

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES
ON \`$esc_db\`.*
TO '$esc_user'@'127.0.0.1';

FLUSH PRIVILEGES;
EOF
)
    if ! mysql_exec_as_root "$sql"; then
      ui_warn "Could not configure MySQL via root socket auth."
      read -s -p "$(ui_prompt "If your MySQL root user has a password, enter it now (or press Enter to skip DB/user creation): ")" MYSQL_ROOT_PASS
      echo ""
      if [ -n "$MYSQL_ROOT_PASS" ]; then
        if ! mysql_exec_as_root "$sql" "$MYSQL_ROOT_PASS"; then
          ui_warn "Still could not create DB/user automatically. You may need to create them manually."
        else
          ui_success "MySQL database/user configured successfully."
        fi
      else
        ui_warn "Skipping automatic DB/user creation."
      fi
    else
      ui_success "MySQL database/user configured successfully."
    fi
  fi

  local encoded_pass
  encoded_pass=$(node -e "console.log(encodeURIComponent(process.argv[1] || ''))" "$MYSQL_PASSWORD")
  MYSQL_URL="mysql://${MYSQL_USER}:${encoded_pass}@127.0.0.1:${MYSQL_PORT}/${MYSQL_DATABASE}"
  return 0
}

wait_for_mysql_table() {
  local db="$1"
  local table="$2"
  local host="$3"
  local port="$4"
  local user="$5"
  local pass="$6"
  local retries=40
  local delay=2
  local max_wait=$((retries * delay))

  if [ -z "$db" ] || [ -z "$table" ] || [ -z "$host" ] || [ -z "$port" ] || [ -z "$user" ]; then
    return 1
  fi

  local cli
  cli="$(mysql_detect_cli)"
  if ! cmd_exists "$cli"; then
    return 1
  fi

  ui_info "Waiting for MySQL table ${db}.${table} to be created (up to ${max_wait}s)..."
  for i in $(seq 1 "$retries"); do
    local out=""
    if [ -n "$pass" ]; then
      out=$(MYSQL_PWD="$pass" "$cli" -h "$host" -P "$port" -u "$user" -Nse \
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${db}' AND table_name='${table}';" 2>/dev/null || true)
    else
      out=$("$cli" -h "$host" -P "$port" -u "$user" -Nse \
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${db}' AND table_name='${table}';" 2>/dev/null || true)
    fi

    if [ "$out" == "1" ] || [ "$out" -gt 0 ] 2>/dev/null; then
      ui_success "MySQL table ${db}.${table} is ready."
      return 0
    fi

    printf "."
    sleep "$delay"
  done

  echo ""
  return 1
}

bootstrap_db_and_create_admin() {
  local admin_email="$1"
  local admin_hash="$2"
  local admin_secret="$3"
  local admin_avatar_url=""

  admin_avatar_url="$(pick_default_avatar_url admin)"

  if [ -z "$MYSQL_URL" ] || [ -z "$MYSQL_DATABASE" ] || [ -z "$MYSQL_USER" ] || [ -z "$MYSQL_PASSWORD" ]; then
    return 0
  fi

  if [ -z "$admin_email" ] || [ -z "$admin_hash" ]; then
    return 0
  fi

  local script_path="$CREATE_USER_SCRIPT"
  if [ -z "$script_path" ]; then
    ui_warn "create-user.js not found (expected ${SCRIPT_DIR}/scripts/create-user.js); skipping automatic admin DB seeding."
    return 0
  fi

  ui_info "Bootstrapping database schema via: npm start"
  local boot_log="/tmp/adpanel-npm-start-bootstrap.log"
  local pid=""

  if cmd_exists timeout; then
    timeout 90s npm start >"$boot_log" 2>&1 &
    pid=$!
  else
    npm start >"$boot_log" 2>&1 &
    pid=$!
  fi

  if ! wait_for_mysql_table "$MYSQL_DATABASE" "users" "127.0.0.1" "$MYSQL_PORT" "$MYSQL_USER" "$MYSQL_PASSWORD"; then
    ui_warn "users table did not appear. Check bootstrap log: ${boot_log}"
  fi

  if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    sleep 2
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi

  ui_info "Seeding admin user via ${script_path}..."

  if [ -n "$admin_secret" ]; then
    $SUDO node "$script_path" --email "$admin_email" --password-hash "$admin_hash" --admin --secret "$admin_secret" --avatar-url "$admin_avatar_url"
  else
    $SUDO node "$script_path" --email "$admin_email" --password-hash "$admin_hash" --admin --avatar-url "$admin_avatar_url"
  fi

  return $?
}


install_redis() {
  if is_redis_installed; then
    ui_success "Redis (or compatible) is already installed."
    detect_redis_commands
    install_mysql >/dev/null 2>&1 || true
    return 0
  fi

  ui_info "Installing Redis (This may take a few minutes)..."
  if ! ensure_supported_package_manager; then
    ui_error "Could not detect a supported package manager to install Redis."
    return 1
  fi

  ensure_pkg_metadata >/dev/null 2>&1 || true

  case "$PKG_MGR" in
    apt)
      pkg_install_try_sets \
        "redis-server redis-tools" \
        "redis-server" \
        "valkey-redis-compat" \
        "valkey valkey-tools" \
        "valkey-server valkey-tools" || return 1
      ;;
    dnf|yum|zypper)
      pkg_install_try_sets \
        "redis" \
        "redis-server redis" \
        "valkey" \
        "valkey-redis-compat" || return 1
      ;;
    apk|pacman)
      pkg_install_try_sets \
        "redis" \
        "valkey" \
        "valkey-redis-compat" || return 1
      ;;
    *)
      ui_error "Unsupported package manager for Redis install."
      return 1
      ;;
  esac

  install_mysql >/dev/null 2>&1 || true

  detect_redis_commands

  if is_redis_installed; then
    ui_success "Redis installed successfully."
    return 0
  fi

  ui_error "Redis installation failed."
  return 1
}

detect_redis_conf() {
  local candidates=(
    "/etc/redis/redis.conf"
    "/etc/redis.conf"
    "/usr/local/etc/redis/redis.conf"
    "/etc/valkey/valkey.conf"
    "/etc/valkey.conf"
  )
  for f in "${candidates[@]}"; do
    if [ -f "$f" ]; then
      echo "$f"
      return 0
    fi
  done
  echo ""
  return 1
}

calculate_redis_maxmemory() {
  local mem_kb=""
  if [ -r /proc/meminfo ]; then
    mem_kb=$(awk '/MemTotal:/ {print $2}' /proc/meminfo | head -n1)
  fi

  if [ -n "$mem_kb" ]; then
    local mem_mb=$((mem_kb / 1024))
    local mm_mb=$((mem_mb / 4))
    if [ "$mm_mb" -lt 64 ]; then mm_mb=64; fi
    if [ "$mm_mb" -gt 2048 ]; then mm_mb=2048; fi
    echo "${mm_mb}mb"
    return 0
  fi

  echo "256mb"
  return 0
}

redis_bind_line() {
  if [ -r /proc/sys/net/ipv6/conf/all/disable_ipv6 ]; then
    local disabled
    disabled=$(cat /proc/sys/net/ipv6/conf/all/disable_ipv6 2>/dev/null || echo "1")
    if [ "$disabled" == "0" ]; then
      echo "bind 127.0.0.1 ::1"
      return 0
    fi
  fi
  echo "bind 127.0.0.1"
  return 0
}

redis_supervised_value() {
  if [ "$INIT_SYSTEM" == "systemd" ]; then
    echo "systemd"
  else
    echo "no"
  fi
}

redis_can_enable_activedefrag() {
  detect_redis_commands
  if ! cmd_exists "$REDIS_SERVER_CMD"; then
    return 1
  fi
  if ! cmd_exists timeout; then
    return 1
  fi

  local tmp
  tmp="$(mktemp /tmp/adpanel-redis-probe.XXXXXX.conf 2>/dev/null || echo "")"
  if [ -z "$tmp" ]; then
    return 1
  fi

  cat > "$tmp" <<'EOF'
port 0
bind 127.0.0.1
protected-mode yes
daemonize no
save ""
appendonly no
activedefrag yes
EOF

  local out rc
  out="$(timeout 1 "$REDIS_SERVER_CMD" "$tmp" 2>&1 || true)"
  rc=$?

  rm -f "$tmp" >/dev/null 2>&1 || true

  if echo "$out" | grep -qi "Active defragmentation cannot be enabled"; then
    return 1
  fi
  if echo "$out" | grep -qi "FATAL CONFIG FILE ERROR"; then
    return 1
  fi

  if [ "$rc" -eq 124 ] || [ "$rc" -eq 0 ]; then
    return 0
  fi

  return 1
}

configure_redis_auth() {
  local conf="$1"
  local password="$2"
  if [ -z "$conf" ] || [ -z "$password" ]; then
    return 1
  fi

  $SUDO sed -i '/^[[:space:]]*# === ADPANEL REDIS SETTINGS START ===$/,/^[[:space:]]*# === ADPANEL REDIS SETTINGS END ===$/d' "$conf" 2>/dev/null || true

  $SUDO sed -i '/^[[:space:]]*requirepass[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*bind[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*protected-mode[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*supervised[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*save[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*appendonly[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*maxmemory[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*maxmemory-policy[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*lazyfree-lazy-eviction[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*lazyfree-lazy-expire[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*lazyfree-lazy-server-del[[:space:]]/d' "$conf" 2>/dev/null || true
  $SUDO sed -i '/^[[:space:]]*activedefrag[[:space:]]/d' "$conf" 2>/dev/null || true

  local maxmemory
  maxmemory=$(calculate_redis_maxmemory)

  local bindline
  bindline=$(redis_bind_line)

  local supervised_val
  supervised_val=$(redis_supervised_value)

  local enable_activedefrag="no"
  if redis_can_enable_activedefrag; then
    enable_activedefrag="yes"
  fi

  cat <<EOF | $SUDO tee -a "$conf" >/dev/null
${bindline}
protected-mode yes
supervised ${supervised_val}

requirepass ${password}

save ""
appendonly no

maxmemory ${maxmemory}
maxmemory-policy noeviction

lazyfree-lazy-eviction yes
lazyfree-lazy-expire yes
lazyfree-lazy-server-del yes
EOF

  if [ "$enable_activedefrag" == "yes" ]; then
    echo "activedefrag yes" | $SUDO tee -a "$conf" >/dev/null
  else
    cat <<'EOF' | $SUDO tee -a "$conf" >/dev/null
EOF
  fi

  cat <<EOF | $SUDO tee -a "$conf" >/dev/null
EOF

  local acl_file
  acl_file=$(grep -E '^[[:space:]]*aclfile[[:space:]]+' "$conf" | awk '{print $2}' | tail -n 1)
  if [ -z "$acl_file" ] && [ -f "/etc/redis/users.acl" ]; then
    acl_file="/etc/redis/users.acl"
  fi

  if [ -n "$acl_file" ]; then
    $SUDO mkdir -p "$(dirname "$acl_file")" >/dev/null 2>&1 || true
    $SUDO sed -i "/^[[:space:]]*user[[:space:]]\\+default[[:space:]]\\+/d" "$acl_file" 2>/dev/null || true
    echo "user default on >${password} ~* +@all" | $SUDO tee -a "$acl_file" >/dev/null
  fi
}

ensure_overcommit_memory() {
  cmd_exists sysctl || return 0
  $SUDO sysctl -w vm.overcommit_memory=1 >/dev/null 2>&1 || true
  if [ -f /etc/sysctl.conf ]; then
    if grep -qE '^[[:space:]]*vm\.overcommit_memory' /etc/sysctl.conf; then
      $SUDO sed -i "s|^[[:space:]]*vm\.overcommit_memory[[:space:]]*=.*|vm.overcommit_memory = 1|g" /etc/sysctl.conf
    else
      echo "vm.overcommit_memory = 1" | $SUDO tee -a /etc/sysctl.conf >/dev/null
    fi
  fi
}

restart_redis_service() {
  local svc=""

  if [ "$INIT_SYSTEM" == "systemd" ]; then
    $SUDO systemctl daemon-reload >/dev/null 2>&1 || true
    if systemctl list-unit-files 2>/dev/null | grep -q "^redis-server.service"; then
      svc="redis-server"
    elif systemctl list-unit-files 2>/dev/null | grep -q "^redis.service"; then
      svc="redis"
    elif systemctl list-unit-files 2>/dev/null | grep -q "^valkey.service"; then
      svc="valkey"
    elif systemctl list-unit-files 2>/dev/null | grep -q "^valkey-server.service"; then
      svc="valkey-server"
    fi

    if [ -n "$svc" ]; then
      $SUDO systemctl reset-failed "$svc" >/dev/null 2>&1 || true
      service_enable "$svc"
      service_restart "$svc"
      return 0
    fi
  fi

  for svc in redis-server redis valkey valkey-server; do
    service_restart "$svc"
  done

  return 0
}

configure_redis_firewall() {
  ui_info "Redis stays bound to localhost only; skipping automatic firewall changes for port 6379."
}

wait_for_redis() {
  local host="$1"
  local port="$2"
  local pass="$3"
  local retries=10
  local delay=2
  local max_wait=$((retries * delay))

  detect_redis_commands
  if ! command -v "$REDIS_CLI_CMD" >/dev/null 2>&1; then
    return 1
  fi

  ui_info "Waiting for Redis to accept connections (up to ${max_wait}s)..."

  for i in $(seq 1 "$retries"); do
    local out
    if command -v timeout >/dev/null 2>&1; then
      out=$(timeout 2 "$REDIS_CLI_CMD" -h "$host" -p "$port" -a "$pass" --no-auth-warning ping 2>/dev/null || true)
    else
      out=$("$REDIS_CLI_CMD" -h "$host" -p "$port" -a "$pass" --no-auth-warning ping 2>/dev/null || true)
    fi
    if [ "$out" == "PONG" ]; then
      ui_success "Redis is ready."
      return 0
    fi
    printf "."
    sleep "$delay"
  done

  echo ""
  return 1
}

prompt_redis_password() {
  local pass1=""
  while true; do
    read -r -s -p "$(ui_prompt "Enter Redis password: ")" pass1
    printf '\n' >&2

    if [ -z "$pass1" ]; then
      ui_error "Redis password cannot be empty." >&2
      continue
    fi
    if printf '%s' "$pass1" | grep -qE '[[:space:]]'; then
      ui_error "Redis password cannot contain spaces or newlines." >&2
      continue
    fi

    printf '%s' "$pass1"
    return 0
  done
}

setup_redis() {
  REDIS_HOST="127.0.0.1"
  REDIS_PORT="6379"
  REDIS_USER="default"
  REDIS_PASSWORD=""
  REDIS_URL=""
  SESSION_STORE="file"

  read -p "$(ui_prompt "Do you want to configure Redis (with password) for session storage? (yes/no, default yes): ")" USE_REDIS
  USE_REDIS=$(echo "$USE_REDIS" | tr '[:upper:]' '[:lower:]' | xargs)
  if [ -z "$USE_REDIS" ] || [ "$USE_REDIS" == "yes" ] || [ "$USE_REDIS" == "y" ]; then
    :
  else
    ui_warn "Skipping Redis setup; using file sessions."
    SESSION_STORE="file"
    REDIS_PASSWORD=""
    REDIS_URL=""
    return 0
  fi

  if ! install_redis; then
    ui_warn "Redis installation failed; falling back to file sessions."
    SESSION_STORE="file"
    REDIS_PASSWORD=""
    REDIS_URL=""
    return 1
  fi

  if ! is_redis_installed; then
    ui_warn "Redis not detected after install; falling back to file sessions."
    SESSION_STORE="file"
    REDIS_PASSWORD=""
    REDIS_URL=""
    return 1
  fi

  detect_redis_commands

  REDIS_PASSWORD="$(prompt_redis_password)"
  local conf
  conf=$(detect_redis_conf)
  if [ -z "$conf" ]; then
    ui_warn "Redis config not found; skipping config changes."
  else
    configure_redis_auth "$conf" "$REDIS_PASSWORD"
  fi

  ensure_overcommit_memory
  ui_info "Restarting Redis..."
  restart_redis_service
  configure_redis_firewall

  local encoded_pass
  encoded_pass=$(node -e "console.log(encodeURIComponent(process.argv[1] || ''))" "$REDIS_PASSWORD")
  REDIS_URL="redis://${REDIS_USER}:${encoded_pass}@${REDIS_HOST}:${REDIS_PORT}"
  SESSION_STORE="redis"

  if ! wait_for_redis "$REDIS_HOST" "$REDIS_PORT" "$REDIS_PASSWORD"; then
    ui_warn "Redis did not respond to PING; falling back to file sessions."
    SESSION_STORE="file"
    REDIS_URL=""
    REDIS_PASSWORD=""
    return 1
  fi
}

detect_nginx_layout() {
  if [ -d /etc/nginx/sites-available ] && [ -d /etc/nginx/sites-enabled ]; then
    echo "debian"
  else
    echo "confd"
  fi
}

detect_nginx_user() {
  local nginx_user=""

  if [ -f /etc/nginx/nginx.conf ]; then
    nginx_user=$(grep -E "^\s*user\s+" /etc/nginx/nginx.conf 2>/dev/null | head -1 | awk '{print $2}' | tr -d ';')
  fi

  if [ -z "$nginx_user" ]; then
    if id "www-data" >/dev/null 2>&1; then
      nginx_user="www-data"
    elif id "nginx" >/dev/null 2>&1; then
      nginx_user="nginx"
    elif id "http" >/dev/null 2>&1; then
      nginx_user="http"
    elif id "nobody" >/dev/null 2>&1; then
      nginx_user="nobody"
    fi
  fi

  echo "$nginx_user"
}

fix_nginx_static_permissions() {
  local panel_root="$1"
  local static_root="${panel_root}/public"
  local nginx_user
  nginx_user="$(detect_nginx_user)"

  ui_info "Setting permissions for nginx to serve static files..."

  if [ -z "$nginx_user" ]; then
    ui_warn "Warning: Could not detect nginx user. Using world-readable permissions."
    $SUDO chmod -R o+r "$static_root" 2>/dev/null || true
    $SUDO find "$static_root" -type d -exec chmod o+rx {} \; 2>/dev/null || true
  else
    ui_success "Detected nginx user: ${nginx_user}"
    $SUDO chmod -R o+r "$static_root" 2>/dev/null || true
    $SUDO find "$static_root" -type d -exec chmod o+rx {} \; 2>/dev/null || true
  fi

  local current_dir="$panel_root"
  while [ "$current_dir" != "/" ] && [ -n "$current_dir" ]; do
    $SUDO chmod o+x "$current_dir" 2>/dev/null || true
    current_dir="$(dirname "$current_dir")"
  done

  ui_success "Permissions set successfully."
}

generate_base64url_token() {
  local bytes="${1:-32}"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 "$bytes" | tr '+/' '-_' | tr -d '=\n'
  else
    head -c "$bytes" /dev/urandom | od -An -tx1 | tr -d ' \n'
  fi
}

ensure_stealth_config() {
  local panel_root="$1"
  local stealth_dir="${panel_root}/data"
  local stealth_path="${stealth_dir}/stealth.json"

  if [ -s "$stealth_path" ]; then
    ui_info "Stealth config already present."
    return 0
  fi

  local created_at
  local cookie_secret
  local challenge_secret
  local html_variant_salt

  created_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  cookie_secret="$(generate_base64url_token 48)"
  challenge_secret="$(generate_base64url_token 48)"
  html_variant_salt="$(generate_base64url_token 24)"

  $SUDO mkdir -p "$stealth_dir"
  cat <<EOF | $SUDO tee "$stealth_path" >/dev/null
{
  "enabled": true,
  "cookieName": "adpanel_gate",
  "cookieSecret": "${cookie_secret}",
  "cookieTtlDays": 30,
  "challengeSecret": "${challenge_secret}",
  "htmlVariantSalt": "${html_variant_salt}",
  "createdAt": "${created_at}",
  "rotatedAt": "${created_at}"
}
EOF
  $SUDO chmod 600 "$stealth_path" 2>/dev/null || true
  ui_success "Generated stealth config."
}

write_nginx_config() {
  local server_name="${NGINX_SERVER_NAME:-$HOST}"
  server_name=$(echo "$server_name" | xargs)
  if [ -z "$server_name" ] || [ "$server_name" == "0.0.0.0" ] || [ "$server_name" == "127.0.0.1" ] || [ "$server_name" == "localhost" ]; then
    server_name="_"
  fi

  # Detect nginx brotli module availability
  local nginx_has_brotli="false"
  if nginx -V 2>&1 | grep -q 'brotli' || [ -f /etc/nginx/modules-enabled/*brotli* ] 2>/dev/null || [ -f /usr/share/nginx/modules/ngx_http_brotli_filter_module.so ] 2>/dev/null; then
    nginx_has_brotli="true"
  fi
  # Try to install brotli module if not present
  if [ "$nginx_has_brotli" == "false" ]; then
    if install_optional_nginx_brotli_module >/dev/null 2>&1; then
      nginx_has_brotli="true"
      ui_success "nginx brotli module installed."
    fi
  fi
  if [ "$nginx_has_brotli" == "true" ]; then
    ui_info "nginx brotli compression: enabled"
  else
    ui_info "nginx brotli compression: not available (using gzip only)"
  fi

  local upstream_host="${APP_HOST}"
  local upstream_port="${APP_PORT}"
  local panel_root
  panel_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local static_root="${panel_root}/public"

  local layout
  layout="$(detect_nginx_layout)"

  local config_path=""
  local ssl_config_path=""

  if [ "$layout" == "debian" ]; then
    config_path="/etc/nginx/sites-available/adpanel.conf"
    ssl_config_path="/etc/nginx/sites-available/adpanel-ssl.conf"
  else
    config_path="/etc/nginx/conf.d/adpanel.conf"
    ssl_config_path="/etc/nginx/conf.d/adpanel-ssl.conf"
  fi

  local has_ssl="false"
  if [ "$ENABLE_HTTPS" == "true" ] && [ -n "$SSL_CERT_PATH" ] && [ -n "$SSL_KEY_PATH" ]; then
    has_ssl="true"
  fi
  local https_port_segment=""
  if [ "$has_ssl" == "true" ] && [ "$HTTPS_PORT" != "443" ]; then
    https_port_segment=":${HTTPS_PORT}"
  fi


  cat <<EOF | $SUDO tee "$config_path" >/dev/null
map \$http_upgrade \$connection_upgrade {
  default upgrade;
  '' close;
}

upstream adpanel_backend {
  server ${upstream_host}:${upstream_port};
  keepalive 64;
}

server {
  listen ${HTTP_PORT};
  server_name ${server_name};
  server_tokens off;
  client_max_body_size 1100m;
  root ${static_root};

  if (\$request_method = TRACE) {
    return 444;
  }
  if (\$request_method = TRACK) {
    return 444;
  }
  if (\$request_method = OPTIONS) {
    return 204;
  }

  sendfile on;
  tcp_nopush on;
  tcp_nodelay on;
  keepalive_timeout 65;
  keepalive_requests 1000;

EOF

  if [ "$has_ssl" == "true" ] && [ "$FORCE_HTTPS" == "true" ]; then
    cat <<EOF | $SUDO tee -a "$config_path" >/dev/null
  return 301 https://\$host${https_port_segment}\$request_uri;
}
EOF
  else
    cat <<EOF | $SUDO tee -a "$config_path" >/dev/null
  gzip on;
  gzip_vary on;
  gzip_min_length 256;
  gzip_comp_level 2;
  gzip_proxied any;
  gzip_types text/plain text/css application/javascript application/json application/xml application/rss+xml image/svg+xml application/vnd.ms-fontobject application/x-font-ttf font/opentype;

EOF
    if [ "$nginx_has_brotli" == "true" ]; then
      cat <<EOF | $SUDO tee -a "$config_path" >/dev/null
  brotli on;
  brotli_comp_level 4;
  brotli_min_length 256;
  brotli_types text/plain text/css application/javascript application/json application/xml application/rss+xml image/svg+xml application/vnd.ms-fontobject application/x-font-ttf font/opentype;

EOF
    fi
    cat <<EOF | $SUDO tee -a "$config_path" >/dev/null

  location = /_stealth/nginx-auth {
    internal;
    proxy_pass http://adpanel_backend/_stealth/nginx-auth;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
  }

  location @adpanel_stealth_404 {
    internal;
    default_type text/plain;
    add_header Cache-Control "no-store, private" always;
    return 404 "";
  }

  location ^~ /.well-known/acme-challenge/ {
    auth_request off;
    access_log off;
    try_files \$uri @adpanel_backend;
  }

  location = /favicon.ico {
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location ^~ /auth-assets/ {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location = /branding-media/login-watermark {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location = /branding-media/login-background {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location = /login.css {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    access_log off;
  }

  location = /images/adpanel-dark.webp {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    access_log off;
  }

  location = /images/ADPanel-christmas.png {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    access_log off;
  }

  location = /images/bgvid.webm {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    access_log off;
  }

  location /css/ {
    alias ${static_root}/css/;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    etag on;
    access_log off;
    open_file_cache max=1000 inactive=60s;
    open_file_cache_valid 30s;
    open_file_cache_min_uses 1;
  }

  location /js/ {
    alias ${static_root}/js/;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    etag on;
    access_log off;
    open_file_cache max=1000 inactive=60s;
    open_file_cache_valid 30s;
    open_file_cache_min_uses 1;
  }

  location /images/ {
    alias ${static_root}/images/;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    etag on;
    access_log off;
    open_file_cache max=500 inactive=60s;
    open_file_cache_valid 30s;
    open_file_cache_min_uses 1;
  }

  location ~* \\.(?:woff2?|ttf|eot|otf)\$ {
    try_files \$uri =404;
    expires 365d;
    add_header Cache-Control "public, max-age=31536000, immutable";
    add_header Access-Control-Allow-Origin "*";
    access_log off;
  }

  location ~* ^/[^/]+\\.(?:css|js|mjs|png|jpe?g|gif|ico|svg|webp)\$ {
    try_files \$uri =404;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    etag on;
    access_log off;
  }

  location / {
    try_files \$uri @adpanel_backend;
  }

  location ~ ^/api/nodes/server/[^/]+/logs\$ {
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;

    proxy_buffering off;
    proxy_cache off;
    add_header X-Accel-Buffering no always;

    proxy_read_timeout 600s;
    proxy_send_timeout 600s;

    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;

    proxy_set_header Connection "";
  }

  location @adpanel_backend {
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_set_header Upgrade \$http_upgrade;
    proxy_set_header Connection \$connection_upgrade;

    proxy_buffering on;
    proxy_buffer_size 16k;
    proxy_buffers 8 32k;
    proxy_busy_buffers_size 64k;

    proxy_read_timeout 600s;
    proxy_send_timeout 600s;

    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;

    proxy_set_header Connection "";
  }
}
EOF
  fi

  if [ "$has_ssl" == "true" ]; then
    cat <<EOF | $SUDO tee "$ssl_config_path" >/dev/null
server {
  listen ${HTTPS_PORT} ssl http2;
  server_name ${server_name};
  server_tokens off;
  root ${static_root};

  if (\$request_method = TRACE) {
    return 444;
  }
  if (\$request_method = TRACK) {
    return 444;
  }
  if (\$request_method = OPTIONS) {
    return 204;
  }

  ssl_certificate ${SSL_CERT_PATH};
  ssl_certificate_key ${SSL_KEY_PATH};
  ssl_protocols TLSv1.2 TLSv1.3;
  ssl_prefer_server_ciphers off;
  ssl_session_cache shared:SSL:10m;
  ssl_session_timeout 1d;
  ssl_session_tickets off;

  add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;

  client_max_body_size 1100m;

  sendfile on;
  tcp_nopush on;
  tcp_nodelay on;
  keepalive_timeout 65;
  keepalive_requests 1000;

  gzip on;
  gzip_vary on;
  gzip_min_length 256;
  gzip_comp_level 2;
  gzip_proxied any;
  gzip_types text/plain text/css application/javascript application/json application/xml application/rss+xml image/svg+xml application/vnd.ms-fontobject application/x-font-ttf font/opentype;

EOF
    if [ "$nginx_has_brotli" == "true" ]; then
      cat <<EOF | $SUDO tee -a "$ssl_config_path" >/dev/null
  brotli on;
  brotli_comp_level 4;
  brotli_min_length 256;
  brotli_types text/plain text/css application/javascript application/json application/xml application/rss+xml image/svg+xml application/vnd.ms-fontobject application/x-font-ttf font/opentype;

EOF
    fi
    cat <<EOF | $SUDO tee -a "$ssl_config_path" >/dev/null

  location = /_stealth/nginx-auth {
    internal;
    proxy_pass http://adpanel_backend/_stealth/nginx-auth;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
  }

  location @adpanel_stealth_404 {
    internal;
    default_type text/plain;
    add_header Cache-Control "no-store, private" always;
    return 404 "";
  }

  location ^~ /.well-known/acme-challenge/ {
    auth_request off;
    access_log off;
    try_files \$uri @adpanel_backend;
  }

  location = /favicon.ico {
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location ^~ /auth-assets/ {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location = /branding-media/login-watermark {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location = /branding-media/login-background {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;
    proxy_set_header Connection "";
  }

  location = /login.css {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    access_log off;
  }

  location = /images/adpanel-dark.webp {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    access_log off;
  }

  location = /images/ADPanel-christmas.png {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    access_log off;
  }

  location = /images/bgvid.webm {
    auth_request /_stealth/nginx-auth;
    error_page 401 403 = @adpanel_stealth_404;
    try_files \$uri =404;
    add_header Cache-Control "no-store, private" always;
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    access_log off;
  }

  location /css/ {
    alias ${static_root}/css/;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    etag on;
    access_log off;
    open_file_cache max=1000 inactive=60s;
    open_file_cache_valid 30s;
    open_file_cache_min_uses 1;
  }

  location /js/ {
    alias ${static_root}/js/;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    etag on;
    access_log off;
    open_file_cache max=1000 inactive=60s;
    open_file_cache_valid 30s;
    open_file_cache_min_uses 1;
  }

  location /images/ {
    alias ${static_root}/images/;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    etag on;
    access_log off;
    open_file_cache max=500 inactive=60s;
    open_file_cache_valid 30s;
    open_file_cache_min_uses 1;
  }

  location ~* \\.(?:woff2?|ttf|eot|otf)\$ {
    try_files \$uri =404;
    expires 365d;
    add_header Cache-Control "public, max-age=31536000, immutable";
    add_header Access-Control-Allow-Origin "*";
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    access_log off;
  }

  location ~* ^/[^/]+\\.(?:css|js|mjs|png|jpe?g|gif|ico|svg|webp)\$ {
    try_files \$uri =404;
    expires 30d;
    add_header Cache-Control "public, max-age=2592000, immutable";
    add_header X-Content-Type-Options nosniff always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
    etag on;
    access_log off;
  }

  location / {
    try_files \$uri @adpanel_backend;
  }

  location ~ ^/api/nodes/server/[^/]+/logs\$ {
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;

    proxy_buffering off;
    proxy_cache off;
    add_header X-Accel-Buffering no always;
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;

    proxy_read_timeout 600s;
    proxy_send_timeout 600s;

    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;

    proxy_set_header Connection "";
  }

  location @adpanel_backend {
    proxy_pass http://adpanel_backend;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Host \$host;
    proxy_set_header X-Forwarded-Port \$server_port;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$remote_addr;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_set_header Upgrade \$http_upgrade;
    proxy_set_header Connection \$connection_upgrade;

    proxy_buffering on;
    proxy_buffer_size 16k;
    proxy_buffers 8 32k;
    proxy_busy_buffers_size 64k;

    proxy_read_timeout 600s;
    proxy_send_timeout 600s;

    proxy_hide_header X-Powered-By;
    proxy_hide_header Server;

    proxy_set_header Connection "";
  }
}
EOF
  else
    $SUDO rm -f "$ssl_config_path" >/dev/null 2>&1 || true
  fi

  if [ "$layout" == "debian" ]; then
    $SUDO ln -sf "$config_path" /etc/nginx/sites-enabled/adpanel.conf
    if [ -f "$ssl_config_path" ]; then
      $SUDO ln -sf "$ssl_config_path" /etc/nginx/sites-enabled/adpanel-ssl.conf
    else
      $SUDO rm -f /etc/nginx/sites-enabled/adpanel-ssl.conf
    fi
    if [ -f /etc/nginx/sites-enabled/default ]; then
      $SUDO rm -f /etc/nginx/sites-enabled/default
    fi
  fi

  fix_nginx_static_permissions "$panel_root"

  apply_nginx_performance_profile

  $SUDO nginx -t && service_reload nginx
}

apply_nginx_performance_profile() {
  local nginx_conf="/etc/nginx/nginx.conf"
  local perf_conf="/etc/nginx/conf.d/adpanel-performance.conf"
  local cache_dir="/var/cache/nginx/adpanel"
  local nginx_user=""

  if [ ! -f "$nginx_conf" ]; then
    ui_warn "nginx.conf not found; skipping nginx performance profile."
    return 0
  fi

  nginx_user="$(detect_nginx_user)"
  $SUDO mkdir -p "$cache_dir" >/dev/null 2>&1 || true
  if [ -n "$nginx_user" ] && id "$nginx_user" >/dev/null 2>&1; then
    $SUDO chown -R "$nginx_user":"$nginx_user" "$cache_dir" >/dev/null 2>&1 || true
  fi

  # Raise FD and connection capacity while keeping worker auto-scaling enabled.
  if ! grep -qE '^\s*worker_rlimit_nofile\s+' "$nginx_conf"; then
    $SUDO sed -i '/^worker_processes\s\+auto;/a worker_rlimit_nofile 65535;' "$nginx_conf" || true
  fi

  $SUDO sed -i -E 's/^\s*worker_connections\s+[0-9]+;/    worker_connections 4096;/' "$nginx_conf" || true
  $SUDO sed -i 's/^\s*#\s*multi_accept\s\+on;/    multi_accept on;/' "$nginx_conf" || true

  if ! grep -qE '^\s*use\s+epoll;' "$nginx_conf"; then
    $SUDO sed -i '/^\s*multi_accept\s\+on;/a\    use epoll;' "$nginx_conf" || true
  fi

  cat <<'EOF' | $SUDO tee "$perf_conf" >/dev/null
# ADPanel nginx performance profile (managed by initialize.sh)
# This file is included from nginx.conf inside the http {} context.

tcp_nodelay on;
keepalive_timeout 20;
keepalive_requests 10000;
send_timeout 30;
reset_timedout_connection on;

open_file_cache max=200000 inactive=30s;
open_file_cache_valid 60s;
open_file_cache_min_uses 2;
open_file_cache_errors on;

proxy_headers_hash_max_size 2048;
proxy_headers_hash_bucket_size 128;

client_body_timeout 15s;
client_header_timeout 15s;
client_body_buffer_size 128k;

gzip_comp_level 2;
gzip_vary on;
EOF
}

BCRYPT_CODE="
let bcrypt;
try { bcrypt = require('bcrypt'); } catch (e) {
  console.log('Using bcryptjs fallback');
  bcrypt = require('bcryptjs');
}
module.exports = bcrypt;
"

change_password() {
  ui_section "=== Change an user password ==="

  if ! ensure_node_prerequisites; then
    ui_error "Cannot continue without the required Node.js runtime and extensions."
    return 1
  fi

  read -p "$(ui_prompt "Enter user email: ")" EMAIL

  read -s -p "$(ui_prompt "Enter current password: ")" CURRENT
  echo ""

  while true; do
    read -s -p "$(ui_prompt "Enter new password: ")" NEW1
    echo ""
    read -s -p "$(ui_prompt "Confirm new password: ")" NEW2
    echo ""
    if [ "$NEW1" != "$NEW2" ]; then
      ui_error "Passwords do not match. Try again."
    else
      break
    fi
  done

  HASH=$(node -e "
    let bcrypt;
    try { bcrypt = require('bcrypt'); } catch (e) { bcrypt = require('bcryptjs'); }
    console.log(bcrypt.hashSync(process.argv[1] || '', 10));
  " "$NEW1")

  if [ -z "$CREATE_USER_SCRIPT" ]; then
    ui_error "create-user.js not found; cannot update password."
    exit 1
  fi

  if [ -n "$CURRENT" ]; then
    $SUDO node "$CREATE_USER_SCRIPT" --update-password --email "$EMAIL" --password-hash "$HASH" --current-password "$CURRENT"
  else
    $SUDO node "$CREATE_USER_SCRIPT" --update-password --email "$EMAIL" --password-hash "$HASH"
  fi
}

delete_user() {
  ui_section "=== Delete an user ==="

  if ! ensure_node_prerequisites; then
    ui_error "Cannot continue without the required Node.js runtime and extensions."
    return 1
  fi

  read -p "$(ui_prompt "Enter user email: ")" EMAIL
  ui_warn "This will permanently delete the user: ${EMAIL}"
  read -p "$(ui_prompt "Type YES to confirm: ")" CONFIRM
  if [ "$CONFIRM" != "YES" ]; then
    ui_warn "Cancelled."
    exit 0
  fi

  if [ -z "$CREATE_USER_SCRIPT" ]; then
    ui_error "create-user.js not found; cannot delete user."
    exit 1
  fi

  $SUDO node "$CREATE_USER_SCRIPT" --delete --email "$EMAIL"
}

initialize_panel() {
  ui_section "=== Panel Initialization ==="

  read -p "$(ui_prompt "Enter admin email: ")" EMAIL
  read -s -p "$(ui_prompt "Enter admin password: ")" PASSWORD
  echo ""

  if ! ensure_node_prerequisites; then
    ui_error "Cannot continue without the required system packages, Node.js runtime, and extensions."
    return 1
  fi

  SECRET=$(node - <<'EOF'
const speakeasy = require('speakeasy');
console.log(speakeasy.generateSecret({length: 20}).base32);
EOF
)

  ui_warn "Your 2FA secret (manual entry works too): $SECRET"

  ui_info "Scan this QR code in your Authenticator app:"
  node -e "
    const speakeasy = require('speakeasy');
    const qrcode = require('qrcode-terminal');
    const otpAuth = speakeasy.otpauthURL({
      secret: process.argv[1],
      label: process.argv[2],
      issuer: 'ADPanel',
      encoding: 'base32'
    });
    qrcode.generate(otpAuth, { small: true });
  " "$SECRET" "$EMAIL"

  HASH=$(node -e "
    let bcrypt;
    try { bcrypt = require('bcrypt'); } catch (e) { bcrypt = require('bcryptjs'); }
    console.log(bcrypt.hashSync(process.argv[1] || '', 10));
  " "$PASSWORD")

  ui_section "=== Network & HTTPS configuration ==="

  read -p "$(ui_prompt "Enter host to bind (default 0.0.0.0): ")" HOST
  HOST=${HOST:-0.0.0.0}

  read -p "$(ui_prompt "Do you want to use a domain name for the panel? (yes/no, default no): ")" DOMAIN_CHOICE
  DOMAIN_CHOICE=$(echo "$DOMAIN_CHOICE" | tr '[:upper:]' '[:lower:]')
  PANEL_DOMAIN=""
  if [ "$DOMAIN_CHOICE" == "yes" ] || [ "$DOMAIN_CHOICE" == "y" ]; then
    read -p "$(ui_prompt "Enter domain (e.g. panel.example.com): ")" PANEL_DOMAIN
    PANEL_DOMAIN=$(echo "$PANEL_DOMAIN" | xargs)
    if echo "$PANEL_DOMAIN" | tr '[:upper:]' '[:lower:]' | grep -q "adpanel"; then
      ui_warn "This hostname contains 'adpanel'. Public TLS certificates are logged in Certificate Transparency logs, so a neutral hostname is better for stealth."
    fi
  fi

  if [ -n "$PANEL_DOMAIN" ]; then
    NGINX_SERVER_NAME="$PANEL_DOMAIN"
    CSRF_ALLOWED_HOSTS="$PANEL_DOMAIN"
    PANEL_PUBLIC_URL="https://${PANEL_DOMAIN}"
  else
    ui_info "Detecting public IP..."
    inputs_ip=""
    if command -v curl >/dev/null 2>&1; then
      inputs_ip=$(curl -4 -s --max-time 3 ifconfig.me || curl -4 -s --max-time 3 icanhazip.com)
    fi
     if [ -z "$inputs_ip" ]; then
      inputs_ip=$(hostname -I | awk '{print $1}')
    fi
    if [ -z "$inputs_ip" ]; then
      inputs_ip="127.0.0.1"
    fi

    ui_warn "No domain selected. To prevent CSRF issues, we need the IP you will use to access the panel."
    read -p "$(ui_prompt "Enter panel IP (default ${inputs_ip}): ")" PANEL_IP_IN
    PANEL_IP_IN=$(echo "$PANEL_IP_IN" | xargs)
    if [ -z "$PANEL_IP_IN" ]; then
      PANEL_IP_IN="$inputs_ip"
    fi

    NGINX_SERVER_NAME="$PANEL_IP_IN"
    CSRF_ALLOWED_HOSTS="$PANEL_IP_IN"
    PANEL_PUBLIC_URL="http://${PANEL_IP_IN}"
    ui_success "Configured for IP access: ${PANEL_IP_IN}"
  fi

  read -p "$(ui_prompt "Do you want to enable HTTPS? (yes/no, default yes): ")" HTTPS_CHOICE
  HTTPS_CHOICE=$(echo "$HTTPS_CHOICE" | tr '[:upper:]' '[:lower:]')

  if [ -z "$HTTPS_CHOICE" ] || [ "$HTTPS_CHOICE" == "yes" ] || [ "$HTTPS_CHOICE" == "y" ]; then
    ENABLE_HTTPS=true

    read -p "$(ui_prompt "Enter HTTP port (default 80): ")" HTTP_PORT
    HTTP_PORT=${HTTP_PORT:-80}

    read -p "$(ui_prompt "Enter HTTPS port (default 443): ")" HTTPS_PORT
    HTTPS_PORT=${HTTPS_PORT:-443}

    read -p "$(ui_prompt "Force redirect HTTP to HTTPS? (yes/no, default yes): ")" FORCE_CHOICE
    FORCE_CHOICE=$(echo "$FORCE_CHOICE" | tr '[:upper:]' '[:lower:]')
    if [ -z "$FORCE_CHOICE" ] || [ "$FORCE_CHOICE" == "yes" ] || [ "$FORCE_CHOICE" == "y" ]; then
      FORCE_HTTPS=true
    else
      FORCE_HTTPS=false
    fi

    SESSION_COOKIE_SECURE=true
    if [ "$HTTP_PORT" != "80" ]; then
      ui_warn "Note: Let's Encrypt validation typically needs port 80 reachable. HTTP_PORT=$HTTP_PORT may cause issues."
    fi

  else
    ENABLE_HTTPS=false
    FORCE_HTTPS=false
    SESSION_COOKIE_SECURE=false

    read -p "$(ui_prompt "Enter HTTP port (default 80): ")" HTTP_PORT
    HTTP_PORT=${HTTP_PORT:-80}

    HTTPS_PORT=443
  fi

  NGINX_ENABLED=false
  APP_HOST="127.0.0.1"
  APP_PORT=3001
  if [ "$HTTP_PORT" == "$APP_PORT" ] || [ "$HTTPS_PORT" == "$APP_PORT" ]; then
    APP_PORT=3002
  fi

  SSL_KEY_PATH=
  SSL_CERT_PATH=

  ui_info "Generating strong SESSION_SECRET..."
  SESSION_SECRET=$(node - <<'EOF'
const crypto = require('crypto');
const min = 64;
const max = 99;
const length = Math.floor(Math.random() * (max - min + 1)) + min;
const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-';
let result = '';
const bytes = crypto.randomBytes(length);
for (let i = 0; i < length; i++) {
  result += chars[bytes[i] % chars.length];
}
console.log(result);
EOF
)

  ui_info "Generating DUMMY_HASH..."
  DUMMY_HASH=$(node - <<'EOF'
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const length = 64;
let raw = '';
const bytes = crypto.randomBytes(length);
for (let i = 0; i < length; i++) {
  raw += chars[bytes[i] % chars.length];
}
const hash = bcrypt.hashSync(raw, 10);
console.log(hash);
EOF
)

  ui_section "=== Captcha configuration (optional) ==="
  read -p "$(ui_prompt "Enter SITE_KEY (leave blank to disable captcha): ")" SITE_KEY
  SITE_KEY=$(echo "$SITE_KEY" | xargs)
  read -s -p "$(ui_prompt "Enter SECRET_KEY (leave blank to disable captcha): ")" SECRET_KEY
  echo ""
  SECRET_KEY=$(echo "$SECRET_KEY" | xargs)

  setup_redis
  setup_mysql

  # Enterprise security: lock down MariaDB and optionally harden firewall
  secure_mariadb_binding
  FIREWALL_HARDEN="${ADPANEL_FIREWALL_HARDEN:-}"
  if [ -z "$FIREWALL_HARDEN" ]; then
    read -p "$(ui_prompt "Apply firewall hardening for SMB/MySQL? This may install or enable a firewall service (yes/no, default no): ")" FIREWALL_HARDEN
  fi
  FIREWALL_HARDEN=$(echo "$FIREWALL_HARDEN" | tr '[:upper:]' '[:lower:]' | xargs)
  if [ "$FIREWALL_HARDEN" == "yes" ] || [ "$FIREWALL_HARDEN" == "y" ]; then
    harden_firewall
  else
    ui_warn "Skipping automatic firewall hardening. You can configure firewall rules manually later."
  fi
  mask_service_versions
  ensure_stealth_config "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  if install_nginx; then
    NGINX_ENABLED=true
    service_enable nginx
    service_start nginx
    write_nginx_config
  else
    ui_warn "nginx install failed; panel will run without reverse proxy."
  fi

  if [ "$ENABLE_HTTPS" == "true" ]; then
    if [ -z "$PANEL_DOMAIN" ]; then
      ui_warn "No domain provided; skipping Let's Encrypt certificate generation."
    else
      if install_certbot; then
        ensure_certbot_nginx_plugin >/dev/null 2>&1 || true
        ui_info "Requesting Let's Encrypt certificate for: ${PANEL_DOMAIN}"
        ui_info "If certbot asks questions (email/TOS/etc.), just answer them."
        if [ "$NGINX_ENABLED" == "true" ]; then
          $SUDO certbot --nginx -d "$PANEL_DOMAIN"
        else
          $SUDO certbot certonly --standalone -d "$PANEL_DOMAIN"
        fi

        if [ $? -eq 0 ]; then
          SSL_KEY_PATH="/etc/letsencrypt/live/$PANEL_DOMAIN/privkey.pem"
          SSL_CERT_PATH="/etc/letsencrypt/live/$PANEL_DOMAIN/fullchain.pem"
          ui_success "Certificate obtained successfully!"
        else
          ui_error "certbot failed or was cancelled. SSL paths will be left empty."
          SSL_KEY_PATH=
          SSL_CERT_PATH=
        fi
      else
        ui_error "certbot could not be installed. SSL paths will be left empty."
        SSL_KEY_PATH=
        SSL_CERT_PATH=
      fi
    fi
  fi

  if [ "$ENABLE_HTTPS" == "true" ] && { [ -z "$SSL_CERT_PATH" ] || [ -z "$SSL_KEY_PATH" ]; }; then
    ui_warn "HTTPS enabled but SSL cert/key missing. Falling back to HTTP-only."
    ENABLE_HTTPS=false
    FORCE_HTTPS=false
    SESSION_COOKIE_SECURE=false
  fi

  if [ "$NGINX_ENABLED" == "true" ]; then
    write_nginx_config
  fi

  APP_PORT_LINE=""
  if [ "$NGINX_ENABLED" == "true" ] && [ "$APP_PORT" != "3001" ]; then
    APP_PORT_LINE="APP_PORT=$APP_PORT"
  fi

  SSH_TERM_SCHEME="http"
  if [ "$ENABLE_HTTPS" == "true" ]; then
    SSH_TERM_SCHEME="https"
  fi
  SSH_TERM_HOST="$HOST"
  if [ -n "$PANEL_PUBLIC_URL" ]; then
    SSH_TERM_HOST="$(printf '%s' "$PANEL_PUBLIC_URL" | sed -E 's#^[A-Za-z]+://##; s#/.*$##')"
    SSH_TERM_HOST="${SSH_TERM_HOST%%:*}"
  fi
  SSH_TERM_PUBLIC_URL_VALUE="${SSH_TERM_SCHEME}://${SSH_TERM_HOST}:9393"

  cat <<EOF > .env
HOST=$HOST
CSRF_ALLOWED_HOSTS=$CSRF_ALLOWED_HOSTS
PANEL_PUBLIC_URL=$PANEL_PUBLIC_URL
HTTP_PORT=$HTTP_PORT
HTTPS_PORT=$HTTPS_PORT
ENABLE_HTTPS=$ENABLE_HTTPS
FORCE_HTTPS=$FORCE_HTTPS
SSL_KEY_PATH=$SSL_KEY_PATH
SSL_CERT_PATH=$SSL_CERT_PATH
SESSION_COOKIE_SECURE=$SESSION_COOKIE_SECURE
SESSION_STORE=$SESSION_STORE
NGINX_ENABLED=$NGINX_ENABLED
APP_HOST=$APP_HOST
${APP_PORT_LINE}
STEALTH_MODE=true
STEALTH_COOKIE_TTL_DAYS=30
STEALTH_RESPONSE_FLOOR_MS=90
STEALTH_ALLOW_HEALTHCHECK=false
SITE_KEY="$SITE_KEY"
SECRET_KEY="$SECRET_KEY"
REDIS_HOST=$REDIS_HOST
REDIS_PORT=$REDIS_PORT
REDIS_USER=$REDIS_USER
REDIS_PASSWORD="$REDIS_PASSWORD"
REDIS_URL="$REDIS_URL"
MYSQL_HOST=$MYSQL_HOST
MYSQL_PORT=$MYSQL_PORT
MYSQL_USER=$MYSQL_USER
MYSQL_PASSWORD="$MYSQL_PASSWORD"
MYSQL_DATABASE=$MYSQL_DATABASE
MYSQL_URL="$MYSQL_URL"
LOGIN_SUSPICIOUS_ATTEMPTS=5
LOGIN_SUSPICIOUS_FAST_WINDOW_MS=20000
LOGIN_SUSPICIOUS_FAST_ATTEMPTS=3
LOGIN_SUSPICIOUS_WINDOW_MS=300000
SESSION_SECRET="$SESSION_SECRET"
DUMMY_HASH="$DUMMY_HASH"
BROTLI_QUALITY=4
SSH_TERM_PORT=9393
SSH_TERM_PUBLIC_URL=$SSH_TERM_PUBLIC_URL_VALUE
EOF

  ui_success ".env file created with network and security settings."

  if bootstrap_db_and_create_admin "$EMAIL" "$HASH" "$SECRET"; then
    ui_success "Admin account created in database."
  else
    ui_warn "Panel setup completed, but admin account creation in database failed."
  fi
  ui_warn "Panel setup complete!"
  ui_info "Configuring and starting systemd service: adpanel..."
  if ! setup_adpanel_systemd_service; then
    ui_error "Failed to configure/start adpanel systemd service. Exiting."
    return 1
  fi
  ui_success "You can now manage ADPanel with systemd:"
  ui_kv "  Start:" "systemctl start adpanel"
  ui_kv "  Stop:" "systemctl stop adpanel"
  ui_kv "  Restart:" "systemctl restart adpanel"
  ui_kv "  Status:" "systemctl status adpanel"
  ui_info "Configuring and starting systemd service: adpanel-sshterm..."
  if setup_adpanel_sshterm_systemd_service; then
    ui_kv "  SSH Terminal status:" "systemctl status adpanel-sshterm"
  else
    ui_warn "SSH terminal service setup skipped."
  fi
}

load_mysql_env() {
  if [ ! -f "${SCRIPT_DIR}/.env" ]; then
    ui_error ".env not found. Run 'Initialize Panel' first."
    return 1
  fi

  local values
  values=$(node - <<'EOF'
const fs = require("fs");
const path = require("path");
const envPath = path.join(process.cwd(), ".env");
if (!fs.existsSync(envPath)) process.exit(2);
let parsed = {};
try {
  const dotenv = require("dotenv");
  parsed = dotenv.parse(fs.readFileSync(envPath, "utf8"));
} catch (err) {
  console.error(err && err.message ? err.message : String(err));
  process.exit(3);
}
const keys = ["MYSQL_URL", "MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"];
for (const key of keys) {
  const raw = parsed[key];
  process.stdout.write((raw == null ? "" : String(raw)).replace(/\r?\n/g, ""));
  process.stdout.write("\n");
}
EOF
)
  if [ $? -ne 0 ]; then
    ui_error "Failed to read MySQL settings from .env."
    return 1
  fi

  IFS=$'\n' read -r MYSQL_URL MYSQL_HOST MYSQL_PORT MYSQL_USER MYSQL_PASSWORD MYSQL_DATABASE <<< "$values"

  if [ -z "$MYSQL_URL" ] && { [ -z "$MYSQL_HOST" ] || [ -z "$MYSQL_DATABASE" ]; }; then
    ui_error "MySQL is not configured in .env. Set MYSQL_URL or MYSQL_HOST/MYSQL_DATABASE."
    return 1
  fi

  return 0
}

create_user() {
  ui_section "=== Create New User ==="

  if ! ensure_node_prerequisites; then
    ui_error "Cannot continue without the required Node.js runtime and extensions."
    return 1
  fi

  read -p "$(ui_prompt "Enter user email: ")" EMAIL
  EMAIL=$(echo "$EMAIL" | xargs)
  if [ -z "$EMAIL" ]; then
    ui_error "Email cannot be empty."
    return 1
  fi
  while true; do
    read -s -p "$(ui_prompt "Enter user password: ")" PASS1
    echo ""
    read -s -p "$(ui_prompt "Confirm user password: ")" PASS2
    echo ""
    if [ "$PASS1" != "$PASS2" ]; then
      ui_error "Passwords do not match. Try again."
    else
      break
    fi
  done

  read -p "$(ui_prompt "Should this user be an admin? (y/n): ")" ISADMIN

  SECRET=$(node - <<'EOF'
const speakeasy = require('speakeasy');
console.log(speakeasy.generateSecret({length: 20}).base32);
EOF
)

  ui_warn "Your 2FA secret (manual entry works too): $SECRET"

  ui_info "Scan this QR code in your Authenticator app:"
  node -e "
    const speakeasy = require('speakeasy');
    const qrcode = require('qrcode-terminal');
    const otpAuth = speakeasy.otpauthURL({
      secret: process.argv[1],
      label: process.argv[2],
      issuer: 'ADPanel',
      encoding: 'base32'
    });
    qrcode.generate(otpAuth, { small: true });
  " "$SECRET" "$EMAIL"

  local create_script="$CREATE_USER_SCRIPT"
  if [ -z "$create_script" ]; then
    ui_error "create-user.js not found; cannot create user."
    return 1
  fi

  if ! load_mysql_env; then
    return 1
  fi

  local admin_flag=""
  local avatar_url=""
  if echo "$ISADMIN" | tr '[:upper:]' '[:lower:]' | grep -qE '^y'; then
    admin_flag="--admin"
    avatar_url="$(pick_default_avatar_url admin)"
  else
    avatar_url="$(pick_default_avatar_url user)"
  fi
  ui_info "Assigned avatar: ${avatar_url}"

  local env_cmd=(env)
  [ -n "$MYSQL_URL" ] && env_cmd+=("MYSQL_URL=$MYSQL_URL")
  [ -n "$MYSQL_HOST" ] && env_cmd+=("MYSQL_HOST=$MYSQL_HOST")
  [ -n "$MYSQL_PORT" ] && env_cmd+=("MYSQL_PORT=$MYSQL_PORT")
  [ -n "$MYSQL_USER" ] && env_cmd+=("MYSQL_USER=$MYSQL_USER")
  env_cmd+=("MYSQL_PASSWORD=$MYSQL_PASSWORD")
  [ -n "$MYSQL_DATABASE" ] && env_cmd+=("MYSQL_DATABASE=$MYSQL_DATABASE")

  local cmd=("${env_cmd[@]}" node "$create_script" --email "$EMAIL" --password "$PASS1" --secret "$SECRET")
  if [ -n "$admin_flag" ]; then
    cmd+=("$admin_flag")
  fi
  if [ -n "$avatar_url" ]; then
    cmd+=("--avatar-url" "$avatar_url")
  fi
  if [ -n "$SUDO" ]; then
    cmd=("$SUDO" "${cmd[@]}")
  fi

  "${cmd[@]}"
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    ui_error "Failed to create user in MySQL."
    return "$rc"
  fi

  ui_success "User created successfully in MySQL."
}

if [ "$CHOICE" == "1" ]; then
  initialize_panel
elif [ "$CHOICE" == "2" ]; then
  change_password
elif [ "$CHOICE" == "3" ]; then
  delete_user
elif [ "$CHOICE" == "4" ]; then
  create_user
else
  ui_error "Invalid choice. Exiting."
  exit 1
fi
