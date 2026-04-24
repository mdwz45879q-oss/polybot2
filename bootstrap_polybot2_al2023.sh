#!/usr/bin/env bash
set -euo pipefail

# Assumes script lives inside repo root (polybot2/)
APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
VENV_DIR="${VENV_DIR:-$APP_DIR/.venv}"
APP_USER="${APP_USER:-$(stat -c '%U' "$APP_DIR" 2>/dev/null || whoami)}"

if [[ ! -f "$APP_DIR/pyproject.toml" ]]; then
  echo "ERROR: $APP_DIR does not look like repo root (missing pyproject.toml)"
  exit 1
fi

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "ERROR: repo not initialized at $APP_DIR (missing .git). Pull/clone first."
  exit 1
fi

run_as_app_user() {
  if [[ "$(id -un)" == "$APP_USER" ]]; then
    bash -lc "$*"
  else
    sudo -u "$APP_USER" -H bash -lc "$*"
  fi
}

echo "Repo: $APP_DIR"
echo "App user: $APP_USER"

# --- OS deps (AL2023) ---
sudo dnf -y update
sudo dnf -y install \
  git gcc gcc-c++ make pkgconfig openssl-devel libffi-devel \
  python3.11 python3.11-devel

# --- Rust toolchain for APP_USER ---
if ! run_as_app_user 'command -v rustup >/dev/null 2>&1'; then
  run_as_app_user 'curl https://sh.rustup.rs -sSf | sh -s -- -y'
fi
run_as_app_user 'source ~/.cargo/env && rustc --version && cargo --version'

# --- Runtime dirs ---
sudo mkdir -p /var/log/polybot2 "$APP_DIR/runtime" /etc/polybot2
sudo chown -R "$APP_USER:$APP_USER" /var/log/polybot2 "$APP_DIR/runtime"

# --- Python env + deps + native build ---
run_as_app_user "
  cd '$APP_DIR'
  python3.11 -m venv '$VENV_DIR'
  source '$VENV_DIR/bin/activate'
  pip install -U pip wheel setuptools maturin
  pip install -e '.[dev]'
  source ~/.cargo/env
  maturin build --release --manifest-path native/polybot2_native/Cargo.toml
  pip install --force-reinstall native/polybot2_native/target/wheels/polybot2_native-*.whl
"

# --- Optional smoke tests ---
run_as_app_user "
  cd '$APP_DIR'
  source '$VENV_DIR/bin/activate'
  pytest -q tests/test_polybot2_cli_smoke.py tests/test_polybot2_execution_hotpath_imports.py
"

# --- systemd unit ---
sudo tee /etc/systemd/system/polybot2-hotpath.service >/dev/null <<UNIT
[Unit]
Description=polybot2 hotpath service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=/etc/polybot2/polybot2.env
ExecStart=/bin/bash -lc '$VENV_DIR/bin/polybot2 \${POLYBOT2_COMMAND}'
Restart=always
RestartSec=3
TimeoutStopSec=30
KillSignal=SIGINT
LimitNOFILE=65535
StandardOutput=append:/var/log/polybot2/hotpath.log
StandardError=append:/var/log/polybot2/hotpath.err

[Install]
WantedBy=multi-user.target
UNIT

# --- env file template (only create if missing) ---
if [[ ! -f /etc/polybot2/polybot2.env ]]; then
  sudo tee /etc/polybot2/polybot2.env >/dev/null <<'ENV'
BOLTODDS_API_KEY=__SET_ME__
POLY_EXEC_PRESIGN_PRIVATE_KEY=__SET_ME__

# Example command. Edit for your actual run-id/db path/mode.
POLYBOT2_COMMAND=hotpath run --db /opt/polybot2/runtime/polybot2.sqlite --provider boltodds --league mlb --link-run-id 1 --execution-mode paper
ENV
fi

sudo systemctl daemon-reload

echo
echo "Bootstrap complete."
echo "Edit /etc/polybot2/polybot2.env, then run:"
echo "  sudo systemctl enable --now polybot2-hotpath"
echo "  sudo systemctl status polybot2-hotpath"
