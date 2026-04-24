#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# bootstrap_polybot2_al2023.sh
#
# Sets up an Amazon Linux 2023 EC2 instance for polybot2:
#   - OS-level build dependencies
#   - Miniconda (Python 3.11 env with conda for additional packages)
#   - Rust toolchain via rustup
#   - Python package + Rust native module build
#   - Runtime directories, systemd unit, env file template
#
# Usage:
#   cd /path/to/polybot2 && bash bootstrap_polybot2_al2023.sh
#
# Idempotent — safe to re-run after pulling new code.
# ---------------------------------------------------------------------------
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
APP_USER="${APP_USER:-$(stat -c '%U' "$APP_DIR" 2>/dev/null || whoami)}"
CONDA_DIR="${CONDA_DIR:-/home/$APP_USER/miniconda3}"
CONDA_ENV="${CONDA_ENV:-polybot2}"
PYTHON_VERSION="3.11"

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

echo "=== polybot2 bootstrap ==="
echo "Repo:        $APP_DIR"
echo "App user:    $APP_USER"
echo "Conda dir:   $CONDA_DIR"
echo "Conda env:   $CONDA_ENV"
echo "Python:      $PYTHON_VERSION"
echo

# ───────────────────────────────────────────────────────────────────────────
# 1. OS-level build dependencies
# ───────────────────────────────────────────────────────────────────────────
echo ">>> Installing OS packages..."
sudo dnf -y update
sudo dnf -y install \
  git gcc gcc-c++ make pkgconfig \
  openssl-devel libffi-devel bzip2-devel xz-devel zlib-devel \
  tar gzip wget which htop tmux jq

# ───────────────────────────────────────────────────────────────────────────
# 2. Miniconda
# ───────────────────────────────────────────────────────────────────────────
if [[ ! -d "$CONDA_DIR" ]]; then
  echo ">>> Installing Miniconda..."
  INSTALLER="/tmp/miniconda_installer.sh"
  ARCH="$(uname -m)"
  curl -fsSL "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${ARCH}.sh" -o "$INSTALLER"
  run_as_app_user "bash '$INSTALLER' -b -p '$CONDA_DIR'"
  rm -f "$INSTALLER"

  # Add conda init to shell profile
  run_as_app_user "'$CONDA_DIR/bin/conda' init bash"
else
  echo ">>> Miniconda already installed at $CONDA_DIR"
fi

# ───────────────────────────────────────────────────────────────────────────
# 3. Conda environment with Python 3.11
# ───────────────────────────────────────────────────────────────────────────
if ! run_as_app_user "source '$CONDA_DIR/etc/profile.d/conda.sh' && conda env list | grep -q '^${CONDA_ENV} '"; then
  echo ">>> Creating conda env '$CONDA_ENV' with Python $PYTHON_VERSION..."
  run_as_app_user "source '$CONDA_DIR/etc/profile.d/conda.sh' && conda create -y -n '$CONDA_ENV' python=$PYTHON_VERSION"
else
  echo ">>> Conda env '$CONDA_ENV' already exists"
fi

# ───────────────────────────────────────────────────────────────────────────
# 4. Rust toolchain
# ───────────────────────────────────────────────────────────────────────────
if ! run_as_app_user 'command -v rustup >/dev/null 2>&1'; then
  echo ">>> Installing Rust toolchain..."
  run_as_app_user 'curl https://sh.rustup.rs -sSf | sh -s -- -y'
else
  echo ">>> Rust toolchain already installed"
  run_as_app_user 'source ~/.cargo/env && rustup update stable'
fi
run_as_app_user 'source ~/.cargo/env && rustc --version && cargo --version'

# ───────────────────────────────────────────────────────────────────────────
# 5. Runtime directories
# ───────────────────────────────────────────────────────────────────────────
echo ">>> Setting up runtime directories..."
sudo mkdir -p /var/log/polybot2 "$APP_DIR/runtime" /etc/polybot2
sudo chown -R "$APP_USER:$APP_USER" /var/log/polybot2 "$APP_DIR/runtime"

# ───────────────────────────────────────────────────────────────────────────
# 6. Python deps + native Rust module build
# ───────────────────────────────────────────────────────────────────────────
echo ">>> Installing Python deps and building native module..."
run_as_app_user "
  source '$CONDA_DIR/etc/profile.d/conda.sh'
  conda activate '$CONDA_ENV'
  source ~/.cargo/env

  cd '$APP_DIR'
  pip install -U pip wheel setuptools maturin
  pip install -e '.[dev]'
  maturin develop --release --manifest-path native/polybot2_native/Cargo.toml
"

# ───────────────────────────────────────────────────────────────────────────
# 7. Smoke tests
# ───────────────────────────────────────────────────────────────────────────
echo ">>> Running smoke tests..."
run_as_app_user "
  source '$CONDA_DIR/etc/profile.d/conda.sh'
  conda activate '$CONDA_ENV'

  cd '$APP_DIR'
  cargo test --manifest-path native/polybot2_native/Cargo.toml -q
  pytest -q tests/test_polybot2_cli_smoke.py tests/test_polybot2_execution_hotpath_imports.py || true
"

# ───────────────────────────────────────────────────────────────────────────
# 8. systemd service unit
# ───────────────────────────────────────────────────────────────────────────
echo ">>> Installing systemd unit..."
CONDA_PYTHON="$CONDA_DIR/envs/$CONDA_ENV/bin/python"
CONDA_ACTIVATE="source $CONDA_DIR/etc/profile.d/conda.sh && conda activate $CONDA_ENV && source ~/.cargo/env"

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
ExecStart=/bin/bash -lc '$CONDA_ACTIVATE && polybot2 \${POLYBOT2_COMMAND}'
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

# ───────────────────────────────────────────────────────────────────────────
# 9. Environment file template (only create if missing)
# ───────────────────────────────────────────────────────────────────────────
if [[ ! -f /etc/polybot2/polybot2.env ]]; then
  echo ">>> Creating env file template at /etc/polybot2/polybot2.env..."
  sudo tee /etc/polybot2/polybot2.env >/dev/null <<'ENV'
# =============================================================================
# POLYMARKET CREDENTIALS
# =============================================================================
POLY_EXEC_API_KEY=__SET_ME__
POLY_EXEC_API_SECRET=__SET_ME__
POLY_EXEC_API_PASSPHRASE=__SET_ME__
POLY_EXEC_PRESIGN_PRIVATE_KEY=__SET_ME__
POLY_EXEC_FUNDER=__SET_ME__
POLY_EXEC_SIGNATURE_TYPE=1
POLY_EXEC_CLOB_HOST=https://clob.polymarket.com

# =============================================================================
# PROVIDER CREDENTIALS
# =============================================================================
KALSTROP_CLIENT_ID=__SET_ME__
KALSTROP_SHARED_SECRET_RAW=__SET_ME__

# =============================================================================
# HOTPATH COMMAND
# Edit for your actual run-id, db path, and execution mode.
# =============================================================================
POLYBOT2_COMMAND=hotpath run --db /opt/polybot2/runtime/polybot2.sqlite --provider kalstrop --league mlb --link-run-id 1 --execution-mode paper --with-observe
ENV
  sudo chmod 600 /etc/polybot2/polybot2.env
  sudo chown "$APP_USER:$APP_USER" /etc/polybot2/polybot2.env
else
  echo ">>> Env file already exists at /etc/polybot2/polybot2.env (not overwriting)"
fi

sudo systemctl daemon-reload

# ───────────────────────────────────────────────────────────────────────────
# Done
# ───────────────────────────────────────────────────────────────────────────
echo
echo "=== Bootstrap complete ==="
echo
echo "Conda env:   conda activate $CONDA_ENV"
echo "Env file:    /etc/polybot2/polybot2.env"
echo "Service:     sudo systemctl enable --now polybot2-hotpath"
echo "Logs:        tail -f /var/log/polybot2/hotpath.log"
echo
echo "To install additional packages:"
echo "  conda activate $CONDA_ENV && conda install <package>"
echo "  conda activate $CONDA_ENV && pip install <package>"
echo
echo "To rebuild after pulling new code:"
echo "  conda activate $CONDA_ENV && cd $APP_DIR"
echo "  pip install -e '.[dev]'"
echo "  maturin develop --release --manifest-path native/polybot2_native/Cargo.toml"
echo "  sudo systemctl restart polybot2-hotpath"
