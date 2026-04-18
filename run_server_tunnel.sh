#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

if [ -z "${PYTHON_BIN:-}" ]; then
  if command_exists python3; then
    PYTHON_BIN="python3"
  elif command_exists python; then
    PYTHON_BIN="python"
  else
    echo "[ERROR] python3/python not found in PATH."
    echo "Install Python and reopen terminal."
    exit 1
  fi
fi

if ! command_exists "$PYTHON_BIN"; then
  echo "[ERROR] PYTHON_BIN is not executable: $PYTHON_BIN"
  exit 1
fi

if [ ! -f ".env" ]; then
  echo "[ERROR] Missing .env file in project root."
  echo "[INFO] Create .env from template with: cp .env.example .env"
  exit 1
fi

exec "$PYTHON_BIN" run_server_tunnel.py "$@"
