#!/usr/bin/env bash

# wsl -e bash -lc "cd /mnt/c/shanx2026/vn30f1m && source .venv/bin/activate && bash v1/run.sh --port 8098"

set -euo pipefail
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec "$PROJECT_ROOT/.venv/bin/python" "$PROJECT_ROOT/v1/server.py" "$@"
