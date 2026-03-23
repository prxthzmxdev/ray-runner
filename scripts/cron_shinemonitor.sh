#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

python scripts/test_shinemonitor.py
