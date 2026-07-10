#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/agent/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/agent/.env"
  set +a
elif [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

exec python3 "$ROOT/agent/watcher.py" "$@"
