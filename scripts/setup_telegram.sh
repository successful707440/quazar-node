#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
  echo "Usage: TELEGRAM_BOT_TOKEN=your_token bash scripts/setup_telegram.sh" >&2
  echo "Optional: TELEGRAM_PROXY=socks5h://127.0.0.1:1080 (SSH gateway, auto-detected)" >&2
  echo "Before running: message @Quazar_Agent_Bot with /start" >&2
  exit 1
fi

exec bash "$ROOT/scripts/run_watcher.sh" --setup-telegram
