#!/bin/bash
# Daily insider trade monitor — cron wrapper.
# Loads .env before running so SMTP credentials are available.
#
# Crontab entry:
#   30 7 * * 1-5 /home/zacharias/Schedule4/cron_schedule4.sh >> /home/zacharias/Schedule4/logs/cron.log 2>&1

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load credentials — keeps this isolated from other cron jobs
# shellcheck source=/dev/null
[ -f "$SCRIPT_DIR/.env" ] && set -a && source "$SCRIPT_DIR/.env" && set +a

echo "=== $(date) ==="

# Keep only the last 1000 lines (~2 weeks of runs)
LOG_FILE="$SCRIPT_DIR/logs/cron.log"
[ -f "$LOG_FILE" ] && tail -n 1000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"

.venv/bin/python daily_run.py

echo "=== Done ==="
