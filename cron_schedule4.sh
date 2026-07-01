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

LOG_FILE="$SCRIPT_DIR/logs/cron.log"

echo "=== $(date) ==="

flock -n /tmp/schedule4.lock timeout 5400 .venv/bin/python daily_run.py

echo "=== Done ==="

# Trim after all output is flushed (~2 weeks of Mon-Fri runs)
[ -f "$LOG_FILE" ] && tail -n 1000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
