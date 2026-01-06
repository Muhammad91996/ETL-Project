#!/bin/bash
# Wrapper script for running the Python transfer scripts from cron

# Set the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Set PATH for cron compatibility
export PATH="/home/ubuntu/.magento-cloud/bin:/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin:$PATH"

# Set environment variables for cron
export HOME="${HOME:-/home/ubuntu}"
export USER="${USER:-ubuntu}"
export SHELL="${SHELL:-/bin/bash}"

# Clean up old dump files (keep only the oldest)
ls -1tr ./dump/magento_dump_*.sql 2>/dev/null | tail -n +2 | xargs -r rm -f 2>/dev/null || true

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Load .env explicitly for cron
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Run script
./venv/bin/python transfer_commercial_report_to_google.py