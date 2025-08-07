#!/bin/bash
# MarketWatch NYSE Scheduler - Unix Auto-Start Script
# This script runs the scheduler in a loop, automatically restarting if it crashes

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Starting MarketWatch NYSE Scheduler..."
echo "This script will automatically restart the scheduler if it crashes."
echo "Press Ctrl+C to stop."

while true; do
    echo ""
    echo "[$(date)] Starting scheduler..."
    
    # Run the scheduler
    python3 marketwatch_nyse_scheduler.py
    
    # Check exit code
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date)] Scheduler stopped normally."
        break
    else
        echo "[$(date)] Scheduler crashed with exit code $EXIT_CODE"
        echo "Restarting in 10 seconds..."
        sleep 10
    fi
done