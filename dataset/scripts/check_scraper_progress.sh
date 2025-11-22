#!/bin/bash

# Script to monitor KPME full scraper progress

echo "========================================"
echo "KPME Full Scraper - Progress Monitor"
echo "========================================"
echo ""

LOG_FILE="/tmp/kpme_full_scraper.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "Log file not found. Scraper may not be running."
    exit 1
fi

# Get current page
CURRENT_PAGE=$(grep "SCRAPING PAGE" "$LOG_FILE" | tail -1 | grep -o "PAGE [0-9]*" | grep -o "[0-9]*")

# Get total establishments processed
TOTAL_PROCESSED=$(grep -c "^\[" "$LOG_FILE" 2>/dev/null || echo "0")

# Get last few log lines
echo "Current Status:"
echo "==============="
echo "Current Page: ${CURRENT_PAGE:-Unknown}"
echo "Total Processed: $TOTAL_PROCESSED establishments"
echo ""

echo "Recent Activity (last 10 lines):"
echo "================================="
tail -10 "$LOG_FILE"

echo ""
echo "========================================"
echo "To watch live: tail -f /tmp/kpme_full_scraper.log"
echo "========================================"
