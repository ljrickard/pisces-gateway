#!/bin/bash

# Configuration
ENDPOINT="http://34.160.141.167/chat"
MESSAGE=${1:-"Why did Maris leave Niles?"}
SESSION_ID=$2

# Generate a ULID if one isn't provided as the second argument
if [ -z "$SESSION_ID" ]; then
    # If you have 'ulid' installed, use that. 
    # Otherwise, this is a "poor man's ULID" using uuidgen + uppercase
    # Note: Gateway validation is strict, so ensure this matches the ULID format
    if command -v ulid &> /dev/null; then
        SESSION_ID=$(ulid)
    else
        # Fallback: Generate a random string that fits the ULID length/case 
        # (Gateway might reject if checksum/alphabet isn't perfect, 
        # so 'brew install ulid' is recommended)
        SESSION_ID=$(uuidgen | tr -d '-' | tr '[:lower:]' '[:upper:]' | cut -c 1-26)
    fi
fi

echo "🚀 Sending request with SessionID: $SESSION_ID"
echo "💬 Message: $MESSAGE"
echo "--------------------------------------------"

curl -i -X POST "$ENDPOINT" \
  -H "Content-Type: application/json" \
  -H "X-Pisces-Session-ID: $SESSION_ID" \
  -H "X-Pisces-Flag-BypassCache: false" \
  -d "{\"message\": \"$MESSAGE\"}"