#!/bin/bash

# Configuration
ENDPOINT="http://34.160.141.167/chat"
MESSAGE=${1:-"Why did Maris leave Niles?"}
SESSION_ID=$2

# Generate a ULID if one isn't provided as the second argument
if [ -z "$SESSION_ID" ]; then
    if command -v ulid &> /dev/null; then
        SESSION_ID=$(ulid)
    else
        SESSION_ID=$(uuidgen | tr -d '-' | tr '[:lower:]' '[:upper:]' | cut -c 1-26)
    fi
fi

echo "🚀 Sending request with SessionID: $SESSION_ID"
echo "💬 Message: $MESSAGE"
echo "--------------------------------------------"

# Construct the JSON payload with the nested, domain-specific config
JSON_PAYLOAD=$(cat <<EOF
{
  "message": "$MESSAGE",
  "config": {
    "frasier": {
      "fetch_k": 20,
      "final_k": 5,
      "specific_scale_fetch": 0.50,
      "specific_scale_final": 0.50
    }
  }
}
EOF
)

# Fire the request testing both Gateway Headers and Bot Payload
curl -i -X POST "$ENDPOINT" \
  -H "Content-Type: application/json" \
  -H "X-Pisces-Session-ID: $SESSION_ID" \
  -H "X-Pisces-Flag-SkipCache: true" \
  -H "X-Pisces-Similarity-Threshold: 0.95" \
  -d "$JSON_PAYLOAD"