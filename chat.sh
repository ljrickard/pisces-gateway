#!/bin/bash

# Configuration
# 35.244.212.185
ENDPOINT="http://34.8.158.31/chat"
CACHE_ENDPOINT="http://34.8.158.31/cache"

# Check for wipe flag
if [[ "$1" == "--wipe" ]]; then
    echo "🧹 Wiping Semantic Cache and Session History..."
    curl -i -X DELETE "$CACHE_ENDPOINT"
    echo -e "\n--------------------------------------------"
    shift # Remove --wipe from the arguments
fi

MESSAGE=${1:-"Why did Maris leave Niles?"}
SESSION_ID=$2

# Generate a ULID if one isn't provided
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

# Construct the JSON payload
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

# Execute the chat request
curl -i -v -X POST "$ENDPOINT" \
  -H "Content-Type: application/json" \
  -H "X-Pisces-Flag-NoSession: true" \
  -H "X-Pisces-Flag-SkipCache: true" \
  -H "X-Pisces-Similarity-Threshold: 0.60" \
  -d "$JSON_PAYLOAD"