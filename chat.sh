#!/bin/bash

# Ensure jq is installed
if ! command -v jq &> /dev/null; then
    echo "❌ Error: 'jq' is not installed. Please install it (e.g., brew install jq) to parse the JSON."
    exit 1
fi

# --- Configuration ---
ENDPOINT="http://34.8.158.31/chat"
CACHE_ENDPOINT="http://34.8.158.31/cache"

# --- Colors & Formatting ---
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
DIM='\033[2m'
NC='\033[0m' # No Color

# --- Spinner Function ---
spinner() {
    local pid=$1
    local delay=0.1
    local spinstr='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    while kill -0 "$pid" 2>/dev/null; do
        local temp=${spinstr#?}
        printf " ${CYAN}[%c] Dr. Crane is listening...${NC}" "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\r\033[K" # Clear the line
    done
}

# --- Parse Arguments ---
if [[ "$1" == "--wipe" ]]; then
    echo -e "${YELLOW}🧹 Wiping Semantic Cache and Session History...${NC}"
    curl -s -X DELETE "$CACHE_ENDPOINT"
    shift 
fi

MESSAGE=${1:-"Why did Maris leave Niles?"}

# Clean the message of any terminal newlines
CLEAN_MESSAGE=$(echo "$MESSAGE" | tr -d '\n')

# Generate a mock ULID for the session
if command -v ulid &> /dev/null; then
    SESSION_ID=$(ulid)
else
    SESSION_ID=$(uuidgen | tr -d '-' | tr '[:lower:]' '[:upper:]' | cut -c 1-26)
fi

echo -e "${DIM}Session ID: $SESSION_ID${NC}"
echo -e "${CYAN}🎙️  User:${NC} $CLEAN_MESSAGE"
echo ""

# Construct the JSON payload securely using jq
JSON_PAYLOAD=$(jq -n --arg msg "$CLEAN_MESSAGE" '{
  message: $msg,
  config: {
    frasier: {
      fetch_k: 20,
      final_k: 5,
      specific_scale_fetch: 0.50,
      specific_scale_final: 0.50
    }
  }
}')

# --- Execute Request ---
# We use a temporary file to store the result so the spinner can run in the foreground
TMP_FILE=$(mktemp)

# Run cURL silently (-s) in the background
curl -s -X POST "$ENDPOINT" \
  -H "Content-Type: application/json" \
  -H "X-Pisces-Session-ID: $SESSION_ID" \
  -H "X-Pisces-Flag-NoSession: true" \
  -H "X-Pisces-Flag-SkipCache: true" \
  -H "X-Pisces-Similarity-Threshold: 0.60" \
  -d "$JSON_PAYLOAD" > "$TMP_FILE" &

CURL_PID=$!

# Start the loading animation while cURL runs
spinner $CURL_PID

# --- Extract and Print Answer ---
# Extract the 'response' key using jq. (Falls back to 'answer' just in case)
ANSWER=$(jq -r '.response // .answer // "Error: Could not parse response. Run without jq to debug."' "$TMP_FILE")

echo -e "${GREEN}🎧 Frasier:${NC}"
echo -e "$ANSWER"
echo ""

# Clean up
rm -f "$TMP_FILE"