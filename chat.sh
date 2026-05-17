#!/bin/bash

# --- Configuration ---
ENDPOINT="http://34.8.158.31/chat"
CACHE_ENDPOINT="http://34.8.158.31/cache"

# --- Colors & Formatting ---
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
DIM='\033[2m'
NC='\033[0m'

# --- Interactive Spinner Variables ---
SPIN_CHARS='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
SPIN_INDEX=0

print_spinner() {
    local msg="$1"
    local idx=$((SPIN_INDEX % 10))
    local char="${SPIN_CHARS:$idx:1}"
    # \r resets to start, \033[K clears the row forward to prevent ghost letters
    printf "\r${CYAN}[%s] %s...${NC}" "$char" "$msg"
    ((SPIN_INDEX++))
}

# --- Parse Arguments ---
WIPE_CACHE=false
STREAM_MODE=false
POSITIONAL_ARGS=()

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --wipe) WIPE_CACHE=true; shift ;;
        --stream) STREAM_MODE=true; shift ;;
        *) POSITIONAL_ARGS+=("$1"); shift ;;
    esac
done

if [ "$WIPE_CACHE" = true ]; then
    echo -e "${YELLOW}🧹 Wiping Semantic Cache...${NC}"
    curl -s -X DELETE "$CACHE_ENDPOINT"
fi

MESSAGE="${POSITIONAL_ARGS[0]:-\"Why did Maris leave Niles?\"}"
CLEAN_MESSAGE=$(echo "$MESSAGE" | tr -d '\n')
SESSION_ID=$(uuidgen | tr -d '-' | tr '[:lower:]' '[:upper:]' | cut -c 1-26)

echo -e "${DIM}Session ID: $SESSION_ID${NC}"
echo -e "${CYAN}🎙️  User:${NC} $CLEAN_MESSAGE"
echo ""

JSON_PAYLOAD=$(jq -n --arg msg "$CLEAN_MESSAGE" --argjson stream "$STREAM_MODE" '{message: $msg, stream: $stream}')

echo -e "${GREEN}🎧 Frasier:${NC}"

if [ "$STREAM_MODE" = true ]; then
    HEADER_FILE=$(mktemp)
    CURRENT_STATUS="Connecting to Gateway"
    HAS_STARTED_RESPONSE=false
    LAST_EVENT_TYPE=""

    print_spinner "$CURRENT_STATUS"

    curl -s -N -D "$HEADER_FILE" -X POST "$ENDPOINT" \
      -H "Content-Type: application/json" \
      -H "X-Pisces-Session-ID: $SESSION_ID" \
      -H "X-Pisces-Flag-NoSession: true" \
      -H "X-Pisces-Flag-SkipCache: true" \
      -d "$JSON_PAYLOAD" | while IFS= read -r raw_line; do
        
        # CRITICAL FIX: Strip hidden network carriage returns (\r) immediately
        line="${raw_line//$'\r'/}"

        if [[ -z "$line" ]]; then
            if [ "$HAS_STARTED_RESPONSE" = false ]; then
                print_spinner "$CURRENT_STATUS"
            fi
            continue
        fi

        # Track explicitly defined event frames
        if [[ "$line" =~ ^event:\ (.*) ]]; then
            LAST_EVENT_TYPE="${BASH_REMATCH[1]}"
            continue
        fi

        # Extract payload from standard data rows
        if [[ "$line" =~ ^data:\ (.*) ]]; then
            token="${BASH_REMATCH[1]}"

            if [[ "$token" == "[DONE]" ]]; then
                echo ""
                break
            fi

            # Handle status event changes cleanly
            if [[ "$LAST_EVENT_TYPE" == "status" ]]; then
                CURRENT_STATUS="$token"
                print_spinner "$CURRENT_STATUS"
                LAST_EVENT_TYPE=""
                continue
            fi

            # First real data token wipes the spinner line
            if [ "$HAS_STARTED_RESPONSE" = false ]; then
                printf "\r\033[K"
                HAS_STARTED_RESPONSE=true
            fi

            printf "%s" "$token"
            LAST_EVENT_TYPE=""
        fi
    done

    TRACE_ID=$(grep -i "X-Trace-Id:" "$HEADER_FILE" | awk '{print $2}' | tr -d '\r\n')
    if [ ! -z "$TRACE_ID" ]; then
        echo -e "\n\n${DIM}Trace ID: $TRACE_ID${NC}"
    fi
    rm -f "$HEADER_FILE"
else
    # Standard blocking pathway remains pristine
    TMP_FILE=$(mktemp)
    HEADER_FILE=$(mktemp)

    curl -s -D "$HEADER_FILE" -X POST "$ENDPOINT" \
      -H "Content-Type: application/json" \
      -H "X-Pisces-Session-ID: $SESSION_ID" \
      -H "X-Pisces-Flag-NoSession: true" \
      -H "X-Pisces-Flag-SkipCache: true" \
      -d "$JSON_PAYLOAD" > "$TMP_FILE" &

    CURL_PID=$!
    while kill -0 "$CURL_PID" 2>/dev/null; do
        print_spinner "Dr. Crane is listening"
        sleep 0.1
    done
    printf "\r\033[K"

    ANSWER=$(jq -r '.response // .answer // "Error: Could not parse response."' "$TMP_FILE")
    echo -e "$ANSWER\n"

    TRACE_ID=$(grep -i "X-Trace-Id:" "$HEADER_FILE" | awk '{print $2}' | tr -d '\r\n')
    if [ ! -z "$TRACE_ID" ]; then
        echo -e "${DIM}Trace ID: $TRACE_ID${NC}"
    fi
    rm -f "$TMP_FILE" "$HEADER_FILE"
fi