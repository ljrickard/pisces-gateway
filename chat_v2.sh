#!/bin/bash

# --- Configuration ---
# Pointing to the new V2 Engine endpoint
ENDPOINT="http://34.8.158.31/v2/chat"
CACHE_ENDPOINT="http://34.8.158.31/cache"

# --- Colors & Formatting ---
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
DIM='\033[2m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# --- Temp Files & Cleanup Trap ---
HEADER_FILE=$(mktemp)
STATUS_FILE=$(mktemp)
TMP_FILE=$(mktemp)

cleanup() {
    if [ -n "$SPINNER_PID" ]; then
        echo "STOP_SPINNER" > "$STATUS_FILE" 2>/dev/null
        kill -9 $SPINNER_PID 2>/dev/null
    fi
    printf "\r\033[2K"
    echo -e "\n${YELLOW}Session terminated by user.${NC}"
    rm -f "$HEADER_FILE" "$STATUS_FILE" "$TMP_FILE" 2>/dev/null
    exit 0
}
trap 'cleanup' SIGINT SIGTERM

# --- Interactive Spinner Variables ---
SPIN_CHARS='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'

print_spinner() {
    local msg="$1"
    local idx=$((SPIN_INDEX % 10))
    local char="${SPIN_CHARS:$idx:1}"
    printf "\r\033[2K${MAGENTA}[%s] %s...${NC}" "$char" "$msg"
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

# --- Robust Session ID Generation ---
if command -v uuidgen &> /dev/null; then
    SESSION_ID=$(uuidgen | tr -d '-' | tr '[:lower:]' '[:upper:]' | cut -c 1-26)
else
    SESSION_ID=$(cat /proc/sys/kernel/random/uuid 2>/dev/null | tr -d '-' | tr '[:lower:]' '[:upper:]')
    if [ -z "$SESSION_ID" ]; then
        SESSION_ID=$(date +%s%N | shasum 2>/dev/null | head -c 26 | tr '[:lower:]' '[:upper:]')
    fi
    if [ -z "$SESSION_ID" ]; then
        SESSION_ID=$RANDOM$RANDOM$RANDOM
    fi
fi

echo -e "${DIM}Session ID (V2): $SESSION_ID${NC}"
echo -e "${CYAN}🎙️  User:${NC} $CLEAN_MESSAGE"
echo ""

# 🚀 THE FIX: Inject the downstream config overrides here to force a broader search
JSON_PAYLOAD=$(jq -n \
  --arg msg "$CLEAN_MESSAGE" \
  --argjson stream "$STREAM_MODE" \
  '{
    message: $msg, 
    stream: $stream,
    config: {
      "use_episode_limit": false,
      "use_query_classification": false,
      "final_k": 30
    }
  }')

echo -e "${MAGENTA}🎧 Frasier (V2 Engine):${NC}"

if [ "$STREAM_MODE" = true ]; then
    echo "Connecting to V2 Graph Engine" > "$STATUS_FILE"

    # --- BACKGROUND SPINNER THREAD ---
    (
        SPIN_INDEX=0
        while [ -f "$STATUS_FILE" ]; do
            CURRENT_STATUS=$(cat "$STATUS_FILE")
            if [ "$CURRENT_STATUS" == "STOP_SPINNER" ]; then
                break
            fi
            print_spinner "$CURRENT_STATUS"
            sleep 0.1
        done
    ) &
    SPINNER_PID=$!

    HAS_STARTED_RESPONSE=false
    LAST_EVENT_TYPE="message"
    IGNORE_DATA_BLOCK=false

    curl -s -N -D "$HEADER_FILE" -X POST "$ENDPOINT" \
      -H "Content-Type: application/json" \
      -H "X-Pisces-Session-ID: $SESSION_ID" \
      -H "X-Pisces-Flag-NoSession: true" \
      -H "X-Pisces-Flag-SkipCache: true" \
      -d "$JSON_PAYLOAD" | while IFS= read -r raw_line; do
        
        line="${raw_line//$'\r'/}"

        if [[ -z "$line" ]]; then
            LAST_EVENT_TYPE="message"
            IGNORE_DATA_BLOCK=false
            continue
        fi

        if [[ "$line" =~ ^event:\ (.*) ]]; then
            LAST_EVENT_TYPE="${BASH_REMATCH[1]}"
            continue
        fi

        if [[ "$line" =~ ^data:\ (.*) ]]; then
            token="${BASH_REMATCH[1]}"

            if [[ "$token" == "[DONE]" ]]; then
                echo -e "\n"
                break
            fi

            if [[ "$LAST_EVENT_TYPE" == "status" ]]; then
                echo "$token" > "$STATUS_FILE"
                continue
            fi

            if [[ "$LAST_EVENT_TYPE" != "message" ]]; then
                continue
            fi

            if [[ "$token" =~ ^[[:space:]]*\{[[:space:]]*$ ]] || [[ "$token" =~ \"prompt_tokens\" ]]; then
                IGNORE_DATA_BLOCK=true
            fi

            if [ "$IGNORE_DATA_BLOCK" = true ]; then
                continue
            fi

            if [ "$HAS_STARTED_RESPONSE" = false ]; then
                echo "STOP_SPINNER" > "$STATUS_FILE"
                wait $SPINNER_PID 2>/dev/null
                printf "\r\033[2K" 
                HAS_STARTED_RESPONSE=true
            fi

            printf "%s" "$token"
        fi
    done

    if [ "$HAS_STARTED_RESPONSE" = false ]; then
        echo "STOP_SPINNER" > "$STATUS_FILE"
        wait $SPINNER_PID 2>/dev/null
        printf "\r\033[2K"
    fi

    TRACE_ID=$(grep -i "X-Trace-Id:" "$HEADER_FILE" | awk '{print $2}' | tr -d '\r\n')
    if [ ! -z "$TRACE_ID" ]; then
        echo -e "\n${DIM}V2 Trace ID: $TRACE_ID${NC}"
    fi
    
    rm -f "$HEADER_FILE" "$STATUS_FILE" "$TMP_FILE" 2>/dev/null
else
    # Standard blocking pathway
    curl -s -D "$HEADER_FILE" -X POST "$ENDPOINT" \
      -H "Content-Type: application/json" \
      -H "X-Pisces-Session-ID: $SESSION_ID" \
      -H "X-Pisces-Flag-NoSession: true" \
      -H "X-Pisces-Flag-SkipCache: true" \
      -d "$JSON_PAYLOAD" > "$TMP_FILE" &

    CURL_PID=$!
    SPIN_INDEX=0
    while kill -0 "$CURL_PID" 2>/dev/null; do
        print_spinner "Agent Graph is processing"
        sleep 0.1
    done
    printf "\r\033[2K"

    ANSWER=$(jq -r '.response // .answer // "Error: Could not parse response."' "$TMP_FILE")
    echo -e "$ANSWER\n"

    TRACE_ID=$(grep -i "X-Trace-Id:" "$HEADER_FILE" | awk '{print $2}' | tr -d '\r\n')
    if [ ! -z "$TRACE_ID" ]; then
        echo -e "${DIM}V2 Trace ID: $TRACE_ID${NC}"
    fi
    rm -f "$TMP_FILE" "$HEADER_FILE" "$STATUS_FILE" 2>/dev/null
fi