#!/bin/bash

cd "$(dirname "$0")/.." || exit

PID_FILE=".pids/streamlit.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "PID file ($PID_FILE) not found. Is Streamlit running?"
    echo "Attempting to stop via pkill as a fallback..."
    pkill -f "streamlit run src/dashboard.py"
    echo "Attempted to stop Streamlit process via pkill."
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! ps -p "$PID" > /dev/null; then
    echo "Process with PID $PID not found. Removing stale PID file."
    rm "$PID_FILE"
    exit 0
fi

echo "Stopping Streamlit process with PID: $PID..."
kill "$PID"

rm "$PID_FILE"
echo "Streamlit Dashboard has been stopped."