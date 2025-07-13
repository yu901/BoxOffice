#!/bin/bash

cd "$(dirname "$0")/.." || exit

PID_FILE=".pids/dagster.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "PID file ($PID_FILE) not found. Is Dagster running?"
    echo "Attempting to stop via pkill as a fallback..."
    pkill -f "dagster dev"
    pkill -f "dagster-daemon"
    echo "Attempted to stop Dagster processes via pkill."
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! ps -p "$PID" > /dev/null; then
    echo "Process with PID $PID not found. Removing stale PID file."
    rm "$PID_FILE"
    exit 0
fi

echo "Stopping Dagster process group for PID: $PID..."
PGID=$(ps -o pgid= "$PID" | grep -o '[0-9]*')

if [ -n "$PGID" ]; then
    kill -- -"$PGID"
else
    kill "$PID"
fi

rm "$PID_FILE"
echo "Dagster processes stopped."