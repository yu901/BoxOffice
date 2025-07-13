#!/bin/bash

cd "$(dirname "$0")/.." || exit

if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

mkdir -p .pids

echo "Starting Dagster UI & Daemon in the background..."
nohup dagster dev -f src/definitions.py > dagster.log 2>&1 &
DAGSTER_PID=$!

echo $DAGSTER_PID > .pids/dagster.pid
echo "Dagster UI & Daemon started with PID: $DAGSTER_PID. Logs are in dagster.log"