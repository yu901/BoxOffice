#!/bin/bash

cd "$(dirname "$0")/.." || exit

if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

mkdir -p .pids

echo "Starting Dagster UI & Daemon in the background..."
nohup dagster dev -f src/definitions.py > dagster.log 2>&1 &

echo "Logs are in dagster.log"