#!/bin/bash

cd "$(dirname "$0")/.." || exit

if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

mkdir -p .pids

echo "Starting Streamlit Dashboard in the background..."
nohup streamlit run src/dashboard.py > streamlit.log 2>&1 &
STREAMLIT_PID=$!

echo $STREAMLIT_PID > .pids/streamlit.pid
echo "Streamlit Dashboard started with PID: $STREAMLIT_PID. Logs are in streamlit.log"