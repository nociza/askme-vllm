#!/bin/bash

# PID file
PID_FILE="/tmp/background_task_pids.txt"

# Check if the PID file exists
if [ ! -f $PID_FILE ]; then
    echo "PID file not found!"
    exit 1
fi

# Kill each process listed in the PID file
while read -r pid; do
    if [ -n "$pid" ]; then
        kill -9 $pid
        echo "Killed process with PID: $pid"
    fi
done < $PID_FILE

# Remove the PID file after killing the processes
rm -f $PID_FILE

rm -f ./logs/*.log

echo "All background tasks killed and PID file removed."
