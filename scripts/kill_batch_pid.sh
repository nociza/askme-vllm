#!/bin/bash

# Read PIDs from the file and kill each process
while read pid; do
    kill $pid
done < uvicorn_pids.txt

# Optionally, clean up the PID file
rm uvicorn_pids.txt

echo "All Uvicorn instances stopped."
