#!/bin/bash

export PATH="$PATH:$HOME/.local/bin"

# Directory where socket files will be stored
SOCKET_DIR=/tmp/uvicorn_sockets
LOGS_DIR=./logs

# Create the directory if it doesn't exist
mkdir -p $SOCKET_DIR
mkdir -p $LOGS_DIR

# Number of instances
NUM_INSTANCES=16

# Start multiple instances of the app with unique socket files
for ((i=1; i<=NUM_INSTANCES; i++))
do
    # Generate a unique random hash for each socket using openssl
    UNIQUE_HASH=$(openssl rand -hex 12)

    SOCKET_PATH="$SOCKET_DIR/uvicorn_app_$UNIQUE_HASH.sock"
    LOG_FILE="$LOGS_DIR/uvicorn_app_$UNIQUE_HASH.log"
    
    # Start Uvicorn with the socket
    poetry run uvicorn fleecekmbackend.main:app --uds $SOCKET_PATH > $LOG_FILE 2>&1 & 

    # Save PID to a file
    echo $SOCKET_PATH >> "/tmp/uvicorn_pids.txt"
done

echo "Started $NUM_INSTANCES instances of the app."
