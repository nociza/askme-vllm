#!/bin/bash

# Directory where socket files will be stored
SOCKET_DIR=/tmp/uvicorn_sockets

# Create the directory if it doesn't exist
mkdir -p $SOCKET_DIR

# Number of instances
NUM_INSTANCES=1

# Start multiple instances of the app with unique socket files
for ((i=1; i<=NUM_INSTANCES; i++))
do
    # Define the socket path
    SOCKET_PATH="$SOCKET_DIR/uvicorn_app_$i.sock"
    
    # Start Uvicorn with the socket
    poetry run uvicorn fleecekmbackend.main:app --uds $SOCKET_PATH &
done

echo "Started $NUM_INSTANCES instances of the app."
