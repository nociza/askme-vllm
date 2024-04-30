#!/bin/bash

SOCKET_DIR=/tmp/uvicorn_sockets

# Loop over each socket file and kill the associated process
for socket in $SOCKET_DIR/*.sock; do
    # Extract the process using the socket
    pid=$(fuser $socket 2>/dev/null)
    if [ -n "$pid" ]; then
        kill $pid
    fi
done

# Optionally, remove socket files
rm -f $SOCKET_DIR/*.sock

echo "All Uvicorn instances stopped."
