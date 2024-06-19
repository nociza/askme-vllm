#!/bin/bash

export PATH="$PATH:$HOME/.local/bin"

# Directory where logs will be stored
LOGS_DIR=./logs

# Create the logs directory if it doesn't exist
mkdir -p $LOGS_DIR

# Number of instances
NUM_INSTANCES=128

BACKGROUND_TASK_SCRIPT="fleecekmbackend/main.py"

for ((i=1; i<=NUM_INSTANCES; i++))
do
    UNIQUE_HASH=$(openssl rand -hex 12)

    LOG_FILE="$LOGS_DIR/background_task_$UNIQUE_HASH.log"
    
    poetry run python $BACKGROUND_TASK_SCRIPT > $LOG_FILE 2>&1 & 

    echo $! >> "/tmp/background_task_pids.txt"
done

echo "Started $NUM_INSTANCES instances of the background task."
