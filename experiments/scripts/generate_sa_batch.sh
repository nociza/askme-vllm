#!/bin/bash

# Directories
DATA_FILE="experiments/data_samples/askme-10k.csv"
OUTPUT_DIR="experiments/data_samples/short_answers"
TEMP_DIR="temp_batches"
LOGS_DIR="logs"

# Batch size and number of instances
BATCH_SIZE=1000
NUM_INSTANCES=32

# Ensure the necessary directories exist
mkdir -p $OUTPUT_DIR
mkdir -p $TEMP_DIR
mkdir -p $LOGS_DIR

# Split the data into smaller CSV files
split -l $BATCH_SIZE --additional-suffix=.csv $DATA_FILE $TEMP_DIR/batch_

# Function to process a single batch
process_batch() {
    batch_file=$1
    output_file=$2
    log_file=$3
    python experiments/scripts/generate_short_answers.py $batch_file $output_file > $log_file 2>&1
}

# Process batches in parallel
pids=()
for batch_file in $TEMP_DIR/batch_*.csv; do
    output_file=$OUTPUT_DIR/$(basename $batch_file)
    log_file=$LOGS_DIR/$(basename $batch_file).log
    process_batch $batch_file $output_file $log_file &
    pids+=($!)
    
    # Limit the number of parallel jobs
    if [ ${#pids[@]} -ge $NUM_INSTANCES ]; then
        wait -n
        pids=(${pids[@]/$!})
    fi
done

# Wait for all background jobs to complete
wait

# Clean up temporary files
rm -rf $TEMP_DIR

echo "Processing completed. Results are in $OUTPUT_DIR and logs are in $LOGS_DIR."
