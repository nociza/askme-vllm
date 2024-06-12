#!/bin/bash

DATA_FILE="experiments/data_samples/askme-10k.csv"
OUTPUT_DIR="experiments/data_samples/short_answers"
TEMP_DIR="temp_batches"
LOGS_DIR="logs"
SPLIT_SCRIPT="experiments/scripts/split_csv.py"

NUM_INSTANCES=32

mkdir -p $OUTPUT_DIR
mkdir -p $TEMP_DIR
mkdir -p $LOGS_DIR

poetry run python $SPLIT_SCRIPT

process_batch() {
    batch_file=$1
    output_file=$2
    log_file=$3
    poetry run python experiments/scripts/generate_short_answers.py $batch_file $output_file > $log_file 2>&1
}

pids=()
for batch_file in $TEMP_DIR/batch_*.csv; do
    output_file=$OUTPUT_DIR/$(basename $batch_file)
    log_file=$LOGS_DIR/$(basename $batch_file).log
    process_batch $batch_file $output_file $log_file &
    pids+=($!)
    
    if [ ${#pids[@]} -ge $NUM_INSTANCES ]; then
        wait -n
        pids=(${pids[@]/$!})
    fi
done

wait

rm -rf $TEMP_DIR

echo "Processing completed. Results are in $OUTPUT_DIR and logs are in $LOGS_DIR."
