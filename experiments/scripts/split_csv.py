import pandas as pd
import os

# Configurations
data_file = "experiments/data_samples/askme-10k.csv"
output_dir = "temp_batches"
batch_size = 1000

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the entire CSV file
data_df = pd.read_csv(data_file)

# Calculate the number of batches
total_batches = (len(data_df) + batch_size - 1) // batch_size

# Split the data into smaller CSV files
for i in range(total_batches):
    start_index = i * batch_size
    end_index = min((i + 1) * batch_size, len(data_df))
    batch_df = data_df[start_index:end_index]
    batch_file = os.path.join(output_dir, f"batch_{i}.csv")
    batch_df.to_csv(batch_file, index=False)

print(f"Splitting completed. {total_batches} batches created in {output_dir}.")
