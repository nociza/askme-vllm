import pandas as pd
import os


def combine_csvs(input_dir, output_file):
    all_files = [
        os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".csv")
    ]

    combined_df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    combined_df.to_csv(output_file, index=False)
    print(f"Combined {len(all_files)} files into {output_file}")


if __name__ == "__main__":
    import sys

    input_dir = sys.argv[1]
    output_file = sys.argv[2]
    combine_csvs(input_dir, output_file)
