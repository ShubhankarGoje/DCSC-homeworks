import pandas as pd
import argparse
def process_csv(input_csv,output_csv):
    # Read the input CSV file
    df = pd.read_csv(input_csv)

    # Do something to the data (for example, let's assume we're adding 1 to all values in a column named 'value')
    df['value'] = df['Index'] + 10

    # Save the modified DataFrame to the output CSV file
    df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a CSV file.")
    parser.add_argument("input_csv", help="Input CSV file")
    parser.add_argument("output_csv", help="Output CSV file")
    args = parser.parse_args()

    process_csv(args.input_csv, args.output_csv)
    