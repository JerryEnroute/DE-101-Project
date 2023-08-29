import os
import pathlib
import pandas as pd

def main():
    # Define paths
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    directory = pathlib.Path(AIRFLOW_HOME, "scrapper", "data", "sales")
    
    # Create an empty list to store individual dataframes
    dfs = []

    # Iterate through subdirectories and fetch CSV files
    for subdir, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(subdir, file)
                df = pd.read_csv(filepath)
                dfs.append(df)
                print(f"Read {filepath}")

    # Concatenate all the dataframes into one
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Save the final dataframe to sales_etl.csv
    destination_filename = "dates_etl2.csv"
    raw_destination = pathlib.Path(AIRFLOW_HOME, "data_warehouse", "data", destination_filename)
    combined_df.to_csv(raw_destination, index=False)
    print(f"Data merged and saved to {destination_filename}")

    return [destination_filename]

if __name__ == '__main__':
    main()
