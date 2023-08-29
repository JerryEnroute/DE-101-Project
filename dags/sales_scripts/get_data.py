import os
import pathlib
import pandas as pd

def main():
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    directory = pathlib.Path(AIRFLOW_HOME, "scrapper", "data", "sales")
    
    dfs = []

    for subdir, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(subdir, file)
                df = pd.read_csv(filepath)
                dfs.append(df)
                print(f"Read {filepath}")

    combined_df = pd.concat(dfs, ignore_index=True)
    
    destination_filename = "sales_etl.csv"
    raw_destination = pathlib.Path(AIRFLOW_HOME, "data_warehouse", "data", destination_filename)
    combined_df.to_csv(raw_destination, index=False)
    print(f"Data merged and saved to {destination_filename}")

    return [destination_filename]

if __name__ == '__main__':
    main()
