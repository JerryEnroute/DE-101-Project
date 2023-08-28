import os
import pathlib

import pandas as pd


def main():
    # Reads CSV data
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    directory = pathlib.Path(AIRFLOW_HOME, "scrapper", "data", "sales", "2023", "08", "22")
    input_files = os.listdir(f"{directory}/")
    destination_files = []
    for input_file in input_files:
        destination_filename = "dates_etl.csv"
        print(input_file, "mapped to", destination_filename)
        raw_destination = pathlib.Path(AIRFLOW_HOME, "data_warehouse", "data", destination_filename)
        df = pd.read_csv(os.path.join(AIRFLOW_HOME, "scrapper", "data", "sales", "2023", "08", "22", input_file))
        df.to_csv(raw_destination)
        destination_files.append(destination_filename)
    return destination_files
