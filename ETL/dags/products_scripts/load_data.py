import os
import pathlib

import pandas as pd


def main(ti, **kwargs):
    file_names = ti.xcom_pull(task_ids=["transform_task"])[0]
    for file_name in file_names:
        AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

        directory = pathlib.Path(AIRFLOW_HOME, "dags", "files", "output", file_name)
        df = pd.read_csv(directory)
        df.to_csv(pathlib.Path(AIRFLOW_HOME, "dags", "files", "output", file_name))
