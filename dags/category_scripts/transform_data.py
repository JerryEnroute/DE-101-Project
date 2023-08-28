import os
import pathlib
import pandas as pd


def main(ti, **kwargs):
    file_names = ti.xcom_pull(task_ids=["extract_task"])[0]
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    for file_name in file_names:
        print(file_name)
        extracted_data = os.path.join(AIRFLOW_HOME, "data_warehouse", "data", file_name)

        df = pd.read_csv(extracted_data, index_col=0)
        
        df_category: pd.DataFrame = df[['category']].drop_duplicates()

        destination = pathlib.Path(AIRFLOW_HOME, "data_warehouse", "data", file_name)
        df_category.to_csv(destination, index=False)

        ti.xcom_push(key='data', value=df_category)

    return file_names