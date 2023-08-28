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

        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['sales'] = df['sales'].apply(lambda x: round(x, 2))        
        transformed_df: pd.DataFrame = df[["ticket_id", "UID", "currency", "sales", "quantity", "date", "year", "month", "day"]]
        destination = pathlib.Path(AIRFLOW_HOME, "data_warehouse", "data", file_name)
        transformed_df.to_csv(destination, index=False)

        ti.xcom_push(key='data', value=transformed_df)


    return file_names