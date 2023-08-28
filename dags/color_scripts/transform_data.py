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

        df['color-FullPrice'] = df['color-FullPrice'].astype(float)
        df['color-CurrentPrice'] = df['color-CurrentPrice'].astype(float)
        boolean_cols = ['color-Discount', 'color-BestSeller', 'color-InStock', 'color-MemberExclusive', 'color-New']
        for col in boolean_cols:
            df[col] = df[col].apply(lambda x: True if x == "True" else False)
        
        transformed_df: pd.DataFrame = df[[
            'color-ID', 'TopColor', 'color-Description', 'color-Label', 'color-Image-url', 
            'color-FullPrice', 'color-CurrentPrice', 'color-Discount', 'color-BestSeller', 
            'color-InStock', 'color-MemberExclusive', 'color-New'
        ]].drop_duplicates()

        destination = pathlib.Path(AIRFLOW_HOME, "data_warehouse", "data", file_name)
        transformed_df.to_csv(destination, index=False)

        ti.xcom_push(key='data', value=transformed_df)

    return file_names