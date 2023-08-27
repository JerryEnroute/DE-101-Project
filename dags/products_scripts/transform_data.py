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

        df['fullPrice'] = df['fullPrice'].astype(float)
        df['currentPrice'] = df['currentPrice'].astype(float)
        df['color-FullPrice'] = df['color-FullPrice'].astype(float)
        df['color-CurrentPrice'] = df['color-CurrentPrice'].astype(float)
        boolean_cols = ['sale', 'customizable', 'ExtendedSizing', 'inStock', 'ComingSoon', 'BestSeller', 'Excluded', 'GiftCard', 'Jersey', 'Launch', 'MemberExclusive', 'NBA', 'NFL', 'Sustainable', 'color-Discount', 'color-BestSeller', 'color-InStock', 'color-MemberExclusive', 'color-New']
        for col in boolean_cols:
            df[col] = df[col].apply(lambda x: True if x == "True" else False)
        transformed_df: pd.DataFrame = df[[ 'UID', 'cloudProdID', 'productID', 'shortID', 'colorNum', 'title', 'subtitle', 'category', 'type', 'currency', 'fullPrice', 'currentPrice', 'TopColor', 'channel', 'short_description', 'label', 'prebuildId', 'prod_url', 'color-ID', 'color-Description', 'color-Label', 'color-Image-url', 'color-FullPrice', 'color-CurrentPrice', 'rating', 'sale', 'customizable', 'ExtendedSizing', 'inStock', 'ComingSoon', 'BestSeller', 'Excluded', 'GiftCard', 'Jersey', 'Launch', 'MemberExclusive', 'NBA', 'NFL', 'Sustainable', 'color-Discount', 'color-BestSeller', 'color-InStock', 'color-MemberExclusive', 'color-New']]
        destination = pathlib.Path(AIRFLOW_HOME, "data_warehouse", "data", file_name)
        transformed_df.to_csv(destination, index=False)

        ti.xcom_push(key='data', value=transformed_df)


    return file_names
