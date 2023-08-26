import os
import pandas as pd

from snowflake.connector import connect, SnowflakeConnection

def upload_to_snowflake(connection: SnowflakeConnection, data_frame, table_name):
    with connection.cursor() as cursor:
        query = f"INSERT INTO {table_name} (ticket_id, product_id, currency, sales, quantity) VALUES (%s, %s, %s, %s, %s)"
        data = data_frame[["ticket_id", "UID", "currency", "sales", "quantity"]].values.tolist()

        cursor.executemany(query, data)

with connect(
        account="emb55035",
        user="jerryenroute",
        password="2MQP2.Se",
        database="FACT_SALES",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
        region="us-west-2"
) as connection:
    list_dir = os.listdir("data")

    csv_files = filter(lambda item: item.endswith(".csv"),
    list_dir)

    for current_file in csv_files:
        df = pd.read_csv(f'./data/{current_file}')
    upload_to_snowflake(connection, df, "fact_sales")
