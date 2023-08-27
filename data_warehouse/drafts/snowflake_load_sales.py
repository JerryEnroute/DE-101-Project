import os
import pandas as pd

from snowflake.connector import connect, SnowflakeConnection

def get_date_id(connection: SnowflakeConnection, date):
    with connection.cursor() as cursor:
        query = f"SELECT date_id FROM dim_dates WHERE date_id = %s"
        cursor.execute(query, (date,))
        result = cursor.fetchone()
        return result[0] if result else None
    
def date_exists(connection: SnowflakeConnection, date):
    with connection.cursor() as cursor:
        query = f"SELECT COUNT(*) FROM dim_dates WHERE date_id = %s"
        cursor.execute(query, (date,))
        count = cursor.fetchone()[0]
        return count > 0

def upload_to_snowflake_dates(connection: SnowflakeConnection, data_frame, table_name):
    with connection.cursor() as cursor:
        query = f"INSERT INTO {table_name} (date_id, day, month, year) VALUES (%s, %s, %s, %s)"
        
        # Deduplicate dates in the dataframe
        data_frame.drop_duplicates(subset="date", inplace=True)
        data = data_frame[["date", "day", "month", "year"]].values.tolist()

        # Filter out dates that already exist in Snowflake
        data = [row for row in data if not date_exists(connection, row[0])]
        
        if data:  # Only execute if there's data to insert
            cursor.executemany(query, data)

def upload_to_snowflake_sales(connection: SnowflakeConnection, data_frame, table_name):
    with connection.cursor() as cursor:
        query = f"INSERT INTO {table_name} (ticket_id, product_id, date_id, currency, sales, quantity) VALUES (%s, %s, %s, %s, %s, %s)"
        data = [(row["ticket_id"], row["UID"], get_date_id(connection, row["date"]), 
                 row["currency"], row["sales"], row["quantity"]) for index, row in data_frame.iterrows()]
        cursor.executemany(query, data)

with connect(
        account="emb55035",
        user="jerryenroute",
        password="2MQP2.Se",
        database="NIKE_SALES",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
        region="us-west-2"
) as connection:
    list_dir = os.listdir("data")
    csv_files = filter(lambda item: item.endswith(".csv"), list_dir)

    for current_file in csv_files:
        df = pd.read_csv(f'./data/{current_file}')
        upload_to_snowflake_dates(connection, df, "dim_dates")
        upload_to_snowflake_sales(connection, df, "fact_sales")
