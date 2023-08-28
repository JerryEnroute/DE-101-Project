import os
import pandas as pd
from snowflake.connector import connect, SnowflakeConnection

def get_category_id(connection: SnowflakeConnection, category):
    with connection.cursor() as cursor:
        query = f"SELECT id FROM dim_category WHERE category = %s"
        cursor.execute(query, (category,))
        result = cursor.fetchone()
        return result[0] if result else None

def get_color_id(connection: SnowflakeConnection, uid):
    with connection.cursor() as cursor:
        query = f"SELECT color_ID FROM dim_color WHERE color_ID = %s"
        cursor.execute(query, (uid,))
        result = cursor.fetchone()
        return result[0] if result else None

def upload_to_snowflake_category(connection: SnowflakeConnection, data_frame, table_name):
    unique_categories = data_frame['category'].drop_duplicates().dropna()
    with connection.cursor() as cursor:
        for category in unique_categories:
            if not get_category_id(connection, category):
                query = f"INSERT INTO {table_name} (category) VALUES (%s)"
                cursor.execute(query, (category,))

def upload_to_snowflake_color(connection: SnowflakeConnection, data_frame, table_name):
    unique_colors = data_frame.drop_duplicates(subset='UID')
    with connection.cursor() as cursor:
        for _, row in unique_colors.iterrows():
            if not get_color_id(connection, row['UID']):
                query = f"INSERT INTO {table_name} (color_ID, TopColor, color_Description, color_Label, color_Image_url, color_FullPrice, color_CurrentPrice, color_Discount, color_BestSeller, color_InStock, color_MemberExclusive, color_New) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (row['UID'], row['TopColor'], row['color-Description'], row['color-Label'], row['color-Image-url'], row['color-FullPrice'], row['color-CurrentPrice'], row['color-Discount'], row['color-BestSeller'], row['color-InStock'], row['color-MemberExclusive'], row['color-New']))

def upload_to_snowflake_products(connection: SnowflakeConnection, data_frame, table_name):
    with connection.cursor() as cursor:
        for _, row in data_frame.iterrows():
            category_id = get_category_id(connection, row['category'])
            color_id = get_color_id(connection, row['UID'])
            query = f"INSERT INTO {table_name} (UID, cloudProdID, productID, shortID, prebuildId, title, subtitle, prod_url, short_description, rating, currency, fullPrice, currentPrice, color_id, colorNum, category_id, type, channel, GiftCard, Jersey, Launch, MemberExclusive, NBA, NFL, Sustainable, customizable, ExtendedSizing, sale, label, inStock, ComingSoon, BestSeller, Excluded) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (row['UID'], row['cloudProdID'], row['productID'], row['shortID'], row['prebuildId'], row['title'], row['subtitle'], row['prod_url'], row['short_description'], row['rating'], row['currency'], row['fullPrice'], row['currentPrice'], color_id, row['colorNum'], category_id, row['type'], row['channel'], row['GiftCard'], row['Jersey'], row['Launch'], row['MemberExclusive'], row['NBA'], row['NFL'], row['Sustainable'], row['customizable'], row['ExtendedSizing'], row['sale'], row['label'], row['inStock'], row['ComingSoon'], row['BestSeller'], row['Excluded'])
            cursor.execute(query, values)

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
        data_frame.drop_duplicates(subset="date", inplace=True)
        data = data_frame[["date", "day", "month", "year"]].values.tolist()
        data = [row for row in data if not date_exists(connection, row[0])]
        if data:
            cursor.executemany(query, data)

def get_product_id_by_uid(connection: SnowflakeConnection, uid):
    with connection.cursor() as cursor:
        query = f"SELECT id FROM dim_products WHERE UID = %s"
        cursor.execute(query, (uid,))
        result = cursor.fetchone()
        return result[0] if result else None

def upload_to_snowflake_sales(connection: SnowflakeConnection, data_frame, table_name):
    with connection.cursor() as cursor:
        for index, row in data_frame.iterrows():
            product_id = get_product_id_by_uid(connection, row["UID"])
            date_id = get_date_id(connection, row["date"])
            query = f"INSERT INTO {table_name} (ticket_id, product_id, date_id, currency, sales, quantity) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (row["ticket_id"], product_id, date_id, row["currency"], row["sales"], row["quantity"])
            cursor.execute(query, values)

with connect(
        account="emb55035",
        user="jerryenroute",
        password="2MQP2.Se",
        database="NIKE_SALES",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
        region="us-west-2"
) as connection:
    # Process products_etl.csv
    product_df = pd.read_csv('./data/products_etl.csv')
    product_df = product_df.where(pd.notna(product_df), None)
    print("Uploading data to category table")
    upload_to_snowflake_category(connection, product_df, "dim_category")
    print("Uploading data to color table")
    upload_to_snowflake_color(connection, product_df, "dim_color")
    print("Uploading data to products table")
    upload_to_snowflake_products(connection, product_df, "dim_products")

    # Process sales_etl.csv
    sales_df = pd.read_csv('./data/sales_etl.csv')
    print("Uploading data to dates table")
    upload_to_snowflake_dates(connection, sales_df, "dim_dates")
    print("Uploading data to sales table")
    upload_to_snowflake_sales(connection, sales_df, "fact_sales")
    print("Upload to snowflae complete")
