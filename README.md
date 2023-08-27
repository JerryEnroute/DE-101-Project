# DE 101 Project | Nike data scrappe 2 DB

## Step 1

### Run the scrapper API

Run the nike scrapper API by following the [instructions](./scrapper/scrapper_instructions.md)

## Step 2

### Start docker container

Once the scrapper finishes, bring up the docker container by running the command:

```sh
docker-compose up -d
```

## Step 3

### ETL

- Go to http://localhost:8080/
  
- Login to Airflow with the credentials:
  
```txt
User: airflow
Password: airflow
```

- The dags should start running automatically; if not, start both manually.
  
## Step 4

### Create tables

- Login into your [Snowflake](https://www.snowflake.com) account.

- Create a database named `NIKE_SALES`.
  
- Create the tables by running the commands in the file [ddl.sql](reporting/ddl.sql) in a Snowflake worksheet.
  
## Step 5

### Upload the data to the tables

- Start the data warehouse connector environment:
  
```sh
cd data_warehouse
python3 -m venv dw_env
pip3 install -r requirements.txt
```

-Modify the connection params according to your account details in the [upload_snoflake.py](data_warehouse/upload_snowflake.py) file.

-Run the upload script:

```sh
python3 upload_snowflake.py
```

The table joins with their respective foreign key constraints will be handled by this script.

## Step 6

### Query the tables

In a new snowflake worksheet, run the commands in the file [queries.sql](reporting/queries.sql) inside the reporting directory.