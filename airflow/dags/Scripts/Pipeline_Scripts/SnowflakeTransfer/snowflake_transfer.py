import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path

# Load environment variables
dotenv_path = Path(__file__).parent / '.env'
# Load environment variables
load_dotenv(dotenv_path=dotenv_path)

# Establish a connection to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    # The following parameters are placeholders and should be set as per your Snowflake setup.
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    role= 'SYSADMIN',
)

# Function to create a warehouse if it doesn't exist
def create_warehouse_if_not_exists(conn, warehouse_name):
    conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")

# Function to create a database if it doesn't exist
def create_database_if_not_exists(conn, database_name):
    conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# Function to create a schema if it doesn't exist
def create_schema_if_not_exists(conn, schema_name):
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Call the functions to ensure warehouse, database, and schema exist
create_warehouse_if_not_exists(conn, os.getenv('SNOWFLAKE_WAREHOUSE'))
create_database_if_not_exists(conn, os.getenv('SNOWFLAKE_DATABASE'))
create_schema_if_not_exists(conn, os.getenv('SNOWFLAKE_SCHEMA'))

# Function to map pandas data types to Snowflake SQL types
def pandas_dtype_to_snowflake_sql_type(dtype):
    mapping = {
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP_NTZ',
        'object': 'VARCHAR'
    }
    return mapping.get(str(dtype), 'VARCHAR')

# Function to dynamically create tables based on DataFrame's structure
def create_table_from_df(df, table_name, conn):
    column_definitions = ', '.join([f'"{col.upper()}" {pandas_dtype_to_snowflake_sql_type(str(dtype))}' for col, dtype in df.dtypes.items()])
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ({column_definitions})"
    conn.cursor().execute(create_table_sql)

# Function to upload a CSV file to Snowflake
def upload_csv_to_snowflake(csv_path, table_name, conn):
    df = pd.read_csv(csv_path)
    df.columns = [col.upper() for col in df.columns]
    create_table_from_df(df, table_name, conn)
    write_pandas(conn, df, table_name.upper())
    print("Data Transfer Completed for table:", table_name)

if __name__ == "__main__":
    # project_root = Path(__file__).parent.parent
    project_root = Path(__file__).parents[2]

    # Paths to CSV files
    csv_files = {
        'grobid_content_2024_l1_topics_combined_2': project_root / 'Pipeline_Scripts/parsed_into_schema/content/csv/grobid_content_2024_l1_topics_combined_2.csv',
        'grobid_content_2024_l2_topics_combined_2': project_root / 'Pipeline_Scripts/parsed_into_schema/content/csv' / 'grobid_content_2024_l2_topics_combined_2.csv',
        'grobid_content_2024_l3_topics_combined_2': project_root / 'Pipeline_Scripts/parsed_into_schema/content/csv' / 'grobid_content_2024_l3_topics_combined_2.csv',
        'grobid_metadata_2024_l1_topics_combined_2': project_root / 'Pipeline_Scripts/parsed_into_schema/metadata/csv' / 'grobid_metadata_2024_l1_topics_combined_2.csv',
        'grobid_metadata_2024_l2_topics_combined_2': project_root / 'Pipeline_Scripts/parsed_into_schema/metadata/csv' / 'grobid_metadata_2024_l2_topics_combined_2.csv',
        'grobid_metadata_2024_l3_topics_combined_2': project_root / 'Pipeline_Scripts/parsed_into_schema/metadata/csv' / 'grobid_metadata_2024_l3_topics_combined_2.csv',
        # ...
    }

    # Process each CSV file
    for table_name, csv_file_path in csv_files.items():
        if csv_file_path.exists():
            upload_csv_to_snowflake(csv_file_path, table_name, conn)
            # print("Data Transfer Completed")
        else:
            print(f"File not found: {csv_file_path}")

    # Close the Snowflake connection
    conn.close()
