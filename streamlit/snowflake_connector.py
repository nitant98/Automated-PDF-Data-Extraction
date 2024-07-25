import streamlit as st
import snowflake.connector
import os
import pandas as pd
from dotenv import load_dotenv

class SnowflakeConnector:
    
    def __init__(self, user, password, account, warehouse, database, schema):
            self.conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
         
            )

    def execute_sql(self, sql):
        
            cursor = self.conn.cursor()
            cursor.execute(sql)
            # Fetch all rows from the result set
            rows = cursor.fetchall()
            # Get column names from cursor description
            columns = [x[0] for x in cursor.description]
            cursor.close()
            # Create DataFrame from fetched rows and column names
            df = pd.DataFrame(rows, columns=columns)
            return df


def load_env():
    load_dotenv()
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
   
    return user, password, account, warehouse, database, schema

# Load environment variables
user, password, account, warehouse, database, schema = load_env()

# Initialize Snowflake connector
snowflake = SnowflakeConnector(user, password, account, warehouse, database, schema)