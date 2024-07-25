from fastapi import FastAPI, HTTPException,  Request
import requests
from requests.auth import HTTPBasicAuth
from pydantic import BaseModel
from datetime import datetime
from dotenv import load_dotenv
from fastapi.exceptions import RequestValidationError
import os
import snowflake.connector

# Load environment variables from .env file
load_dotenv()

app = FastAPI()

airflow_endpoint = os.getenv('airflowurl')

# Define a request model to expect certain JSON payload structure
class TriggerDAGRequest(BaseModel):
    dag_id: str
    conf: dict = None  # Optional configuration for the DAG

# Endpoint to trigger an Airflow DAG
@app.post("/trigger-airflow-dag")
async def trigger_airflow_dag(request_data: TriggerDAGRequest):
    airflow_url = f"{airflow_endpoint}/api/v1/dags/{request_data.dag_id}/dagRuns"
    # airflow_url = f"http://localhost:8080/api/v1/dags/grobid_processing/dagRuns"
    airflow_username = os.getenv('AIRFLOW_USERNAME')
    airflow_password = os.getenv('AIRFLOW_PASSWORD')
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    unique_run_id = f"triggered_via_fastapi_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    payload = {
        "dag_run_id": unique_run_id,
        "conf": request_data.conf if request_data.conf else {},
    }
    
    response = requests.post(
        airflow_url, 
        json=payload, 
        headers=headers,
        auth=HTTPBasicAuth(airflow_username, airflow_password)
    )
    
    if response.status_code in [200, 201]:
        return {"message": "DAG triggered successfully", "details": response.json()}
    else:
        raise HTTPException(status_code=response.status_code, detail=response.text)


## Endpoints for Snowflake

# Initialize Snowflake connection
snowflake_ctx = snowflake.connector.connect(
user=os.getenv('SNOWFLAKE_USER'),
password=os.getenv('SNOWFLAKE_PASSWORD'),
account=os.getenv('SNOWFLAKE_ACCOUNT'),
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
database = os.getenv('SNOWFLAKE_DATABASE'),
schema = os.getenv('SNOWFLAKE_SCHEMA')
)

# Snowflake endpoint to get tables
@app.get("/snowflake/tables")
async def get_tables():
    with snowflake_ctx.cursor() as cur:
        cur.execute("SHOW TABLES;")
        tables = cur.fetchall()
        return {"tables": [table[1] for table in tables]}  # Adjust based on actual structure

# Snowflake endpoint to get table data

@app.get("/snowflake/table/{table_name}")
async def get_table_data(table_name: str):
    print(table_name)
    with snowflake_ctx.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name};")
        rows = cur.fetchall()
        print(rows)
        columns = [desc[0] for desc in cur.description]
        return {"columns": columns, "rows": rows}

# endpoint to execute SQL queries on Snowflake
@app.post("/snowflake/execute")
async def execute_query(request: Request):
    # Extract the SQL query from the request body
    body = await request.json()
    sql_query = body.get("query")

    if "DELETE" in sql_query or "DROP" in sql_query or "INSERT" in sql_query:
        raise HTTPException(status_code=400, detail="Query type not allowed")

    # Execute the query in Snowflake
    with snowflake_ctx.cursor() as cur:
        cur.execute(sql_query)
        rows = cur.fetchall()
        if not rows:
            return {"results": []}  
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in rows]
        return {"results": results}