from diagrams import Diagram, Cluster
from diagrams.saas.analytics import Snowflake
from diagrams.programming.language import Python
# from diagrams.saas.storage import S3
from diagrams.onprem.workflow import Airflow
from diagrams.custom import Custom
# from diagrams.saas.application import Streamlit

with Diagram("Updated Workflow Diagram", show=False, direction="LR"):
    with Cluster("Step 1 - User Uploads PDF"):
        user = Custom("User", "./user.png")  # Path to a user icon image file
        streamlit_app = Custom("Streamlit App","./streamlit.png")
        s3_storage = Custom("AWS S3 Bucket", "./s3.png")
        user >> streamlit_app >> s3_storage

    with Cluster("Step 2 - Trigger Pipeline"):
        fastapi = Custom("FastAPI","./fastapi.png")
        airflow_orchestrator = Airflow("Airflow Pipeline")
        s3_storage >> fastapi >> airflow_orchestrator

    with Cluster("Step 3 - PDF Parsing and Data Storage"):
        grobid_parser = Python("PDF Parser")
        parsed_data = Custom("Parsed Schema", "./schema.png")  # Path to a database icon image file
        snowflake_db = Snowflake("Snowflake")
        airflow_orchestrator >> grobid_parser >> parsed_data >> snowflake_db

    with Cluster("Step 4 - Fetch Results and Execute Queries"):
        query_api = Custom("QueryAPI","./fastapi.png")
        query_results = Custom("SQL Query Results", "./result.jpg")  # Path to a results icon image file
        snowflake_db >> query_api >> query_results

    with Cluster("Step 5 - OpenAI SQL Query Generation"):
        openai_tool = Custom("OpenAI", "./openai.png")  # Path to OpenAI icon image file
        sql_queries = Custom("SQL Queries", "./sql.jpg")  # Path to SQL icon image file
        query_api >> openai_tool >> sql_queries >> snowflake_db

