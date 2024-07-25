import streamlit as st
import boto3
import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials from environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID_ALL')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY_ALL')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Create S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# FastAPI service URL
FASTAPI_URL = "http://fastapi:8095"
# Snowflake API service URL
SNOWFLAKE_API_URL = "http://localhost:8001"

def upload_to_s3(file_content, file_name):
    try:
        # Check if the file already exists
        try:
            s3.head_object(Bucket=S3_BUCKET_NAME, Key=file_name)
            st.warning(f"File '{file_name}' already exists in the S3 bucket and will be replaced.")
        except s3.exceptions.ClientError as e:
            # If a client error is thrown, then check if it was a 404 error.
            # If it was a 404 error, then the object does not exist.
            if e.response['Error']['Code'] == "404":
                st.info(f"File '{file_name}' does not exist in the S3 bucket. It will be uploaded.")
            else:
                # If the error was something else, then re-raise the error.
                raise

        # Upload file content to S3 bucket
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=file_name, Body=file_content)
        st.success("File uploaded successfully to S3!")
    except Exception as e:
        st.error(f"Failed to upload file to S3. Error: {e}")

def main():
    st.title("PDF File Uploader to S3")

    # File uploader widget
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")

    if uploaded_file is not None:
        st.success("File selected successfully!")

        # Upload the file to S3
        upload_to_s3(uploaded_file.getvalue(), uploaded_file.name)

        # Trigger Airflow pipeline if file uploaded successfully
        if st.button("Trigger Pipeline"):
            # Define payload here, inside the button's if statement
            payload = {
                "dag_id": "grobid_processing",  # Replace with your actual DAG ID
                "conf": {}
            }
            # Trigger DAG via FastAPI
            response = requests.post(f"{FASTAPI_URL}/trigger-airflow-dag", json=payload)
            if response.status_code in [200, 201]:
                st.success("Pipeline triggered successfully!")
            else:
                st.error(f"Error triggering pipeline: {response.text}")

        # Invoke Snowflake API service to bring back results
               
        '''if st.button("Fetch Results from Snowflake"):
            query = "SELECT * FROM your_table"
            response = requests.get(f"{SNOWFLAKE_API_URL}/execute_query", params={"query": query})
            if response.status_code == 200:
                data = response.json()
                if data["status"] == "success":
                    results = data["data"]
                    st.write("Query Results:")
                    st.write(results)
                else:
                    st.error(f"Error executing query: {data['message']}")
            else:
                st.error("Error connecting to Snowflake API")

        # Dummy button
        if st.button("Dummy Button"):
            st.write("You clicked the Dummy Button!")
        '''
if __name__ == "__main__":
    main()