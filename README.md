
## Assignment 4
Automated PDF Data Extraction and Querying Pipeline with Airflow and Snowflake Integration


## Problem Statement:

Develop an end-to-end pipeline utilizing Airflow to automate the extraction and storage of meta-data and content from PDF files into Snowflake. The task involves building two API services using FastAPI: one to trigger the Airflow pipeline and another to interface with Snowflake for querying.


## Project Goals

  1.Build a FastAPI service to accept S3 file locations and initiate an Airflow pipeline for:
    a. Extraction of data and metadata from PDF files.
    b. Validating the extracted data using predefined tools.
    c. Loading the data and metadata into Snowflake.

  2.Develop a separate FastAPI service to interact with Snowflake and provide query responses.


## Codelab

[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=1GaUW9ixS5DoZZtLuGraSBG1kyH8JeJ18ZtBA3PeZngo#4)

[Demo](https://www.youtube.com/watch?v=Aocn1MS2RkA)

## Technologies Used

[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-387BC3?style=for-the-badge&logo=snowflake&logoColor=light)](https://www.snowflake.com/)
[![Beautiful Soup](https://img.shields.io/badge/Beautiful%20Soup-59666C?style=for-the-badge&logo=python&logoColor=blue)](https://www.crummy.com/software/BeautifulSoup/)
[![Grobid](https://img.shields.io/badge/Grobid-007396?style=for-the-badge&logo=java&logoColor=white)](https://github.com/kermitt2/grobid)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Google Cloud Platform](https://img.shields.io/badge/Google%20Cloud%20Platform-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://www.streamlit.io/)
[![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)](https://aws.amazon.com/s3/)

## Project URLs

Airflow: http://34.75.0.13:8080/
FastAPI: http://34.75.0.13:8095/docs
Streamlit: http://34.75.0.13:8000/
Grobid: http://34.75.0.13:8070/


## Project Structure

```
├── Makefile
├── README.md
├── airflow
│   ├── Dockerfile
│   ├── config
│   ├── dags
│   │   ├── Scripts
│   │   │   ├── Pipeline_Scripts
│   │   │   │   ├── Grobid
│   │   │   │   │   ├── grobid.sh
│   │   │   │   │   ├── grobid2.sh
│   │   │   │   │   ├── grobid_process.py
│   │   │   │   │   ├── requirements.txt
│   │   │   │   │   ├── txt
│   │   │   │   │   │   ├── Grobid_2024-l1-topics-combined-2_combined.txt
│   │   │   │   │   │   ├── Grobid_2024-l2-topics-combined-2_combined.txt
│   │   │   │   │   │   └── Grobid_2024-l3-topics-combined-2_combined.txt
│   │   │   │   │   └── xml
│   │   │   │   │       ├── Grobid_2024-l1-topics-combined-2_combined.xml
│   │   │   │   │       ├── Grobid_2024-l2-topics-combined-2_combined.xml
│   │   │   │   │       └── Grobid_2024-l3-topics-combined-2_combined.xml
│   │   │   │   ├── PyPDF
│   │   │   │   │   ├── PyPDF
│   │   │   │   │   │   ├── 2024-l1-topics-combined-2.txt
│   │   │   │   │   │   ├── 2024-l2-topics-combined-2.txt
│   │   │   │   │   │   └── 2024-l3-topics-combined-2.txt
│   │   │   │   │   ├── pypdf.py
│   │   │   │   │   └── requirements.txt
│   │   │   │   ├── SnowflakeTransfer
│   │   │   │   │   └── snowflake_transfer.py
│   │   │   │   ├── __init__.py
│   │   │   │   ├── grobid_csv.py
│   │   │   │   ├── parsed_into_schema
│   │   │   │   │   ├── content
│   │   │   │   │   │   └── csv
│   │   │   │   │   │       ├── grobid_content_2024_l1_topics_combined_2.csv
│   │   │   │   │   │       ├── grobid_content_2024_l2_topics_combined_2.csv
│   │   │   │   │   │       └── grobid_content_2024_l3_topics_combined_2.csv
│   │   │   │   │   └── metadata
│   │   │   │   │       └── csv
│   │   │   │   │           ├── grobid_metadata_2024_l1_topics_combined_2.csv
│   │   │   │   │           ├── grobid_metadata_2024_l2_topics_combined_2.csv
│   │   │   │   │           └── grobid_metadata_2024_l3_topics_combined_2.csv
│   │   │   │   └── requirements.txt
│   │   │   ├── Validation.py
│   │   │   └── __init__.py
│   │   └── dag.py
│   ├── docker-compose.yaml
│   ├── logs
│   │   └── scheduler
│   │       ├── 2024-03-28
│   │       └── latest -> 2024-03-28
│   └── plugins
├── docker-compose-local.yaml
├── fastapi
│   ├── Dockerfile
│   ├── requirements.txt
│   └── restApi.py
└── streamlit
    ├── app.py
    ├── dockerfile
    ├── fetch_result.py
    ├── main.py
    ├── requirements.txt
    └── snowflake_connector.py

```


## Architectural Diagram

![image](https://github.com/BigDataIA-Spring2024-Sec1-Team4/Assignment4/assets/114356265/5f3cf4d9-03b1-4fa6-b291-82b5a0c91597)



## To run the application locally, follow these steps:

1. **Clone the Repository**: Clone the repository onto your local machine.

   ```bash
   git clone https://github.com/BigDataIA-Spring2024-Sec1-Team4/Assignment4
   ```

2. **Create a Virtual Environment**: Set up a virtual environment to isolate project dependencies.

   ```bash
   python -m venv venv
   ```

3. **Activate the Virtual Environment**: Activate the virtual environment.

   - **Windows**:

     ```bash
     venv\Scripts\activate
     ```

   - **Unix or MacOS**:

     ```bash
     source venv/bin/activate
     ```

4. **Run MakeFile to start Docker Compose**: Start the Docker containers using Docker Compose.

   ```bash
   cd Assignment4
   make build-up
   ```

5. **Access Streamlit Interface**: Open your web browser and go to `34.75.0.13:8000` to access the Streamlit interface.

6. **Upload PDF to S3**: On the Streamlit homepage, upload a PDF file to S3. After successful upload, trigger the Airflow pipeline.

7. **Fetch Results**: Navigate to the "Fetch Result" page on the Streamlit interface. Select a table from which you want to retrieve data from Snowflake. Write a prompt and click on "Generate SQL Query". Review the generated SQL query and edit if necessary. Finally, click on "Execute Query" to retrieve the desired data from the Snowflake table.

By following these steps, you should be able to run the application locally and interact with it using the provided Streamlit interface to upload PDF files, trigger data processing pipelines, and query Snowflake for results.


## Team Information and Contribution 

Name           | NUID          |
---------------|---------------|
Anirudh Joshi  | 002991365     |      
Nitant Jatale  | 002776669     |      
Rutuja More    | 00272782      |      
