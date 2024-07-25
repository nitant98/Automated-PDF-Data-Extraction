import streamlit as st
import requests
from dotenv import load_dotenv
import os
import openai
import pandas as pd

# Load environment variables
load_dotenv()

# Set your OpenAI API key
openai.api_key = os.getenv('openai_api_key')

# FastAPI service URL
FASTAPI_SERVICE_URL = os.getenv('FASTAPI_SERVICE_URL')  # Ensure this is in your .env file


# Function to get table names via FastAPI
def get_table_names():
    response = requests.get(f"{FASTAPI_SERVICE_URL}/snowflake/tables")
    if response.status_code == 200:
        return response.json()["tables"]
    else:
        st.error("Failed to fetch table names.")
        return []

# Function to display table data via FastAPI
def display_table_data(table_name):
    response = requests.get(f"{FASTAPI_SERVICE_URL}/snowflake/table/{table_name}")
    #st.error(table_name)
    if response.status_code in [200, 201]:
        try:
            data = response.json()
            df = pd.DataFrame(data['rows'], columns=data['columns'])
            
            st.write(f"All rows of {table_name}:")
            # Use st.dataframe to display the data with a scrolling window
            st.dataframe(df)
        except ValueError: 
            st.error("Failed to decode the response as JSON.")
    elif response.status_code == 500:
        st.error("Server error occurred.")
    else:
        st.error(f"Failed to fetch table data for {table_name}. Status code: {response.status_code}")

# Function to generate SQL query using OpenAI's API
def generate_sql_query(prompt, selected_table):
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",  # Ensure you use the correct model
        messages=[{"role": "system", "content": prompt + f" from {selected_table}"}],
        temperature=0.5,
        max_tokens=150,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0,
        stop=[";"]
    )
    sql_query = response.choices[0].message["content"].strip()
    return extract_sql_query(sql_query)

# Function to extract SQL query
def extract_sql_query(sql_query):
    lines = sql_query.split('\n')
    select_index = next((i for i, line in enumerate(lines) if "SELECT" in line), None)
    return '\n'.join(lines[select_index:]) if select_index is not None else None

def main():
    st.title("Snowflake Table Viewer")
    
    table_names = get_table_names()
    selected_table = st.selectbox("Select a table:", table_names)
    if selected_table:
        display_table_data(selected_table)

    prompt = st.text_area("Enter your prompt:", height=100)
    
    if st.button("Generate SQL"):
        sql_query = generate_sql_query(prompt, selected_table)
        if sql_query:
            st.write("Generated SQL Query:")
            st.code(sql_query, language="sql")
            st.session_state.generated_sql_query = sql_query
        else:
            st.error("Failed to generate a valid SQL query.")

    if st.button("Execute SQL"):
        if "generated_sql_query" in st.session_state:
            sql_query = st.session_state.generated_sql_query
            # Send the SQL query to the FastAPI endpoint for execution
            response = requests.post(f"{FASTAPI_SERVICE_URL}/snowflake/execute", json={"query": sql_query})
            if response.status_code in [200, 201]:
                data = response.json()
                st.write(f"Query Results:")
                df = pd.DataFrame(data["results"])
                st.dataframe(df)
            else:
                st.error("Failed to execute the query. Error: {}".format(response.text))

if __name__ == "__main__":
    main()
