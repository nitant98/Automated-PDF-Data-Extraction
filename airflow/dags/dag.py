from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Paths are now relative to the DAG file's location
base_scripts_dir = Path(__file__).parent / "Scripts/Pipeline_Scripts"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 1, 1),
}

dag = DAG(
    'grobid_processing',
    default_args=default_args,
    description='Process files with Grobid and further analysis',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Task to run grobid.sh
# run_grobid_sh = BashOperator(
#     task_id='run_grobid_sh',
#     bash_command=f'cd {base_scripts_dir}/Grobid && ./grobid.sh ',
#     dag=dag,
# )

# Task to run grobid2.sh
run_grobid2_sh = BashOperator(
    task_id='run_grobid2_sh',
    bash_command=f'cd {base_scripts_dir}/Grobid && ./grobid2.sh ',
    dag=dag,
)

# Task to install Python requirements
install_requirements = BashOperator(
    task_id='install_requirements',
    bash_command=f'pip install -r {base_scripts_dir}/requirements.txt',
    dag=dag,
)

# Define a Python callable for grobid_csv.py execution
def run_grobid_csv():
    scripts_path = str(base_scripts_dir)
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)

    # Now you can execute your script and it should find the Validation module
    grobid_csv_path = base_scripts_dir / 'grobid_csv.py'
    exec(open(grobid_csv_path).read(), globals())

# Task to run grobid_csv.py
run_grobid_csv_py = PythonOperator(
    task_id='run_grobid_csv_py',
    python_callable=run_grobid_csv,
    dag=dag,
)

# def run_snowflake_transfer():
#     scripts_path = str(base_scripts_dir)
#     if scripts_path not in sys.path:
#         sys.path.insert(0, scripts_path)

#     # Log the environment variables
#     print("Environment variables inside the Airflow task:")
#     for key, value in os.environ.items():
#         print(f"{key}: {value}")
    
#     snowflake_path = base_scripts_dir / 'SnowflakeTransfer/snowflake_transfer.py'
#     exec(open(snowflake_path).read(), globals())

# # Task to run grobid_csv.py
# run_snowflake_transfer_py = PythonOperator(
#     task_id='run_snowflake_transfer_py',
#     python_callable=run_snowflake_transfer,
#     dag=dag,
# )

# run_grobid_csv_py = BashOperator(
#     task_id='run_grobid_csv_py',
#     bash_command=f'python {base_scripts_dir / "grobid_csv.py"} ',
#     dag=dag,
# )

# Task to run snowflake_transfer.py using BashOperator
run_snowflake_transfer_py = BashOperator(
    task_id='run_snowflake_transfer_py',
    bash_command=f'python {base_scripts_dir / "SnowflakeTransfer/snowflake_transfer.py"} ',
    dag=dag,
)
# Define dependencies
run_grobid2_sh >> install_requirements >> run_grobid_csv_py >> run_snowflake_transfer_py
