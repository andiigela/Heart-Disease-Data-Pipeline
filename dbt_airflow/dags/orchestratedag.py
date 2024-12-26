from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def trigger_dlt_pipeline():
    url = "https://dbc-b2b4296e-0cfb.cloud.databricks.com/api/2.0/pipelines/990ccc63-9702-4837-b1e6-e986ee7c716e/start"
    headers = {
        "Authorization": "Bearer dapi8160591b38569d886fb54da1a037b735",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to trigger DLT pipeline: {response.text}")

with DAG(
    dag_id='databricks_dbt_orchestration',
    description='Orchestrate Databricks DLT and dbt',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust as needed
    start_date=datetime(2024, 12, 4),
    catchup=False,
) as dag:
    
    trigger_dlt_task = PythonOperator(
        task_id='trigger_dlt_task',
        python_callable=trigger_dlt_pipeline
    )
    
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run',
        env={
            'DBT_PROFILES_DIR': '/usr/local/airflow/dbt/profiles',  # Ensure this points to your profiles.yml directory
        },
        cwd='/usr/local/airflow/dbt/mydbt_mydbc',
    )
    
    # Correct dependency setup
    trigger_dlt_task >> run_dbt_models
