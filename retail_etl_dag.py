import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the parent directory to Python path
dag_dir = os.path.dirname(os.path.abspath(__file__))
airflow_dir = os.path.dirname(dag_dir)
sys.path.append(airflow_dir)

from utils.etl_functions import (
    check_s3_files,
    run_glue_crawler,
    load_to_staging,
    transform_to_dim_fact,
    unload_to_processed
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'retail_etl',
    default_args=default_args,
    description='Retail ETL Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Create tasks
check_files_task = PythonOperator(
    task_id='check_s3_files',
    python_callable=check_s3_files,
    provide_context=True,
    dag=dag
)

glue_crawler_task = PythonOperator(
    task_id='run_glue_crawler',
    python_callable=run_glue_crawler,
    provide_context=True,
    dag=dag
)

load_staging_task = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_to_dim_fact',
    python_callable=transform_to_dim_fact,
    provide_context=True,
    dag=dag
)

unload_task = PythonOperator(
    task_id='unload_to_processed',
    python_callable=unload_to_processed,
    provide_context=True,
    dag=dag
)

# Set dependencies
check_files_task >> glue_crawler_task >> load_staging_task >> transform_task >> unload_task