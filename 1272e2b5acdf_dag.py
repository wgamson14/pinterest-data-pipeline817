from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta

notebook_task = {
    'notebook_path': '/Users/willgamson00@gmail.com/cleaning_posts' 
}

default_args = {
    'owner': 'Will',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('1272e2b5acdf_dag',
         start_date=datetime(2023, 12, 13),
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
         ) as dag:
    
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='mwaa-dags-bucket',
        notebook_task=notebook_task
    )
    opr_submit_run
