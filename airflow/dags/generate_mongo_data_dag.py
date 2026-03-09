from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="generate_mongo_data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mongo", "generate"],
)

generate_data = BashOperator(
    task_id="generate_data_in_mongo",
    bash_command="python /opt/airflow/scripts/generate_mongo_data.py",
    dag=dag,
)