from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="partition_event_logs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "partitioning", "postgres"],
)

create_partitioned_event_logs = BashOperator(
    task_id="create_partitioned_event_logs",
    bash_command="python /opt/airflow/scripts/create_partitioned_event_logs.py",
    dag=dag,
)