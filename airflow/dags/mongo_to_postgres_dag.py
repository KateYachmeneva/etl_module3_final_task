from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="mongo_to_postgres_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mongo", "postgres"],
)

create_schema = BashOperator(
    task_id="create_postgres_schema",
    bash_command="python /opt/airflow/scripts/init_postgres.py",
    dag=dag,
)

load_sessions = BashOperator(
    task_id="load_user_sessions",
    bash_command="python /opt/airflow/scripts/mongo_to_postgres_etl.py load_user_sessions",
    dag=dag,
)

load_events = BashOperator(
    task_id="load_event_logs",
    bash_command="python /opt/airflow/scripts/mongo_to_postgres_etl.py load_event_logs",
    dag=dag,
)

load_tickets = BashOperator(
    task_id="load_support_tickets",
    bash_command="python /opt/airflow/scripts/mongo_to_postgres_etl.py load_support_tickets",
    dag=dag,
)
quality_checks = BashOperator(
    task_id="run_data_quality_checks",
    bash_command="python /opt/airflow/scripts/data_quality_checks.py",
    dag=dag,
)

create_schema >> load_sessions >> load_events >> load_tickets >> quality_checks