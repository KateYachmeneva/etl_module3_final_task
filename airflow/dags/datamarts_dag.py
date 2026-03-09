from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="build_datamarts",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "datamarts", "postgres"],
)

build_user_activity = BashOperator(
    task_id="build_user_activity_datamart",
    bash_command="python /opt/airflow/scripts/build_datamarts.py build_user_activity",
    dag=dag,
)

build_support_efficiency = BashOperator(
    task_id="build_support_efficiency_datamart",
    bash_command="python /opt/airflow/scripts/build_datamarts.py build_support_efficiency",
    dag=dag,
)
build_popular_pages = BashOperator(
    task_id="build_popular_pages_datamart",
    bash_command="python /opt/airflow/scripts/build_datamarts.py build_popular_pages",
    dag=dag,
)

build_user_activity >> build_support_efficiency >> build_popular_pages