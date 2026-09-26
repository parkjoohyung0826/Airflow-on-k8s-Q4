from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_gitsync",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["q4", "gitsync"],
) as dag:

    hello_task = BashOperator(
        task_id="hello_task",
        bash_command="""
        echo "Hello from GitSync"
        echo "name=박주형"
        echo "GitHub DAG sync success"
        """,
    )