import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "gold_realestate_aggregate"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# =========================================================
# PostgreSQL 5개 테이블 검증
# =========================================================

def validate_gold_tables():

    hook = PostgresHook(
        postgres_conn_id="postgres_default"
    )

    tables = [
        "gold_realestate_district_avg",
        "gold_realestate_top10",
        "gold_realestate_size_dist",
        "gold_realestate_age_avg",
        "gold_realestate_mom_change",
    ]

    for table in tables:

        result = hook.get_first(
            f"SELECT COUNT(*) FROM {table}"
        )

        count = result[0]

        print(
            f"{table} row count = {count}"
        )

        if count <= 0:
            raise ValueError(
                f"{table} has no rows"
            )


with DAG(
    dag_id=DAG_ID,

    default_args=default_args,

    start_date=datetime(
        2026,
        1,
        1,
    ),

    schedule="@monthly",

    catchup=True,

    max_active_runs=1,

    tags=[
        "q3",
        "realestate",
        "gold",
        "spark",
        "postgres",
    ],

) as dag:

    # =====================================================
    # Q2 완료 대기
    # =====================================================

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",

        external_dag_id=(
            "silver_realestate_transform"
        ),

        external_task_id=None,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],

        poke_interval=10,
        timeout=600,
        mode="poke",
    )


    # =====================================================
    # Spark Gold 집계
    # =====================================================

    run_gold_spark = BashOperator(
        task_id="run_gold_spark",

        env={
            "AWS_ACCESS_KEY_ID":
                os.getenv(
                    "AWS_ACCESS_KEY_ID",
                    ""
                ),

            "AWS_SECRET_ACCESS_KEY":
                os.getenv(
                    "AWS_SECRET_ACCESS_KEY",
                    ""
                ),

            "AWS_DEFAULT_REGION":
                os.getenv(
                    "AWS_DEFAULT_REGION",
                    "ap-northeast-2"
                ),

            "POSTGRES_HOST":
                "postgres",

            "POSTGRES_PORT":
                "5432",

            "POSTGRES_DB":
                "airflow",

            "POSTGRES_USER":
                "airflow",

            "POSTGRES_PASSWORD":
                "airflow",
        },

        append_env=True,

        bash_command="""
        set -e

        echo "=========================================="
        echo "Q3 Gold Spark Job Start"
        echo "=========================================="

        spark-submit \
          --master local[*] \
          /opt/airflow/scripts/gold_spark_sql.py

        echo "=========================================="
        echo "Q3 Gold Spark Job Finished"
        echo "=========================================="
        """,
    )


    # =====================================================
    # 적재 후 5개 Gold 테이블 검증
    #
    # PostgresHook 사용
    # =====================================================

    from airflow.operators.python import PythonOperator

    validate_tables = PythonOperator(
        task_id="validate_gold_tables",
        python_callable=validate_gold_tables,
    )


    wait_for_silver >> run_gold_spark >> validate_tables