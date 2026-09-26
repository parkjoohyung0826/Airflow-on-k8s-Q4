import os

from datetime import datetime, timedelta

from airflow import DAG

from airflow.sensors.external_task import (
    ExternalTaskSensor,
)

from airflow.operators.bash import (
    BashOperator,
)


DAG_ID = "silver_realestate_transform"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(
        minutes=1
    ),
}


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
        "q2",
        "realestate",
        "silver",
        "spark",
    ],

) as dag:

    # =====================================================
    # Q1 완료 대기
    # =====================================================

    wait_for_bronze = ExternalTaskSensor(

        task_id="wait_for_bronze",

        external_dag_id=(
            "bronze_realestate_collect"
        ),

        external_task_id=None,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],

        poke_interval=30,

        timeout=600,

        mode="poke",
    )


    # =====================================================
    # Spark Job
    #
    # PythonOperator 사용 금지
    #
    # 반드시 spark-submit
    # =====================================================

    run_silver_spark = BashOperator(

        task_id="run_silver_spark",

        env={
            "TARGET_YYYYMM":
                "{{ logical_date.strftime('%Y%m') }}",

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
        },

        append_env=True,

        bash_command="""
        spark-submit \
          --master local[*] \
          --conf spark.ui.port=4040 \
          /opt/airflow/scripts/silver_spark.py
        """,
    )


    wait_for_bronze >> run_silver_spark