import os
import boto3
import requests
import xml.etree.ElementTree as ET

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


DAG_ID = "bronze_realestate_collect"

BUCKET_NAME = "realestate-parkjoohyung"

API_URL = (
    "https://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)

LAWD_CODES = [
    "11680",
    "11650",
    "11710",
    "11440",
    "11170",
    "11200",
]


def collect_realestate(lawd_cd, **context):

    # 과제 필수 출력
    print(
        f"collector=박주형, "
        f"time={datetime.now()}, "
        f"lawd={lawd_cd}"
    )

    service_key = os.getenv("DATA_GO_KR_SERVICE_KEY")

    if not service_key:
        print("DATA_GO_KR_SERVICE_KEY is missing")
        return False

    logical_date = context["logical_date"]
    deal_ymd = logical_date.strftime("%Y%m")

    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": "1",
        "numOfRows": "1000",
    }

    try:
        response = requests.get(
            API_URL,
            params=params,
            timeout=30,
        )

        print(f"HTTP status={response.status_code}")

        if response.status_code != 200:
            return False

        xml_data = response.text

        # XML 파싱 검증
        root = ET.fromstring(xml_data)

        result_code = root.findtext(".//resultCode")
        result_msg = root.findtext(".//resultMsg")

        print(
            f"resultCode={result_code}, "
            f"resultMsg={result_msg}"
        )

        if result_code not in ("000", "00"):
            return False

        items = root.findall(".//item")

        print(
            f"LAWD_CD={lawd_cd}, "
            f"item_count={len(items)}"
        )

        # 응답 0건이면 분기 대상으로 처리
        if len(items) == 0:
            return False

        s3_key = (
            f"bronze/"
            f"{deal_ymd}/"
            f"{lawd_cd}.xml"
        )

        s3 = boto3.client("s3")

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=response.content,
            ContentType="application/xml",
        )

        print(
            f"uploaded="
            f"s3://{BUCKET_NAME}/{s3_key}"
        )

        return True

    except ET.ParseError as e:
        print(f"XML parsing failed: {e}")
        return False

    except Exception as e:
        print(f"collect failed: {e}")
        return False


def branch_after_collect(**context):

    ti = context["ti"]

    results = []

    for code in LAWD_CODES:

        result = ti.xcom_pull(
            task_ids=f"collect_group.collect_{code}"
        )

        results.append(result)

    print(f"results={results}")

    if all(result is True for result in results):
        return "summary_done"

    return "skip_upload"


with DAG(
    dag_id=DAG_ID,

    start_date=datetime(2026, 1, 1),

    schedule="@monthly",

    catchup=True,

    max_active_runs=1,

    tags=[
        "q1",
        "realestate",
        "bronze",
    ],
) as dag:

    with TaskGroup(
        group_id="collect_group"
    ) as collect_group:

        for lawd_cd in LAWD_CODES:

            PythonOperator(
                task_id=f"collect_{lawd_cd}",

                python_callable=collect_realestate,

                op_kwargs={
                    "lawd_cd": lawd_cd
                },
            )

    branch = BranchPythonOperator(
        task_id="branch_after_collect",

        python_callable=branch_after_collect,

        trigger_rule=TriggerRule.ALL_DONE,
    )

    summary_done = EmptyOperator(
        task_id="summary_done"
    )

    skip_upload = EmptyOperator(
        task_id="skip_upload"
    )

    collect_group >> branch

    branch >> [
        summary_done,
        skip_upload,
    ]