from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import sys, os

# scripts 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from fetch_traffic_csv import main as fetch_main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_its_event",
    default_args=default_args,
    schedule_interval="*/30 * * * *",  # 30분마다 실행
    start_date=datetime(2025, 11, 17),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1️⃣ CSV 수집 + S3 업로드
    fetch_csv_task = PythonOperator(
        task_id="fetch_and_upload_csv",
        python_callable=fetch_main
    )

    # 2️⃣ Stage 생성 (없으면 생성)
    create_stage_task = SnowflakeOperator(
        task_id="create_stage",
        snowflake_conn_id="snowflake_default",
        sql="""
        CREATE OR REPLACE STAGE raw_event_info_stage
        URL='s3://traffic-s3-team4/rawdata/jihwan/'
        STORAGE_INTEGRATION = s3_int;
        """
    )

    # 3️⃣ Raw Table 생성 (없으면 생성)
    create_table_task = SnowflakeOperator(
        task_id="create_raw_table",
        snowflake_conn_id="snowflake_default",
        sql="""
        CREATE OR REPLACE TABLE raw_event_info_table_jihwan (
            type STRING,
            eventType STRING,
            eventDetailType STRING,
            startDate STRING,
            endDate STRING,
            coordX STRING,
            coordY STRING,
            linkId STRING,
            roadName STRING,
            roadNo STRING,
            roadDrcType STRING,
            lanesBlockType STRING,
            lanesBlocked STRING,
            message STRING
        );
        """
    )

    # 4️⃣ S3 → Snowflake Raw Table 적재
    load_s3_to_snowflake_task = SnowflakeOperator(
        task_id="load_s3_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
        COPY INTO raw_event_info_table_jihwan
        FROM @raw_event_info_stage
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """
    )

    # DAG 순서 정의
    fetch_csv_task >> create_stage_task >> create_table_task >> load_s3_to_snowflake_task
