import os
from datetime import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


"""
DAG: traffic_pipeline

Purpose:
- Real-time traffic data collection
- End-to-end pipeline: S3 → Snowflake → dbt → Superset
- Deliver analytics-ready datasets with automated data quality checks

System Architecture:
1. ITS OpenAPI: Periodic raw traffic data collection
2. PythonOperator: Fetching and initial transformation
3. AWS S3: Store raw data (partitioned by date)
4. Snowflake: ETL & data modeling
5. dbt: Transform tables & run tests
6. Superset: Visualize Chart

Pipeline Flow:
- ITS API → PythonOperator(Fetch & Transform) → S3 → Snowflake → dbt(Run & Test) → Superset
"""

def fetch_and_process(**context):
    api_key = Variable.get("ITS_API_KEY")
    url = f"https://openapi.its.go.kr:9443/eventInfo?apiKey={api_key}&type=all&eventType=all&getType=json"

    res = requests.get(url)
    res.raise_for_status()  # 상태 코드 간결 체크

    events = res.json().get("body", {}).get("items", [])
    if not events:
        raise ValueError("API에서 이벤트 데이터를 가져오지 못했어요!")

    df = pd.DataFrame(events)
    df = df.astype(str)
    ingestion_date = datetime.now().strftime("%Y-%m-%d")
    
    # 원본 CSV 저장 (Airflow 서버 or 컨테이너 내 원본 보존)
    csv_file_path = f"/tmp/events_{ingestion_date}.csv"
    df.to_csv(csv_file_path, index=False)
    
    # Parquet 변환 저장
    parquet_file_path = f"/tmp/events_{ingestion_date}.parquet"
    df.to_parquet(parquet_file_path, index=False)
    #df.to_parquet(parquet_file_path, index=False, engine="pyarrow")
    # XCom에 Parquet 경로만 전달
    context["ti"].xcom_push(key="file_path", value=parquet_file_path)



def upload_to_s3_partitioned(**context):
    file_path = context["ti"].xcom_pull(key="file_path")
    print(f"[DEBUG] Uploading file: {file_path}, exists: {os.path.exists(file_path)}")
    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path}가 존재하지 않음")

    s3_key = f"rawdata/traffic_event/ingestion_date={context['ds']}/event_info.parquet"
    S3Hook("aws_default").load_file(
        filename=file_path,
        key=s3_key,
        bucket_name="traffic-s3-team4",
        replace=True  # overwrite
    )


with DAG(
    dag_id="traffic_pipeline_s3_snowflake_folder_partitioning_parquet",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    doc_md="""
    ## 교통 이벤트 파이프라인 DAG
    1. ITS OpenAPI 데이터 수집 -> CSV/Parquet
    2. S3 업로드
    3. Snowflake 테이블 생성/적재
    4. dbt Run & Test -> Superset 시각화
    """
) as dag:
    
    t1_fetch = PythonOperator(
        task_id="fetch_and_process",
        python_callable=fetch_and_process,
        doc_md="**ITS API 데이터 수집 및 DataFrame 전처리**"
    )

    t2_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3_partitioned,
        doc_md="**CSV 파일을 S3에 업로드**"
    )

    t3_create_table = SnowflakeOperator(
    task_id="create_snowflake_table",
    snowflake_conn_id="sf_conn",
    sql="""
        CREATE TABLE IF NOT EXISTS TRAFFIC_DB.RAW_DATA.TRAFFIC_EVENT_FOLDER_PARTITIONING_PARQUET_SOOJIN (
            type VARCHAR,                -- 고속도로, 일반도로 등
            eventType VARCHAR,           -- 공사, 기타돌발 등
            eventDetailType VARCHAR,     -- 작업, 고장, 이벤트/홍보 등
            startDate VARCHAR,           -- YYYYMMDDHHMMSS
            coordX FLOAT,                -- 경도
            coordY FLOAT,                -- 위도
            linkId BIGINT,               -- 링크 ID
            roadName VARCHAR,            -- 제2경인선, 영동선 등
            roadNo BIGINT,               -- 110, 50 등
            roadDrcType VARCHAR,         -- 기점, 종점 등
            lanesBlockType VARCHAR,      -- (현재 샘플 빈값 많음)
            lanesBlocked VARCHAR,        -- "1차로 차단", "4차로 차단" 등
            message VARCHAR,             -- 공사 메시지 등
            endDate VARCHAR,             -- YYYYMMDDHHMMSS
            ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
    """,
    doc_md="**Snowflake 테이블 생성: 컬럼 정의, 원본 보존**"
    )

    t4_copy_into = SnowflakeOperator(
    task_id="copy_into_snowflake",
    snowflake_conn_id="sf_conn",
    sql="""
        COPY INTO TRAFFIC_DB.RAW_DATA.TRAFFIC_EVENT_FOLDER_PARTITIONING_PARQUET_SOOJIN
        FROM 's3://traffic-s3-team4/rawdata/traffic_event/ingestion_date={{ ds }}/'
        CREDENTIALS=(
            AWS_KEY_ID='{{ conn.aws_default.login }}'
            AWS_SECRET_KEY='{{ conn.aws_default.password }}'
        )
        FILE_FORMAT=(TYPE='PARQUET')
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        -- ON_ERROR='ABORT_STATEMENT';
        ON_ERROR='CONTINUE';
        """,
    doc_md="**S3 Parquet → Snowflake COPY, 원본 보존**",
    )

    t4_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt/my_project && dbt run && dbt test",
        doc_md="**dbt를 이용해 Transform Table 검증 및 문서화 수행**"
    )


    t1_fetch >> t2_s3 >> t3_create_table >> t4_copy_into >> t4_dbt_run
