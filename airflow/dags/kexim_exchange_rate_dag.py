from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import pendulum

# 분리한 scripts 폴더에서 함수 불러오기
from scripts.extract_kexim import fetch_and_upload_to_s3

# 한국 시간대 설정
local_tz = pendulum.timezone("Asia/Seoul")

# 설정 정보
DBT_PROJECT_DIR = "/opt/airflow/dbt/public_data_mart"
SNOWFLAKE_CONN_ID = 'snowflake_default' # Airflow UI에 등록할 ID

default_args = {
    'owner': 'InHwan Cho',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='kexim_exchange_rate_elt_pipeline',
    # start_date에 한국 시간대를 적용해야 schedule도 한국 시간 기준으로 작동합니다.
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    schedule='0 12 * * *',  # 이제 한국 시간 낮 12시를 의미합니다.
    catchup=False,
    default_args=default_args
) as dag:

    # [1단계: E] API 호출 후 S3 적재
    extract_task = PythonOperator(
        task_id='E_extract_to_s3',
        python_callable=fetch_and_upload_to_s3
    )

    # [2단계: L] Snowflake Bronze 적재
    # Snowflake UI에서 쓴 쿼리 중 매일 반복되어야 하는 부분만 넣습니다.
    load_to_bronze = SQLExecuteQueryOperator(
        task_id='L_load_s3_to_snowflake',
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE ROLE SYSADMIN;
            USE WAREHOUSE COMPUTE_WH;
            USE SCHEMA SANDBOX.BRONZE;

            -- 1. Stage의 파일 목록 새로고침
            ALTER STAGE my_s3_stage REFRESH;

            -- 2. 테이블 생성 (search_date의 DEFAULT는 제거하고 직접 insert하도록 구성)
            CREATE TABLE IF NOT EXISTS raw_exchange_rate (
                raw_data VARIANT,
                loaded_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('UTC', 'Asia/Seoul', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
                search_date VARCHAR2(8)
            );

            -- 3. 파일명에서 날짜 추출하여 적재 (Transformation during COPY)
            COPY INTO raw_exchange_rate (raw_data, search_date)
            FROM (
              SELECT 
                $1, -- 파일의 전체 내용(JSON)
                REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]{8})'), -- 파일명에서 숫자 8자리 추출
              FROM @my_s3_stage
            )
            PATTERN='.*exchange_rate_.*.json';
        """
    )

    # [3단계: T] dbt를 이용한 변환 및 검증
    dbt_run = BashOperator(
        task_id='T_dbt_run',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
        task_id='T_dbt_test',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}"
    )

    # 파이프라인 순서: 추출 -> 적재 -> 변환 -> 검증
    extract_task >> load_to_bronze >> dbt_run >> dbt_test