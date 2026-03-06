from airflow import DAG
# 2.x 버전 경로로 수정됨
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import pendulum
import requests
# 2.x 버전 경로로 수정됨
from airflow.models import Variable

from scripts.extract_kexim import fetch_and_upload_to_s3
from scripts.extract_etf import fetch_etf_and_upload_to_s3

def send_slack_alert(context):
    webhook_url = Variable.get("SLACK_WEBHOOK_URL")
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    state = ti.state
    logical_date = context.get('logical_date').strftime('%Y-%m-%d')
    log_url = ti.log_url

    if state == 'success':
        msg = f"✅ *[SUCCESS]* DAG: `{dag_id}` | Date: `{logical_date}` | Task: `{task_id}` 완료"
    else:
        msg = f"<@U09DTEKRBFZ>🚨 *[FAILED]* DAG: `{dag_id}` | Task: `{task_id}`\n🔍 <{log_url}|에러 로그 확인하기>"

    payload = {"text": msg}
    requests.post(webhook_url, json=payload)

local_tz = pendulum.timezone("Asia/Seoul")
DBT_PROJECT_DIR = "/opt/airflow/dbt/public_data_mart"
SNOWFLAKE_CONN_ID = 'snowflake_default'

default_args = {
    'owner': 'InHwan Cho',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback': send_slack_alert 
}

with DAG(
    dag_id='financial_data_elt_pipeline',
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    schedule='45 11 * * *',
    catchup=False,
    default_args=default_args
) as dag:

    # 1. 병렬 추출 태스크 (환율 & ETF)
    extract_kexim_task = PythonOperator(
        task_id='E_extract_kexim_to_s3',
        python_callable=fetch_and_upload_to_s3
    )

    extract_etf_task = PythonOperator(
        task_id='E_extract_etf_to_s3',
        python_callable=fetch_etf_and_upload_to_s3
    )

    # 2. Snowflake 적재 태스크 (환율)
    load_kexim_to_bronze = SQLExecuteQueryOperator(
        task_id='L_load_kexim_to_snowflake',
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE ROLE SYSADMIN;
            USE WAREHOUSE COMPUTE_WH;
            USE SCHEMA SANDBOX.BRONZE;

            ALTER STAGE my_s3_stage REFRESH;

            -- 증분 적재를 위해 기존 데이터 보존 (IF NOT EXISTS)
            CREATE TABLE IF NOT EXISTS raw_exchange_rate (
                raw_data VARIANT,
                loaded_at TIMESTAMP_TZ DEFAULT CONVERT_TIMEZONE('UTC', current_timestamp()),
                search_date VARCHAR2(8)
            );

            COPY INTO raw_exchange_rate (raw_data, search_date)
            FROM (
              SELECT $1, REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]{8})') 
              FROM @my_s3_stage
            )
            PATTERN='.*/exchange_rate/.*exchange_rate_{{ ds_nodash }}\.json';
        """
    )

    # 3. Snowflake 적재 태스크 (ETF)
    load_etf_to_bronze = SQLExecuteQueryOperator(
        task_id='L_load_etf_to_snowflake',
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE ROLE SYSADMIN;
            USE WAREHOUSE COMPUTE_WH;
            USE SCHEMA SANDBOX.BRONZE;

            ALTER STAGE my_s3_stage REFRESH;

            CREATE TABLE IF NOT EXISTS raw_etf (
                raw_data VARIANT,
                loaded_at TIMESTAMP_TZ DEFAULT CONVERT_TIMEZONE('UTC', current_timestamp()),
                search_date VARCHAR2(8)
            );

            COPY INTO raw_etf (raw_data, search_date)
            FROM (
              SELECT $1, REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]{8})') 
              FROM @my_s3_stage
            )
            PATTERN='.*/etf/.*etf_{{ ds_nodash }}\.json';
        """
    )

    # 4. dbt 변환 및 테스트
    dbt_run = BashOperator(
        task_id='T_dbt_run',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
        task_id='T_dbt_test',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
        on_success_callback=send_slack_alert
    )

    # 파이프라인 의존성 설정 (병렬 추출 -> 병렬 적재 -> dbt)
    extract_kexim_task >> load_kexim_to_bronze
    extract_etf_task >> load_etf_to_bronze
    
    [load_kexim_to_bronze, load_etf_to_bronze] >> dbt_run >> dbt_test