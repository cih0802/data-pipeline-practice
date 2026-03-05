from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import pendulum
import requests
# Deprecation 경고 대응 패키지 경로 변경. models → sdk
from airflow.sdk import Variable

# 분리한 scripts 폴더에서 함수 불러오기
from scripts.extract_kexim import fetch_and_upload_to_s3

# [추가] Slack 알림 전송 함수 정의
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

# [수정] 실패 시 기본적으로 알림 함수가 작동하도록 추가
# api 서버 불안정으로 1회 실패하고 데이터 불러옴(20260305)
# → 시도횟수를 3회로 늘리고 점차 시간을 늘림
default_args = {
    'owner': 'InHwan Cho',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, # 재시도 간격을 점점 늘림
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback': send_slack_alert 
}

with DAG(
    dag_id='kexim_exchange_rate_elt_pipeline',
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    schedule='45 11 * * *',
    catchup=False,
    default_args=default_args
) as dag:

    # Python 스크립트 내부에서 logical_date를 사용하므로 op_kwargs 제거됨
    extract_task = PythonOperator(
        task_id='E_extract_to_s3',
        python_callable=fetch_and_upload_to_s3
    )

    # Incremental용 {{ ds_nodash }} 패턴 적용
    load_to_bronze = SQLExecuteQueryOperator(
        task_id='L_load_s3_to_snowflake',
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE ROLE SYSADMIN;
            USE WAREHOUSE COMPUTE_WH;
            USE SCHEMA SANDBOX.BRONZE;

            ALTER STAGE my_s3_stage REFRESH;

            CREATE TABLE IF NOT EXISTS raw_exchange_rate (
                raw_data VARIANT,
                loaded_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('UTC', 'Asia/Seoul', SYSDATE())::TIMESTAMP_NTZ,
                search_date VARCHAR2(8)
            );

            COPY INTO raw_exchange_rate (raw_data, search_date)
            FROM (
              SELECT 
                $1, 
                REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]{8})') 
              FROM @my_s3_stage
            )
            PATTERN='.*exchange_rate_{{ ds_nodash }}\.json';
        """
    )

    dbt_run = BashOperator(
        task_id='T_dbt_run',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}"
    )

    # 파이프라인이 최종 성공했을 때만 알림 전송
    dbt_test = BashOperator(
        task_id='T_dbt_test',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
        on_success_callback=send_slack_alert
    )

    extract_task >> load_to_bronze >> dbt_run >> dbt_test