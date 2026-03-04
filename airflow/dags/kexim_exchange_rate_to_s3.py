from airflow import DAG
from airflow.sdk import Variable
# [수정 1] 경고(Warning) 제거를 위한 최신 Import 경로 변경
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import boto3
import json

# dbt 관련 경로 설정 (Airflow 컨테이너 내부 경로 기준)
DBT_PROJECT_DIR = "/opt/airflow/dbt/public_data_mart"
# [수정 2] 가상환경 활성화(VENV_ACTIVATE) 변수 삭제
# VENV_ACTIVATE = "source /opt/airflow/dbt/dbt-env/bin/activate"
# ==========================================

def fetch_and_upload_to_s3(**kwargs):
    # ==========================================
    # [보안 적용] Task가 실행될 때만 DB에서 변수를 가져옵니다.
    # ==========================================
    API_KEY = Variable.get("KEXIM_API_KEY") 
    BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
    
    # 보안 팁: 변수명에 'secret', 'password', 'key'가 포함되면 Airflow UI에서 자동 마스킹(****) 처리됩니다.
    AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")

    execution_date = kwargs['logical_date']
    searchdate = execution_date.strftime('%Y%m%d')
    partition_path = f"year={execution_date.strftime('%Y')}/month={execution_date.strftime('%m')}/day={execution_date.strftime('%d')}"

    url = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"
    params = {'authkey': API_KEY, 'searchdate': searchdate, 'data': 'AP01'}

    response = requests.get(url, params=params, verify=False)
    response.encoding = 'utf-8'
    
    if response.status_code != 200:
        raise Exception(f"API 호출 실패 (상태 코드: {response.status_code})")

    data = response.json()

    if not data:
        print(f"데이터 없음: {searchdate}은(는) 휴일이거나 데이터가 존재하지 않습니다.")
        return

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name='ap-southeast-2' 
    )

    file_name = f"exchange_rate_{searchdate}.json"
    s3_key = f"raw_data/exchange_rate/{partition_path}/{file_name}"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data, ensure_ascii=False).encode('utf-8') 
    )
    print(f"✅ S3 적재 완료: s3://{BUCKET_NAME}/{s3_key} (총 {len(data)}건)")

with DAG(
    dag_id='extract_kexim_exchange_rate_to_s3',
    start_date=datetime(2026, 3, 1),
    schedule='@daily',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)}
) as dag:

    # 1. API에서 S3로 데이터 추출 및 적재
    extract_task = PythonOperator(
        task_id='fetch_api_and_load_s3',
        python_callable=fetch_and_upload_to_s3
    )

    # 2. dbt run: Silver/Gold 계층 변환 실행
    # --profiles-dir을 명시적으로 프로젝트 폴더로 지정합니다.
    """
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=f"cd {DBT_PROJECT_DIR} && {VENV_ACTIVATE} && dbt run --profiles-dir {DBT_PROJECT_DIR}"
    )

    # 3. dbt test: 데이터 품질 검증 (not null 등)
    dbt_test = BashOperator(
        task_id='dbt_test_data',
        bash_command=f"cd {DBT_PROJECT_DIR} && {VENV_ACTIVATE} && dbt test --profiles-dir {DBT_PROJECT_DIR}"
    )
"""
    # [수정 3] bash_command에서 가상환경 활성화 부분 제거
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
        task_id='dbt_test_data',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}"
    )
    # 태스크 순서 연결 (추출 -> dbt 변환 -> dbt 검증)
    extract_task >> dbt_run >> dbt_test