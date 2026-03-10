from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
# 🎯 2번 DAG에도 동일하게 Dataset 임포트 추가
from airflow.datasets import Dataset

# 🎯 1번 DAG와 완전히 똑같은 이름표(URI)를 선언합니다.
fct_metrics_dataset = Dataset("snowflake://SANDBOX/PUBLIC_DATA_MART_DEV/FCT_DAILY_INVESTMENT_METRICS")

# 접속 정보 ID 정의 (Airflow UI에 등록된 ID)
SNOWFLAKE_CONN_ID = 'snowflake_default'
POSTGRES_CONN_ID = 'postgres_serving'

def transfer_snowflake_to_postgres():
    # 1. Snowflake에서 데이터 읽기
    sn_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = "SELECT * FROM SANDBOX.PUBLIC_DATA_MART_DEV.FCT_DAILY_INVESTMENT_METRICS"
    df = sn_hook.get_pandas_df(sql)
    
    if df.empty:
        print("⚠️ Snowflake에서 가져올 데이터가 없습니다.")
        return
        
    print(f"✅ Snowflake 데이터 추출 완료 ({len(df)} rows)")

    # 🎯 최적화 1: Pandas 메모리 단에서 문자열 공백(Padding) 미리 제거
    # DB에 적재한 뒤 UPDATE를 치는 것보다, 메모리에서 정제하고 넣는 것이 성능상 유리합니다.
    # Snowflake CHAR 타입 등으로 인해 생긴 앞뒤 공백을 모든 문자열 컬럼에서 일괄 제거합니다.
    string_columns = df.select_dtypes(include=['object', 'string']).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()

    # 2. Airflow UI의 커넥션 정보로부터 직접 URL 구성 (에러 방지용)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_connection(POSTGRES_CONN_ID)
    # 순수하게 필요한 정보만 조합하여 주소 생성
    # postgresql://user:password@host:port/dbname
    db_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(db_url)

    # 3. 데이터 적재
    # 🎯 최적화 2: Engine의 Context Manager 사용 및 Batch Insert 적용
    with engine.begin() as connection:
        df.to_sql(
            name='daily_investment_metrics',
            con=connection,
            if_exists='replace',
            index=False,
            # 🎯 성능 극대화: 데이터를 1000건씩 묶어서 다중 Insert 쿼리로 실행
            chunksize=1000,
            method='multi' 
        )
        
        # (선택) DB 레벨에서 테이블 권한 부여나 인덱스 생성이 필요하다면 이곳에 추가
        # connection.execute(text("CREATE INDEX idx_ticker ON daily_investment_metrics (\"TICKER\");"))

    print(f"✅ PostgreSQL({POSTGRES_CONN_ID}) 적재 완료! (공백 제거 및 Batch Insert 적용)")

with DAG(
    dag_id='sync_snowflake_to_postgres',
    start_date=datetime(2024, 1, 1),
    # 🎯 핵심: 시간이나 수동 실행(None) 대신, 데이터셋을 스케줄러로 지정 (Consumer)
    schedule=[fct_metrics_dataset],
    catchup=False,
    tags=['serving', 'etl'] # Airflow UI에서 필터링하기 쉽도록 태그 부여
) as dag:

    task_transfer = PythonOperator(
        task_id='transfer_data',
        python_callable=transfer_snowflake_to_postgres
    )