import requests
import boto3
import json
from airflow.models import Variable

def fetch_and_upload_to_s3(**kwargs):
    # Airflow UI의 Variables에서 값을 가져옴
    API_KEY = Variable.get("KEXIM_API_KEY") 
    BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
    AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")

    execution_date = kwargs['logical_date']
    searchdate = execution_date.strftime('%Y%m%d')
    partition_path = f"year={execution_date.strftime('%Y')}/month={execution_date.strftime('%m')}/day={execution_date.strftime('%d')}"

    url = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"
    params = {'authkey': API_KEY, 'searchdate': searchdate, 'data': 'AP01'}

    response = requests.get(url, params=params, verify=False)
    response.encoding = 'utf-8'
    
    if response.status_code == 200:
        data = response.json()
        if not data: return # 데이터 없으면 종료

        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        s3_key = f"raw_data/exchange_rate/{partition_path}/exchange_rate_{searchdate}.json"

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data, ensure_ascii=False).encode('utf-8')
        )