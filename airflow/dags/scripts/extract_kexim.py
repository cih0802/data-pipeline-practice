import requests
import boto3
import json
import urllib3  # [추가] InsecureRequestWarning 숨기기를 위한 모듈

# 2.x 버전 경로로 수정됨
from airflow.models import Variable 

# [추가] verify=False 사용 시 발생하는 HTTPS 보안 경고 로그 숨기기
# 단, 권장사항(best practice)은 해당 기관의 인증서 직접 등록 하는 것
# 여기선 실습을 위해 verify=false 사용
# 브라우저를 통해 한국수출입은행 API 서버의 인증서(예: .pem 파일)를 다운로드한 뒤, Airflow 서버의 특정 경로에 저장하고 코드에 반영합니다.
# verify 인자에 False 대신 다운받은 인증서 경로를 지정합니다.
# response = requests.get(url, params=params, verify='/opt/airflow/certs/kexim_cert.pem', timeout=(10, 30))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_and_upload_to_s3(**kwargs):
    API_KEY = Variable.get("KEXIM_API_KEY") 
    BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
    AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")

    execution_date = kwargs['logical_date']
    searchdate = execution_date.strftime('%Y%m%d')
    partition_path = f"year={execution_date.strftime('%Y')}/month={execution_date.strftime('%m')}/day={execution_date.strftime('%d')}"

    url = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"
    params = {'authkey': API_KEY, 'searchdate': searchdate, 'data': 'AP01'}

    # [수정] timeout 추가: 서버 연결 최대 10초 대기, 데이터 수신 최대 30초 대기
    response = requests.get(url, params=params, verify=False, timeout=(10, 30))
    response.encoding = 'utf-8'
    
    # [추가] HTTP 상태 코드가 200 정상 응답이 아닐 경우 즉시 예외(Exception) 발생
    # 이를 통해 Airflow가 단순 데이터 누락이 아닌 API 호출 실패로 정확히 인지하고 재시도(Retry)하게 됨
    response.raise_for_status() 

    data = response.json()
    
    if not data: 
        print(f"No data found for {searchdate}")
        return # 데이터 없으면 종료

    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    s3_key = f"raw_data/exchange_rate/{partition_path}/exchange_rate_{searchdate}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data, ensure_ascii=False).encode('utf-8')
    )