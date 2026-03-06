import requests
import pandas as pd
import boto3
import json
from datetime import datetime
from airflow.models import Variable

def fetch_etf_and_upload_to_s3(**kwargs):
    BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
    AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")

    execution_date = kwargs['logical_date']
    searchdate = execution_date.strftime('%Y%m%d')
    partition_path = f"year={execution_date.strftime('%Y')}/month={execution_date.strftime('%m')}/day={execution_date.strftime('%d')}"

    # 야후 파이낸스 Query API를 위한 Unix Timestamp 계산
    # 시작일 (오늘 00:00:00) / 종료일 (내일 00:00:00)
    period1 = int(execution_date.timestamp())
    period2 = int(execution_date.add(days=1).timestamp())

    tickers = ['SPY', 'QQQ']
    data_list = []

    # 브라우저처럼 보이게 하기 위한 헤더
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    for ticker in tickers:
        # 유저님이 브라우저에서 확인한 그 URL 구조
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {
            'period1': period1,
            'period2': period2,
            'interval': '1d',
            'events': 'history'
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            res_json = response.json()

            # 야후 JSON 구조에서 필요한 데이터(Open, High, Low, Close, Volume) 추출
            result = res_json['chart']['result'][0]
            indicators = result['indicators']['quote'][0]
            timestamps = result['timestamp']

            for i in range(len(timestamps)):
                data_list.append({
                    'Date': datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d'),
                    'Ticker': ticker,
                    'Open': indicators['open'][i],
                    'High': indicators['high'][i],
                    'Low': indicators['low'][i],
                    'Close': indicators['close'][i],
                    'Volume': indicators['volume'][i]
                })
            print(f"Successfully fetched {ticker} via Direct URL")

        except Exception as e:
            print(f"Failed to fetch {ticker}: {str(e)}")

    # 데이터 검증 (수출입은행 방식과 동일)
    if not data_list:
        if execution_date.weekday() >= 5:
            print(f"Weekend detected for {searchdate}. Skipping.")
            return
        raise ValueError(f"No ETF data fetched for {searchdate} via URL API.")

    # S3 적재
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    s3_key = f"raw_data/etf/{partition_path}/etf_{searchdate}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data_list, ensure_ascii=False).encode('utf-8')
    )