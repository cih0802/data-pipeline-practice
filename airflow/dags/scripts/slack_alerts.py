import requests
from airflow.models import Variable

def send_slack_alert(context):
    """
    Airflow Task의 성공/실패 여부를 판단하여 Slack으로 알림을 전송합니다.
    """
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
        # 사용자 멘션 포함 (에러 발생 시 즉각 확인)
        msg = f"<@U09DTEKRBFZ>🚨 *[FAILED]* DAG: `{dag_id}` | Task: `{task_id}`\n🔍 <{log_url}|에러 로그 확인하기>"

    payload = {"text": msg}
    requests.post(webhook_url, json=payload)