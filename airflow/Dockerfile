FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 1. Airflow 패키지 설치 (안전한 Constraints 방식 유지)
ARG AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2.8.1"
ARG PYTHON_VERSION="3.11"
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/${AIRFLOW_CONSTRAINTS_REFERENCE}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"

# 2. 🎯 dbt 전용 가상 환경(venv) 생성 및 설치
RUN python -m venv /opt/airflow/dbt_venv

# 🎯 핵심: PIP_USER=false를 앞에 붙여서 가상 환경 내부에 정상적으로 설치되도록 유도
RUN PIP_USER=false /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip
RUN PIP_USER=false /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-snowflake

# 3. 가상 환경의 dbt 명령어를 컨테이너 전역에서 쓸 수 있도록 환경변수 PATH 추가
ENV PATH="/opt/airflow/dbt_venv/bin:$PATH"