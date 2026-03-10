# 1. 경량화된 파이썬 베이스 이미지 사용
FROM python:3.12-slim

# 2. 작업 디렉토리 설정
WORKDIR /app

# 3. 시스템 의존성 설치 (psycopg2 등을 위해 필요) 및 캐시 삭제
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. 패키지 설치 (소스코드 복사 전 실행하여 도커 빌드 캐시 효율 극대화)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 소스 코드 복사
COPY . .

# 6. 보안을 위한 비권한 사용자(non-root) 생성 및 권한 부여
RUN useradd -m apiuser && chown -R apiuser /app
USER apiuser

# 7. 포트 노출 및 실행
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]