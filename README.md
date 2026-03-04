# 📊 데이터 파이프라인 0단계: 로컬 개발 환경 및 CI/CD 구축 가이드

**문서 목적:** Windows 11 환경에서 WSL2(Ubuntu)를 활용하여 데이터 파이프라인(Airflow, dbt) 로컬 개발 환경을 구축하고, GitHub Actions 기반의 CI/CD 뼈대를 세팅합니다.

## 1. 기반 환경 세팅 (WSL2 및 Git)

Windows 환경에 리눅스 하위 시스템을 설치하고, 버전 관리를 위한 Git 최초 사용자 정보를 등록합니다.

```bash
# [Windows PowerShell - 관리자 권한]
# 1. WSL2 및 Ubuntu 설치 (설치 후 재부팅 필요)
wsl --install

# [WSL Ubuntu 터미널]
# 2. Git 최초 사용자 명함 등록 (이후 커밋 에러 방지용)
git config --global user.name "본인의_영문_이름"
git config --global user.email "본인의_깃허브_이메일@example.com"

```

## 2. 프로젝트 디렉토리 구조 및 Git 초기화

전체 코드가 담길 최상위 폴더를 만들고, Airflow와 dbt 작업 공간을 분리합니다.

```bash
# [WSL Ubuntu 터미널]
# 1. 최상위 프로젝트 폴더 생성 및 이동
mkdir data-pipeline-practice
cd data-pipeline-practice

# 2. 현재 폴더를 Git 로컬 저장소로 초기화
git init

# 3. 데이터 변환(dbt)용 폴더 생성
mkdir dbt

# 4. 데이터 수집/스케줄링(Airflow)용 하위 폴더들 일괄 생성
mkdir -p airflow/{dags,logs,plugins,config}

```

## 3. Airflow 서버 구축 (Docker Compose)

의존성 충돌을 막기 위해 Airflow를 Docker 컨테이너 위에서 실행합니다.

```bash
# [WSL Ubuntu 터미널]
# 1. Docker 데몬 접근 권한 부여 (초기 1회 세팅, 완료 후 터미널 권한 즉시 적용)
sudo usermod -aG docker $USER
newgrp docker

# 2. Airflow 작업 폴더로 이동
cd airflow

# 3. 공식 Airflow 환경 설정 파일 다운로드
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# 4. 리눅스 호스트와 컨테이너 간의 권한(UID) 일치를 위한 환경변수 파일 생성
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 5. Airflow 메타데이터 DB 초기화 및 기본 계정(airflow/airflow) 생성
docker compose up airflow-init

# 6. Airflow 컨테이너 백그라운드 실행
docker compose up -d

```

> **확인:** 브라우저에서 `http://localhost:8080` 접속 (ID/PW: airflow)

## 4. dbt Core 로컬 가상환경 구축

파이썬 가상환경을 구성하고, Snowflake와 연동할 수 있는 dbt 오픈소스 버전을 설치합니다.

```bash
# [WSL Ubuntu 터미널]
# 1. dbt 폴더로 이동
cd ../dbt

# 2. Ubuntu 환경에 파이썬 가상환경 패키지 설치
sudo apt update
sudo apt install python3-venv -y

# 3. 'dbt-env'라는 이름의 가상환경 생성 및 활성화
python3 -m venv dbt-env
source dbt-env/bin/activate

# 4. 파이썬 패키지 관리자 업데이트 및 dbt-core, dbt-snowflake 설치
pip install --upgrade pip
pip install dbt-core dbt-snowflake

# 5. 실제 쿼리가 담길 dbt 프로젝트 뼈대 생성 (DB는 1번 snowflake 선택, 나머지는 엔터)
dbt init public_data_mart

```

> **참고:** 이후 dbt 작업 시에는 항상 `source dbt-env/bin/activate` 명령어로 가상환경을 켜두어야 합니다.

## 5. CI/CD 파이프라인 스크립트 작성 (GitHub Actions)

코드가 GitHub 서버에 푸시될 때, dbt SQL 문법 오류를 자동으로 검사하도록 세팅합니다.

```bash
# [WSL Ubuntu 터미널]
# 1. 최상위 폴더로 이동하여 GitHub Actions 전용 폴더 생성
cd ..
mkdir -p .github/workflows

# 2. CI 자동화 스크립트를 작성할 빈 yaml 파일 생성
touch .github/workflows/dbt_ci.yml

```

생성된 `.github/workflows/dbt_ci.yml` 파일에 아래 내용을 작성하여 저장합니다.

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI Pipeline

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  dbt-compile-test:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dbt-snowflake
        run: pip install dbt-core dbt-snowflake

      - name: Run dbt deps
        run: dbt deps
        working-directory: ./dbt/public_data_mart

      - name: Run dbt compile
        # 실제 DB 연동 없이 로컬 환경에서 SQL 문법 오류만 테스트합니다.
        run: dbt compile
        working-directory: ./dbt/public_data_mart

```

## 6. GitHub 원격 저장소 연동 및 배포

작성된 모든 초기 세팅 코드를 GitHub에 올려 CI/CD가 정상 작동하는지 확인합니다.

```bash
# [WSL Ubuntu 터미널]
# 1. 로컬의 모든 변경사항을 추적(Staging)
git add .

# 2. 변경사항에 대한 설명표(Commit Message) 부착
git commit -m "chore: CI/CD workflow 및 기초 환경 세팅 완료"

# 3. 로컬 기본 브랜치 이름을 최신 표준인 'main'으로 변경
git branch -M main

# 4. GitHub 원격 저장소 연결 (사전에 GitHub 웹에서 빈 레포지토리 생성 필요)
git remote add origin https://github.com/본인계정/저장소이름.git

# 5. GitHub 서버의 main 브랜치로 코드 푸시
git push -u origin main

```

> **확인:** GitHub 웹사이트 해당 저장소의 **[Actions]** 탭에서 워크플로우 성공(✅) 여부를 확인합니다.
