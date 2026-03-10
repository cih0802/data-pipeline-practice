import os
from datetime import date
from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
import pandas as pd

app = FastAPI(
    title="ETF Investment Metrics API",
    description="Snowflake에서 정제된 일별 ETF 투자 지표를 제공하는 엔터프라이즈급 API",
    version="1.0.0"
)

# 환경 변수에서 DB 주소를 가져오고, 없으면 로컬용 주소 폴백(Fallback)
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://serving_user:serving_password@localhost:5433/etf_service"
)

# 🎯 최적화 1: DB 커넥션 안정성 강화
# pool_pre_ping=True: DB 연결이 끊어졌을 때(Time out) 
# 쿼리 실행 전 연결 상태를 확인하고 자동으로 재연결을 시도합니다.
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
# ==========================================
# 🛡️ Pydantic Schema (데이터 유효성 검사 및 타입 정의)
# ==========================================
class ETFMetricResponse(BaseModel):
    """
    클라이언트에게 반환될 데이터의 엄격한 규격(Schema)입니다.
    데이터베이스 컬럼명(대문자)과 1:1로 매칭되며, None(null) 허용 여부를 명확히 합니다.
    """
    TRADE_DATE: date = Field(..., description="거래 일자 (YYYY-MM-DD)")
    TICKER: str = Field(..., description="ETF 종목 티커 (예: SPY)")
    USD_CLOSE_PRICE: float | None = Field(None, description="USD 기준 종가")
    USD_KRW_RATE: float | None = Field(None, description="당일 원/달러 환율")
    KRW_CLOSE_PRICE: float | None = Field(None, description="KRW 환산 종가")
    USD_DAILY_RETURN_PCT: float | None = Field(None, description="USD 기준 일일 수익률(%)")
    KRW_DAILY_RETURN_PCT: float | None = Field(None, description="KRW 환산 일일 수익률(%)")

# 🎯 최적화 2: 중복 코드 제거 (Helper 함수)
# 여러 엔드포인트에서 공통으로 사용하는 NaN 클렌징 로직을 함수로 분리해 유지보수성을 높였습니다.
def clean_nan_to_none(df: pd.DataFrame) -> list[dict]:
    records = df.to_dict(orient="records")
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in records
    ]

# ==========================================
# 🚀 API Endpoints
# ==========================================
@app.get("/health", tags=["System"])
def health_check():
    """서버 및 데이터베이스 상태를 확인합니다."""
    return {"status": "ok", "message": "Serving DB API is alive"}

# 🎯 response_model 지정: 반환되는 데이터가 리스트 형태의 ETFMetricResponse 임을 강제함
@app.get("/metrics/all", response_model=list[ETFMetricResponse], tags=["Metrics"])
def get_all_metrics(
    limit: int = Query(100, ge=1, le=1000, description="가져올 최대 데이터 개수 (최대 1000)"), 
    offset: int = Query(0, ge=0, description="건너뛸 데이터 개수")
):
    """
    🎯 최적화 3: 페이징(Pagination) 처리
    하드코딩된 LIMIT 100 대신, 클라이언트가 원하는 만큼 끊어서 데이터를 요청할 수 있게 만듭니다.
    예: /metrics/all?limit=50&offset=100
    """
    try:
        query = text("SELECT * FROM daily_investment_metrics LIMIT :limit OFFSET :offset")
        df = pd.read_sql(query, engine, params={"limit": limit, "offset": offset})
        
        return clean_nan_to_none(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail="데이터베이스 조회 중 오류가 발생했습니다.")

@app.get("/metrics/{ticker}", response_model=list[ETFMetricResponse], tags=["Metrics"])
def get_ticker_metrics(
    # 🎯 Field 대신 Path를 사용해야 합니다.
    ticker: str = Path(..., min_length=1, description="조회할 ETF 티커")
):
    """특정 ETF 종목의 전체 기간 지표를 조회합니다."""
    try:
        # 🎯 최적화 4: 쿼리 연산 최소화 및 인덱스 활용
        # 🎯 f-string을 제거하고 text() 함수로 SQL 문자열을 감싸줍니다.
        # 🎯 컬럼명을 쌍따옴표로 감싸서 대문자임을 명시합니다. "TICKER"라고 쓰면 Postgres가 대문자 컬럼을 정확히 찾아갑니다.
        # DAG 적재 단계에서 공백 처리를 완료했으므로 TRIM()을 제거했습니다.
        query = text('SELECT * FROM daily_investment_metrics WHERE UPPER("TICKER") = :ticker')
        df = pd.read_sql(query, engine, params={"ticker": ticker.upper()})
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"'{ticker}' 종목의 데이터를 찾을 수 없습니다.")
            
        return clean_nan_to_none(df)
        
    except HTTPException:
        raise # 404 에러는 그대로 통과
    except Exception as e:
        # 실무 보안: 내부 에러(DB 접속 실패 등) 상세 내용을 클라이언트에게 노출하지 않고 로깅만 수행
        print(f"Internal Error: {e}")
        raise HTTPException(status_code=500, detail="서버 내부 오류가 발생했습니다.")