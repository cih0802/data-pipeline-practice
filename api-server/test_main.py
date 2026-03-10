from fastapi.testclient import TestClient
from main import app

# FastAPI 앱을 TestClient로 감싸서 가상의 요청을 보낼 수 있게 만듭니다.
client = TestClient(app)

def test_health_check():
    """시스템 헬스 체크 엔드포인트가 200 OK와 올바른 메시지를 반환하는지 테스트"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "message": "Serving DB API is alive"}

def test_get_all_metrics():
    """전체 지표 조회 시 리스트 형태로 데이터가 반환되는지 테스트"""
    # 쿼리 파라미터(limit=5)가 정상 작동하는지도 함께 검증합니다.
    response = client.get("/metrics/all?limit=5")
    assert response.status_code == 200
    
    data = response.json()
    assert isinstance(data, list)
    # 데이터가 존재한다면 5개를 넘지 않아야 하며, Pydantic 모델에 정의된 TICKER 필드가 있어야 합니다.
    assert len(data) <= 5
    if len(data) > 0:
        assert "TICKER" in data[0]

def test_get_ticker_metrics_success():
    """존재하는 티커(SPY) 조회 시 200 OK와 해당 티커의 데이터가 반환되는지 테스트"""
    response = client.get("/metrics/SPY")
    assert response.status_code == 200
    
    data = response.json()
    assert isinstance(data, list)
    # 반환된 데이터의 첫 번째 레코드 티커가 'SPY'인지 검증합니다.
    if len(data) > 0:
        assert data[0]["TICKER"] == "SPY"

def test_get_ticker_metrics_not_found():
    """존재하지 않는 티커 조회 시 404 에러와 정확한 에러 메시지가 반환되는지 테스트"""
    response = client.get("/metrics/UNKNOWN_TICKER")
    assert response.status_code == 404
    assert response.json()["detail"] == "'UNKNOWN_TICKER' 종목의 데이터를 찾을 수 없습니다."