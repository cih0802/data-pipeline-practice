

/* --------------------------------------------------------------------------
   1. etf_data (기준 데이터 추출)
   - 미국 주식 시장의 ETF 일별 종가 데이터를 가져옵니다.
   - [증분 최적화] 전체 스캔을 방지하고, 이전 영업일 데이터(LAG) 계산을 위해 
     타겟 테이블의 최신 날짜 기준 과거 14일치 데이터를 여유분으로 가져옵니다.
     (한국의 최장 명절 연휴 등으로 인한 데이터 단절 방어 목적)
-------------------------------------------------------------------------- */
with etf_data as (
    select * from sandbox.public_data_mart_dev.stg_etf
    
    
    where trade_date >= dateadd(day, -14, (select coalesce(max(trade_date), '1900-01-01') from sandbox.public_data_mart_dev.fct_daily_investment_metrics))
    
),

/* --------------------------------------------------------------------------
   2. exchange_rate_data (조인 대상 데이터 추출)
   - 한국수출입은행 기준 환율 마트에서 'USD' 데이터만 필터링합니다.
   - [증분 최적화] ETF 데이터와 동일하게 스캔 범위를 과거 14일로 제한하여
     비용 낭비(Full Scan)를 원천 차단합니다.
-------------------------------------------------------------------------- */
exchange_rate_data as (
    select
        to_date(base_date, 'YYYYMMDD') as ex_date,
        base_rate as usd_krw_rate
    from sandbox.public_data_mart_dev.fct_daily_exchange_rate
    where currency_code = 'USD'
    
    
    and to_date(base_date, 'YYYYMMDD') >= dateadd(day, -14, (select coalesce(max(trade_date), '1900-01-01') from sandbox.public_data_mart_dev.fct_daily_investment_metrics))
    
),

/* --------------------------------------------------------------------------
   3. joined_data (데이터 결합 및 결측치 보간)
   - ETF 거래일(미국 기준)에 맞춰 한국 고시 환율을 매핑합니다.
   - 한국이 휴일이라 환율 데이터가 없는 경우, 가장 최근의 영업일 환율로 채웁니다.
-------------------------------------------------------------------------- */
joined_data as (
    select
        e.trade_date,
        e.ticker,
        e.usd_close_price,
        
        -- [결측치 보간] IGNORE NULLS를 활용한 Forward-fill (이전 값으로 채우기)
        -- 종목별(partition by ticker)로 날짜순 정렬 후, 현재 행 이전까지의 값 중
        -- NULL이 아닌 가장 마지막 환율을 가져와 휴일 환율을 대체합니다.
        last_value(x.usd_krw_rate ignore nulls) over (
            partition by e.ticker 
            order by e.trade_date 
            rows between unbounded preceding and current row
        ) as usd_krw_rate
        
    from etf_data e
    left join exchange_rate_data x 
        on e.trade_date = x.ex_date
),

/* --------------------------------------------------------------------------
   4. metrics_calculated (분석용 파생 변수 생성)
   - 원화 환산 종가 및 수익률 계산을 위한 전일(LAG) 종가 데이터를 생성합니다.
-------------------------------------------------------------------------- */
metrics_calculated as (
    select
        trade_date,
        ticker,
        usd_close_price,
        usd_krw_rate,
        
        -- 현재일 원화 환산 종가
        (usd_close_price * usd_krw_rate) as krw_close_price,
        
        -- 수익률 계산을 위한 전일 종가(달러/원화) 가져오기
        lag(usd_close_price) over (partition by ticker order by trade_date) as prev_usd_close,
        lag(usd_close_price * usd_krw_rate) over (partition by ticker order by trade_date) as prev_krw_close
    from joined_data
)

/* --------------------------------------------------------------------------
   5. Final Select (최종 출력 및 증분 적재 필터링)
   - 지표들의 소수점을 정리하고, Zero Division 에러를 방어하며 수익률을 계산합니다.
-------------------------------------------------------------------------- */
select 
    trade_date,
    ticker,
    round(usd_close_price, 2) as usd_close_price,
    usd_krw_rate,
    round(krw_close_price, 0) as krw_close_price,
    
    -- [안전장치] nullif(분모, 0)을 사용하여 전일 종가가 0일 경우 NULL 처리로 연산 에러 방지
    round(((usd_close_price - prev_usd_close) / nullif(prev_usd_close, 0)) * 100, 2) as usd_daily_return_pct,
    round(((krw_close_price - prev_krw_close) / nullif(prev_krw_close, 0)) * 100, 2) as krw_daily_return_pct
    
from metrics_calculated
where trade_date is not null 


-- [최종 필터링] 윈도우 함수 연산을 위해 14일치를 가져왔지만, 
-- 실제 테이블에 Insert/Update 하는 데이터는 기존에 없는 최신 날짜의 데이터만 반영합니다.
and trade_date > (select coalesce(max(trade_date), '1900-01-01') from sandbox.public_data_mart_dev.fct_daily_investment_metrics)
