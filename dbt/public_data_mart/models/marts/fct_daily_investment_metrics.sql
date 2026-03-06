{{ config(
    materialized='incremental',
    unique_key=['trade_date', 'ticker']
) }}

with etf_data as (
    select * from {{ ref('stg_etf') }}
    
    {% if is_incremental() %}
    -- [포트폴리오 포인트] 증분 적재 시 전일 종가(LAG) 계산을 위해 최근 7일치 데이터를 여유 있게 가져옵니다.
    where trade_date >= dateadd(day, -7, (select coalesce(max(trade_date), '1900-01-01') from {{ this }}))
    {% endif %}
),

-- USD 환율 데이터 준비 (기존에 만드신 데이터 마트 활용)
exchange_rate_data as (
    select
        to_date(base_date, 'YYYYMMDD') as ex_date,
        base_rate as usd_krw_rate
    from {{ ref('fct_daily_exchange_rate') }}
    where currency_code = 'USD'
),

joined_data as (
    select
        e.trade_date,
        e.ticker,
        e.usd_close_price,
        -- 미국장 거래일과 한국 환율 고시일 매핑 (휴일 등으로 환율이 null일 경우를 대비한 처리 가능)
        coalesce(x.usd_krw_rate, lag(x.usd_krw_rate) ignore nulls over (order by e.trade_date)) as usd_krw_rate
    from etf_data e
    left join exchange_rate_data x 
        on e.trade_date = x.ex_date
),

metrics_calculated as (
    select
        trade_date,
        ticker,
        usd_close_price,
        usd_krw_rate,
        -- 1. 원화 환산 종가 계산
        (usd_close_price * usd_krw_rate) as krw_close_price,
        
        -- 2. 달러 기준 전일 대비 수익률 계산
        lag(usd_close_price) over (partition by ticker order by trade_date) as prev_usd_close,
        ((usd_close_price - prev_usd_close) / prev_usd_close) * 100 as usd_daily_return_pct,

        -- 3. 원화 기준 전일 대비 수익률 계산 (환율 변동 효과 반영)
        lag(usd_close_price * usd_krw_rate) over (partition by ticker order by trade_date) as prev_krw_close,
        (((usd_close_price * usd_krw_rate) - prev_krw_close) / prev_krw_close) * 100 as krw_daily_return_pct

    from joined_data
)

select 
    trade_date,
    ticker,
    round(usd_close_price, 2) as usd_close_price,
    usd_krw_rate,
    round(krw_close_price, 0) as krw_close_price,
    round(usd_daily_return_pct, 2) as usd_daily_return_pct,
    round(krw_daily_return_pct, 2) as krw_daily_return_pct
from metrics_calculated
where prev_usd_close is not null -- 첫 거래일의 null 수익률 데이터 제외

{% if is_incremental() %}
-- 윈도우 함수 연산이 끝난 후, 실제 타겟 테이블에 없는 최신 데이터만 필터링하여 삽입/병합
and trade_date > (select coalesce(max(trade_date), '1900-01-01') from {{ this }})
{% endif %}
order by trade_date desc, ticker