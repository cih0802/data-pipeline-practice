{{ config(
    materialized='incremental',
    unique_key=['trade_date', 'ticker']
) }}

with etf_data as (
    select * from {{ ref('stg_etf') }}
    
    {% if is_incremental() %}
    where trade_date >= dateadd(day, -7, (select coalesce(max(trade_date), '1900-01-01') from {{ this }}))
    {% endif %}
),

exchange_rate_data as (
    select
        to_date(base_date, 'YYYYMMDD') as ex_date,
        base_rate as usd_krw_rate
    from {{ ref('fct_daily_exchange_rate') }}
    where currency_code = 'USD'
    
    {% if is_incremental() %}
    -- [최적화] 환율 데이터도 스캔 범위를 줄여 컴퓨팅 비용 절약
    and to_date(base_date, 'YYYYMMDD') >= dateadd(day, -7, (select coalesce(max(trade_date), '1900-01-01') from {{ this }}))
    {% endif %}
),

joined_data as (
    select
        e.trade_date,
        e.ticker,
        e.usd_close_price,
        -- [버그 수정] ticker별로 파티션을 나누어야 종목 간 간섭 없이 정확한 이전 환율을 가져옴
        coalesce(x.usd_krw_rate, lag(x.usd_krw_rate) ignore nulls over (partition by e.ticker order by e.trade_date)) as usd_krw_rate
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
        (usd_close_price * usd_krw_rate) as krw_close_price,
        
        lag(usd_close_price) over (partition by ticker order by trade_date) as prev_usd_close,
        lag(usd_close_price * usd_krw_rate) over (partition by ticker order by trade_date) as prev_krw_close
    from joined_data
)

select 
    trade_date,
    ticker,
    round(usd_close_price, 2) as usd_close_price,
    usd_krw_rate,
    round(krw_close_price, 0) as krw_close_price,
    
    -- [최적화] nullif를 사용하여 Division by Zero 에러 원천 차단
    round(((usd_close_price - prev_usd_close) / nullif(prev_usd_close, 0)) * 100, 2) as usd_daily_return_pct,
    round(((krw_close_price - prev_krw_close) / nullif(prev_krw_close, 0)) * 100, 2) as krw_daily_return_pct
from metrics_calculated

{% if is_incremental() %}
-- 윈도우 함수 연산이 끝난 후, 실제 타겟 테이블에 없는 최신 데이터만 필터링하여 삽입/병합
where trade_date > (select coalesce(max(trade_date), '1900-01-01') from {{ this }})
{% endif %}
-- [최적화] Snowflake에서 테이블 적재 시 무의미한 order by 제거