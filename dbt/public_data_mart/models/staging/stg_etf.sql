{{ config(materialized='view') }}

with raw_source as (
    select
        raw_data,
        loaded_at,
        search_date
    from {{ source('bronze_layer', 'raw_etf') }}
),

parsed_data as (
    -- 데이터가 이미 행 단위로 적재되었으므로 f.value 대신 raw_data를 직접 참조합니다.
    select
        CONVERT_TIMEZONE('Asia/Seoul', loaded_at) as loaded_at_kst,
        search_date,
        raw_data:Date::STRING as trade_date_raw,
        raw_data:Ticker::STRING as ticker,
        raw_data:Open::FLOAT as open_price,
        raw_data:High::FLOAT as high_price,
        raw_data:Low::FLOAT as low_price,
        raw_data:Close::FLOAT as close_price,
        raw_data:Volume::BIGINT as volume -- Volume은 숫자가 클 수 있어 BIGINT 권장
    from raw_source
)

select
    loaded_at_kst,
    search_date,
    to_date(trade_date_raw, 'YYYY-MM-DD') as trade_date,
    ticker,
    open_price,
    high_price,
    low_price,
    close_price as usd_close_price,
    volume
from parsed_data
-- 동일 날짜, 동일 종목 중복 제거 (Idempotency 보장)
qualify row_number() over(partition by trade_date, ticker order by loaded_at_kst desc) = 1