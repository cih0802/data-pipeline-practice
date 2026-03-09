

with parsed_data as (
    select
        CONVERT_TIMEZONE('Asia/Seoul', loaded_at) as loaded_at_kst,
        search_date,
        to_date(f.value:Date::STRING, 'YYYY-MM-DD') as trade_date,
        f.value:Ticker::STRING as ticker,
        f.value:Open::FLOAT as open_price,
        f.value:High::FLOAT as high_price,
        f.value:Low::FLOAT as low_price,
        f.value:Close::FLOAT as usd_close_price,
        f.value:Volume::BIGINT as volume
    from sandbox.bronze.raw_etf,
    lateral flatten(input => raw_data) f
)

select *
from parsed_data
-- 동일 날짜, 동일 종목 중복 제거 (Idempotency 보장)
qualify row_number() over(partition by trade_date, ticker order by loaded_at_kst desc) = 1