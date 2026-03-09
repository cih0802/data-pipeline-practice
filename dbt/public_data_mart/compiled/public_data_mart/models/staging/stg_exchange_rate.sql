

with parsed_data as (
    select
        CONVERT_TIMEZONE('Asia/Seoul', loaded_at) as loaded_at_kst,
        search_date,
        f.value:result::INT as result_code,
        f.value:cur_unit::STRING as currency_code,
        f.value:cur_nm::STRING as currency_name,
        -- 콤마(,) 제거 후 숫자로 변환 (예: "1,350.5" -> 1350.5)
        replace(f.value:deal_bas_r::STRING, ',', '')::FLOAT as base_rate,
        replace(f.value:ttb::STRING, ',', '')::FLOAT as ttb,
        replace(f.value:tts::STRING, ',', '')::FLOAT as tts,
        replace(f.value:bkpr::STRING, ',', '')::FLOAT as bkpr
    from sandbox.bronze.raw_exchange_rate,
    lateral flatten(input => raw_data) f
)

select *
from parsed_data
-- 동일 날짜, 동일 통화 중복 제거 (Idempotency 보장)
qualify row_number() over(partition by search_date, currency_code order by loaded_at_kst desc) = 1