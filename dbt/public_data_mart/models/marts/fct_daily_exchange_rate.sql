-- L2 계층: Incremental 전략 적용 및 unique_key 설정
{{ config(
    materialized='incremental',
    unique_key=['base_date', 'currency_code']
) }}

with staging as (
    select * from {{ ref('stg_exchange_rate') }}
    
    {% if is_incremental() %}
    -- 증분 실행 시, 현재 테이블에 적재된 가장 최신 날짜 이후의 데이터만 필터링
    where search_date >= (select coalesce(max(base_date), '19000101') from {{ this }})
    {% endif %}
),

cleaned_data as (
    select
        CONVERT_TIMEZONE('Asia/Seoul', loaded_at) as loaded_at_kst,
        search_date as base_date,
        currency_code,
        currency_name,
        try_cast(replace(base_rate_raw, ',', '') as numeric(10, 2)) as base_rate,
        try_cast(replace(bkpr_raw, ',', '') as numeric(10, 2)) as book_price,
        try_cast(replace(ttb_raw, ',', '') as numeric(10, 2)) as ttb,
        try_cast(replace(tts_raw, ',', '') as numeric(10, 2)) as tts
    from staging
    where result_code = 1 
)

select
    loaded_at_kst,
    base_date,
    currency_code,
    currency_name,
    base_rate,
    book_price,
    ttb,
    tts
from cleaned_data
-- 하루에 여러 번 적재되었을 경우, 가장 마지막에 적재된(loaded_at_kst desc) 1건만 남김
qualify row_number() over(partition by base_date, currency_code order by loaded_at_kst desc) = 1
-- order by base_date, currency_code