-- L2 계층은 조회 성능을 위해 실제 Table 형태로 물리적으로 생성합니다.
{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_exchange_rate') }}
),

cleaned_data as (
    select
        -- 타임스탬프에서 날짜만 추출
        date(loaded_at) as loaded_at,
        search_date as base_date,
        currency_code,
        currency_name,
        -- 환율 데이터에 포함된 콤마(,)를 제거한 후 숫자형으로 안전하게 변환
        try_cast(replace(base_rate_raw, ',', '') as numeric(10, 2)) as base_rate,
        try_cast(replace(bkpr_raw, ',', '') as numeric(10, 2)) as book_price,
        try_cast(replace(ttb_raw, ',', '') as numeric(10, 2)) as ttb,
        try_cast(replace(tts_raw, ',', '') as numeric(10, 2)) as tts
    from staging
    where result_code = 1 -- API 정상 호출(코드 1) 데이터만 필터링
)

select
    loaded_at,
    base_date,
    currency_code,
    currency_name,
    base_rate,
    book_price,
    ttb,
    tts
from cleaned_data
-- 하루에 API를 여러 번 호출해 중복 데이터가 생겼을 경우, 최신 적재 데이터 1건만 남김
qualify row_number() over(partition by base_date, currency_code order by base_date desc) = 1
