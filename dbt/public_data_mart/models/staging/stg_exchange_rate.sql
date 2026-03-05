-- L1 계층은 보통 View로 생성하여 저장 공간을 절약하고 최신 로직을 반영합니다.
{{ config(materialized='view') }}

with raw_source as (
    select
        raw_data,
        loaded_at,
        search_date
    from {{ source('bronze_layer', 'raw_exchange_rate') }}
),

parsed_data as (
    -- Snowflake의 lateral flatten을 사용하여 JSON 배열을 행으로 펼칩니다.
    -- 현재 json 구조에선 단일 json 오브젝트여서 flatten을 할 필요가 없습니다.
    -- flatten 제외에 따라 기존 f.value로 구성된 컬럼을 raw_data로 변경했습니다.
    -- API 응답 구조에 따라 f.value 경로를 조정해야 할 수 있습니다.
    SELECT
        loaded_at,
        search_date,
        raw_data:result::INT AS result_code,
        raw_data:cur_unit::STRING AS currency_code,
        raw_data:cur_nm::STRING AS currency_name,
        raw_data:deal_bas_r::STRING AS base_rate_raw,
        raw_data:bkpr::STRING AS bkpr_raw,
        raw_data:ttb::STRING AS ttb_raw,
        raw_data:tts::STRING AS tts_raw
    FROM raw_source
    -- , LATERAL FLATTEN(input => raw_data) f
)

select * from parsed_data
