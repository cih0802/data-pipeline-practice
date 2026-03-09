

with staging as (
    select * from sandbox.public_data_mart_dev.stg_exchange_rate
    
    
    -- 증분 실행 시, 현재 테이블에 적재된 가장 최신 날짜(search_date 기준) 이후 데이터만 필터링
    where search_date >= (select coalesce(max(base_date), '19000101') from sandbox.public_data_mart_dev.fct_daily_exchange_rate)
    
),

cleaned_data as (
    select
        loaded_at_kst,
        search_date as base_date,
        currency_code,
        currency_name,
        -- [수정] 상위 stg 모델에서 이미 가공된 컬럼명을 직접 참조 (중복 연산 제거)
        base_rate,
        bkpr as book_price,
        ttb,
        tts
    from staging
    -- stg 모델에서 처리한 result_code가 1인 정상 데이터만 사용
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
qualify row_number() over(partition by base_date, currency_code order by loaded_at_kst desc) = 1