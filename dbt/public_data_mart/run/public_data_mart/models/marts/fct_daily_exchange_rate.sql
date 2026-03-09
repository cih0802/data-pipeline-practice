-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
        
    

    

    merge into sandbox.public_data_mart_dev.fct_daily_exchange_rate as DBT_INTERNAL_DEST
        using sandbox.public_data_mart_dev.fct_daily_exchange_rate__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.base_date = DBT_INTERNAL_DEST.base_date
                ) and (
                    DBT_INTERNAL_SOURCE.currency_code = DBT_INTERNAL_DEST.currency_code
                )

    
    when matched then update set
        "LOADED_AT_KST" = DBT_INTERNAL_SOURCE."LOADED_AT_KST","BASE_DATE" = DBT_INTERNAL_SOURCE."BASE_DATE","CURRENCY_CODE" = DBT_INTERNAL_SOURCE."CURRENCY_CODE","CURRENCY_NAME" = DBT_INTERNAL_SOURCE."CURRENCY_NAME","BASE_RATE" = DBT_INTERNAL_SOURCE."BASE_RATE","BOOK_PRICE" = DBT_INTERNAL_SOURCE."BOOK_PRICE","TTB" = DBT_INTERNAL_SOURCE."TTB","TTS" = DBT_INTERNAL_SOURCE."TTS"
    

    when not matched then insert
        ("LOADED_AT_KST", "BASE_DATE", "CURRENCY_CODE", "CURRENCY_NAME", "BASE_RATE", "BOOK_PRICE", "TTB", "TTS")
    values
        ("LOADED_AT_KST", "BASE_DATE", "CURRENCY_CODE", "CURRENCY_NAME", "BASE_RATE", "BOOK_PRICE", "TTB", "TTS")

;
    commit;