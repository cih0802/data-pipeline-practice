-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
        
    

    

    merge into sandbox.public_data_mart_dev.fct_daily_investment_metrics as DBT_INTERNAL_DEST
        using sandbox.public_data_mart_dev.fct_daily_investment_metrics__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.trade_date = DBT_INTERNAL_DEST.trade_date
                ) and (
                    DBT_INTERNAL_SOURCE.ticker = DBT_INTERNAL_DEST.ticker
                )

    
    when matched then update set
        "TRADE_DATE" = DBT_INTERNAL_SOURCE."TRADE_DATE","TICKER" = DBT_INTERNAL_SOURCE."TICKER","USD_CLOSE_PRICE" = DBT_INTERNAL_SOURCE."USD_CLOSE_PRICE","USD_KRW_RATE" = DBT_INTERNAL_SOURCE."USD_KRW_RATE","KRW_CLOSE_PRICE" = DBT_INTERNAL_SOURCE."KRW_CLOSE_PRICE","USD_DAILY_RETURN_PCT" = DBT_INTERNAL_SOURCE."USD_DAILY_RETURN_PCT","KRW_DAILY_RETURN_PCT" = DBT_INTERNAL_SOURCE."KRW_DAILY_RETURN_PCT"
    

    when not matched then insert
        ("TRADE_DATE", "TICKER", "USD_CLOSE_PRICE", "USD_KRW_RATE", "KRW_CLOSE_PRICE", "USD_DAILY_RETURN_PCT", "KRW_DAILY_RETURN_PCT")
    values
        ("TRADE_DATE", "TICKER", "USD_CLOSE_PRICE", "USD_KRW_RATE", "KRW_CLOSE_PRICE", "USD_DAILY_RETURN_PCT", "KRW_DAILY_RETURN_PCT")

;
    commit;