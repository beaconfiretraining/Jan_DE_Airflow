CREATE OR REPLACE TRANSIENT TABLE dim_STOCK_HISTORY_group5 as (
    select * 
    from US_STOCK_DAILY.DCCM.COMPANY_PROFILE
    full outer join US_STOCK_DAILY.DCCM.SYMBOLS using (symbol, exchange));