CREATE OR REPLACE TRANSIENT TABLE dim_STOCK_HISTORY_group5 as (
    select * 
    from COMPANY_PROFILE
    full outer join SYMBOLS using (symbol, exchange));