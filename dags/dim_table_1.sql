create or replace table DIM_SYMBOL_G2 as
    (select ID
        , SYMBOL
        , PRICE
        , BETA
        , MKTCAP
        , LASTDIV
        , CHANGES
        , EXCHANGE
        , INDUSTRY
        , WEBSITE
        , DESCRIPTION
        , CEO
        , SECTOR
        , DCFDIFF
        , DCF 
    from "US_STOCK_DAILY"."DCCM"."SYMBOLS");
alter table DIM_SYMBOL_G2
add primary key (symbol);