create or replace table DIM_COMPANY_G2 as
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
     from "US_STOCK_DAILY"."DCCM"."COMPANY_PROFILE");
alter table DIM_COMPANY_G2
add primary key (symbol);