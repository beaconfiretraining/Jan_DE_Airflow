create or replace table DIM_SYMBOL_G2 as
    (select * from "US_STOCK_DAILY"."DCCM"."SYMBOLS");
alter table DIM_SYMBOL_G2
add primary key (symbol);