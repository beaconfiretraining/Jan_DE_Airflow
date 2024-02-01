create or replace table DIM_COMPANY_G2 as
(select * from "US_STOCK_DAILY"."DCCM"."COMPANY_PROFILE");
alter table DIM_COMPANY_G2
add primary key (symbol);