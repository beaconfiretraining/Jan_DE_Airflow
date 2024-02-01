drop if exists fact_stock_history_test_group3;
create or replace table fact_stock_history_test_group3 as
(select * from "US_STOCK_DAILY"."DCCM"."STOCK_HISTORY");
-- alter table fact_stock_history_group3
-- add primary key (symbol);

-- drop if exists dim_symbols_group3;
-- create or replace table dim_symbols_group3 as
-- (select * from "US_STOCK_DAILY"."DCCM"."SYMBOLS");
-- alter table dim_symbols_group3
-- add primary key (symbol);
-- -- foreign key (symbol) references fact_stock_history_group3(symbol);

-- drop if exists dim_company_profile_group3;
-- create or replace table dim_company_profile_group3 as
-- (select * from "US_STOCK_DAILY"."DCCM"."COMPANY_PROFILE");
-- alter table dim_company_profile_group3
-- add primary key (ID);
-- -- foreign key (symbol) references fact_stock_history_group3(symbol);
