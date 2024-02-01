-- drop if exists fact_stock_history_test_group3;
-- create or replace table fact_stock_history_test_group3 as
-- (select * from "US_STOCK_DAILY"."DCCM"."STOCK_HISTORY");
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

create or replace table fact_stock_history_test_group3(
    SYMBOL  VARCHAR(16) PRIMARY KEY,
    DATE  DATE,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW  NUMBER(18,8),
    CLOSE  NUMBER(18,8),
    VOLUME  NUMBER(38,8),
    ADJCLOSE  NUMBER(18,8)
);
insert into fact_stock_history_test_group3
select *
from "US_STOCK_DAILY"."DCCM"."STOCK_HISTORY";