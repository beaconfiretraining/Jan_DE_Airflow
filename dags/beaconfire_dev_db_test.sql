create or replace table fact_stock_history_group3(
    SYMBOL  VARCHAR(16) PRIMARY KEY,
    DATE  DATE,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW  NUMBER(18,8),
    CLOSE  NUMBER(18,8),
    VOLUME  NUMBER(38,8),
    ADJCLOSE  NUMBER(18,8)
);
insert into fact_stock_history_group3
select *
from "US_STOCK_DAILY"."DCCM"."STOCK_HISTORY";

create or replace table dim_symbols_group3(
    SYMBOL  VARCHAR(16) PRIMARY KEY,
    NAME VARCHAR(256),
    EXCHANGE VARCHAR(64),
    foreign key (symbol) references fact_stock_history_group3(symbol)
);
insert into dim_symbols_group3
select *
from "US_STOCK_DAILY"."DCCM"."SYMBOLS";

create or replace table dim_company_profile_group3(
    ID NUMBER(38,0)  PRIMARY KEY,
    SYMBOL  VARCHAR(16),
    PRICE NUMBER(18,8),
    BETA NUMBER (18,8),
    VOLAVG NUMBER(38,0),
    MKTCAP NUMBER(38,0),
    LASTDIV NUMBER(18,8),
    RANGE VARCHAR(64),
    CHANGES NUMBER(18,8),
    COMPANYNAME VARCHAR(512),
    EXCHANGE VARCHAR(64),
    INDUSTRY VARCHAR(64),
    WEBSITE VARCHAR(64),
    DESCRIPTION VARCHAR(2048),
    CEO VARCHAR(64),
    SECTOR VARCHAR(64),
    DCFDIFF NUMBER(18,8),
    DCF NUMBER(18,8),
    foreign key (symbol) references fact_stock_history_group3(symbol)
);
insert into dim_company_profile_group3
select *
from "US_STOCK_DAILY"."DCCM"."COMPANY_PROFILE";


CREATE or replace TABLE "AIRFLOW0124"."BF_DEV"."SOURCE_STOCK_HISTORY_G3" 
AS
SELECT 
  * 
FROM "US_STOCK_DAILY"."DCCM"."STOCK_HISTORY";

CREATE or replace TABLE "AIRFLOW0124"."BF_DEV"."SOURCE_SYMBOLS_G3" 
AS
SELECT 
  * 
FROM "US_STOCK_DAILY"."DCCM"."SYMBOLS";

CREATE or replace TABLE "AIRFLOW0124"."BF_DEV"."SOURCE_COMPANY_PROFILE_G3" 
AS
SELECT 
  * 
FROM "US_STOCK_DAILY"."DCCM"."COMPANY_PROFILE";