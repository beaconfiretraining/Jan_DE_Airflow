create or replace transient table FACT_STOCK_G2 (
    KEY  VARCHAR(250) PRIMARY KEY,
    SYMBOL  VARCHAR(16),
    DATE  DATE,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW  NUMBER(18,8),
    CLOSE  NUMBER(18,8),
    VOLUME  NUMBER(38,8),
    ADJCLOSE  NUMBER(18,8),
    FOREIGN KEY (SYMBOL) references DIM_COMPANY_G2 (SYMBOL),
    FOREIGN KEY (SYMBOL) references DIM_SYMBOL_G2 (SYMBOL)
);

insert into FACT_STOCK_G2 
values (select concat(symbol,date)as key, * from "US_STOCK_DAILY"."DCCM"."STOCK_HISTORY");



