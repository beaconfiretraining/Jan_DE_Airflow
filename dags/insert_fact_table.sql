INSERT INTO FACT_Stock_G2 (
    select *
    from "US_STOCK_DAILY"."DCCM"."STOCK_HISTORY"
    where DATE = CURRENT_DATE
);
