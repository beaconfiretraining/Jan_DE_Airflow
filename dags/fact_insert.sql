INSERT into fact_STOCK_HISTORY_group5 (
    select * from US_STOCK_DAILY.DCCM.STOCK_HISTORY
    where date = current_date()
);