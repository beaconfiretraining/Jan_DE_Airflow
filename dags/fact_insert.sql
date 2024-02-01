INSERT into fact_STOCK_HISTORY_group5 (
    select * from STOCK_HISTORY
    where date = current_date()
);