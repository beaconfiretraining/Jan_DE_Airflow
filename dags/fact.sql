create TRANSIENT table if not exists fact_STOCK_HISTORY_group5 as
(select *
from US_STOCK_DAILY.DCCM.STOCK_HISTORY );