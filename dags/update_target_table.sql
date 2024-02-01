-- update target table

merge into fact_stock_history_group3 t
using SOURCE_STOCK_HISTORY_G3 s
on t.SYMBOL = s.SYMBOL
when matched and concat(s.DATE,'|', s.OPEN,'|',s.HIGH,'|',s.LOW,'|', s.CLOSE,'|',s.VOLUME,'|',
s.ADJCLOSE) <> concat(t.DATE,'|', t.OPEN,'|',t.HIGH,'|',t.LOW,'|', t.CLOSE,'|',t.VOLUME,'|',
t.ADJCLOSE) then
update set s.DATE = t.DATE, s.OPEN = t.OPEN, s.HIGH = t.HIGH,
s.LOW = t.LOW, s.CLOSE = t.CLOSE, s.VOLUME = t.VOLUME,s.ADJCLOSE =t.ADJCLOSE

when not matched
then 
insert (SYMBOL,DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,ADJCLOSE)
values (s.SYMBOL,s.DATE,s.OPEN,s.HIGH,s.LOW,s.CLOSE,s.VOLUME,s.ADJCLOSE);




