-- update target table

merge into FACT_STOCK_HISTORY_GROUP3 t
using SOURCE_STOCK_HISTORY_G3 s
on t.SYMBOL = s.SYMBOL
when matched and concat(s.DATE,'|', s.OPEN,'|',s.HIGH,'|',s.LOW,'|', s.CLOSE,'|',s.VOLUME,'|',
s.ADJCLOSE) <> concat(t.DATE,'|', t.OPEN,'|',t.HIGH,'|',t.LOW,'|', t.CLOSE,'|',t.VOLUME,'|',
t.ADJCLOSE) then
update set t.DATE = s.DATE, t.OPEN = s.OPEN, t.HIGH = s.HIGH,
t.LOW = s.LOW, t.CLOSE = s.CLOSE, t.VOLUME = s.VOLUME,t.ADJCLOSE =s.ADJCLOSE

when not matched
then 
insert (SYMBOL,DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,ADJCLOSE)
values (s.SYMBOL,s.DATE,s.OPEN,s.HIGH,s.LOW,s.CLOSE,s.VOLUME,s.ADJCLOSE);




