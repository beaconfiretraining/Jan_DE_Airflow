-- update fact stock history table
merge into FACT_STOCK_HISTORY_GROUP3 t
using SOURCE_STOCK_HISTORY_G3 s
on t.SYMBOL = s.SYMBOL
when matched and s.DATE <> t.DATE then
update set t.DATE = s.DATE, t.OPEN = s.OPEN, t.HIGH = s.HIGH,
t.LOW = s.LOW, t.CLOSE = s.CLOSE, t.VOLUME = s.VOLUME,t.ADJCLOSE =s.ADJCLOSE

when not matched
then 
insert (SYMBOL,DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,ADJCLOSE)
values (s.SYMBOL,s.DATE,s.OPEN,s.HIGH,s.LOW,s.CLOSE,s.VOLUME,s.ADJCLOSE);


-- update dim symbol table
merge into DIM_SYMBOLS_GROUP3 t
using SOURCE_SYMBOLS_G3 s
on t.SYMBOL = s.SYMBOL

when matched and concat(s.NAME, '|', s.EXCHANGE)<> concat(t.NAME, '|', t.EXCHANGE) then

update set t.NAME = s.NAME, t.EXCHANGE = s.EXCHANGE

when not matched
then 
insert (SYMBOL,NAME,EXCHANGE)
values (s.SYMBOL,s.NAME,s.EXCHANGE);

-- update dim company table

merge into DIM_COMPANY_PROFILE_GROUP3 t
using SOURCE_COMPANY_PROFILE_G3 s
on t.ID = s.ID

when matched and concat(s.SYMBOL, '|', s.PRICE)<> concat(t.SYMBOL, '|', t.PRICE) then

update set t.SYMBOL = s.SYMBOL, t.PRICE = s.PRICE

when not matched
then 
insert (ID,SYMBOL,PRICE,BETA,VOLAVG,MKTCAP,LASTDIV,RANGE,CHANGES,COMPANYNAME,EXCHANGE,
INDUSTRY,WEBSITE,DESCRIPTION,CEO,SECTOR,DCFDIFF,DCF)
values (s.ID,s.SYMBOL,s.PRICE,s.BETA,s.VOLAVG,s.MKTCAP,s.LASTDIV,s.RANGE,s.CHANGES,
s.COMPANYNAME,s.EXCHANGE,
s.INDUSTRY,s.WEBSITE,s.DESCRIPTION,s.CEO,s.SECTOR,s.DCFDIFF,s.DCF);
