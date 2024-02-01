MERGE INTO DIM_COMPANY_GROUP1 AS target
USING (select s.name, p.*
from “US_STOCK_DAILY”.“DCCM”.“SYMBOLS” s
join “US_STOCK_DAILY”.“DCCM”.“COMPANY_PROFILE” p
on s.symbol = p.symbol) AS source
ON target.SYMBOL = source.SYMBOL
WHEN MATCHED AND (
    target.NAME <> source.NAME OR
    target.ID <> source.ID OR
    target.PRICE <> source.PRICE OR
    target.BETA <> source.BETA OR
    target.VOLAVG <> source.VOLAVG OR
    target.MKTCAP <> source.MKTCAP OR
    target.LASTDIV <> source.LASTDIV OR  
    target.RANGE <>source.RANGE OR
    target.CHANGES <> source.CHANGES OR
    target.COMPANYNAME <> source.COMPANYNAME OR
    target.EXCHANGE <> source.EXCHANGE OR
    target.INDUSTRY <> source.INDUSTRY OR
    target.WEBSITE <> source.WEBSITE OR
    target.DESCRIPTION <> source.DESCRIPTION OR
    target.CEO <> source.CEO OR
    target.SECTOR <> source.SECTOR OR
    target.DCFDIFF <> source.DCFDIFF OR
    target.DCF <> source.DCF
  -- Add other columns as needed
) THEN
  UPDATE SET
    target.NAME = source.NAME,
    target.ID = source.ID,
    target.PRICE = source.PRICE,
    target.BETA = source.BETA,
    target.VOLAVG = source.VOLAVG,
    target.MKTCAP = source.MKTCAP,
    target.LASTDIV = source.LASTDIV,
    target.RANGE = source.RANGE,
    target.CHANGES = source.CHANGES,
    target.COMPANYNAME = source.COMPANYNAME,
    target.EXCHANGE = source.EXCHANGE,
    target.INDUSTRY = source.INDUSTRY,
    target.WEBSITE = source.WEBSITE,
    target.DESCRIPTION = source.DESCRIPTION,
    target.CEO = source.CEO,
    target.SECTOR = source.SECTOR,
    target.DCFDIFF = source.DCFDIFF,
    target.DCF = source.DCF
    -- Update other columns as needed
WHEN NOT MATCHED THEN
  INSERT (ID,SYMBOL,PRICE,BETA,VOLAVG,MKTCAP,LASTDIV,RANGE,CHANGES,COMPANYNAME,EXCHANGE,INDUSTRY,WEBSITE,DESCRIPTION,CEO,SECTOR,DCFDIFF,DCF)
  VALUES (SOURCE.ID,SOURCE.SYMBOL,SOURCE.PRICE,SOURCE.BETA,SOURCE.VOLAVG,SOURCE.MKTCAP,SOURCE.LASTDIV,SOURCE.RANGE,SOURCE.CHANGES,SOURCE.COMPANYNAME,SOURCE.EXCHANGE,
SOURCE.INDUSTRY,SOURCE.WEBSITE,SOURCE.DESCRIPTION,SOURCE.CEO,SOURCE.SECTOR,SOURCE.DCFDIFF,SOURCE.DCF);

