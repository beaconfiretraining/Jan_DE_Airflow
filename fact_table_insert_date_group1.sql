MERGE INTO AIRFLOW0124.BF_DEV.FACT_STOCK_HISTORY AS target
USING US_STOCK_DAILY.DCCM.STOCK_HISTORY AS source
ON target.SYMBOL = source.SYMBOL
and target.date = source.date
  /*
WHEN MATCHED AND (
  target.open <> source.open OR
  target.high <> source.high OR
  target.low <> source.low OR
  target.close <> source.close OR
  target.volume <> source.volume OR
  target.adjclose <> source.adjclose
  -- Add other columns as needed
) THEN
  UPDATE SET
    target.open = source.open,
    target.high = source.high,
    target.low = source.low,
    target.close = source.close,
    target.volume = source.volume,
    target.adjclose = source.adjclose
    -- Update other columns as needed
*/
WHEN NOT MATCHED THEN
  INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
  VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE);
