MERGE INTO FACT_STOCK_HISTORY_GROUP1 AS target
USING (
  SELECT
    SYMBOL,
    DATE,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    ADJCLOSE,
    ROW_NUMBER() OVER(PARTITION BY SYMBOL, DATE ORDER BY (SELECT NULL)) as rn -- Arbitrarily ordering
  FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY
) AS source
ON target.SYMBOL = source.SYMBOL AND target.date = source.date
WHEN MATCHED AND (
  target.open <> source.open OR
  target.high <> source.high OR
  target.low <> source.low OR
  target.close <> source.close OR
  target.volume <> source.volume OR
  target.adjclose <> source.adjclose
) THEN UPDATE SET
  target.open = source.open,
  target.high = source.high,
  target.low = source.low,
  target.close = source.close,
  target.volume = source.volume,
  target.adjclose = source.adjclose
WHEN NOT MATCHED AND source.rn = 1 THEN INSERT (
  SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
) VALUES (
  source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE
);
