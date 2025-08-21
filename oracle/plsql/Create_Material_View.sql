-- Log must include every column used by the MV select/group by
CREATE MATERIALIZED VIEW LOG ON fact_txn
WITH ROWID ( timestamp, currency, amount )
INCLUDING NEW VALUES;


CREATE MATERIALIZED VIEW mv_txn_daily
BUILD IMMEDIATE  --Creates immediately
REFRESH FAST ON DEMAND  --keep it up-to-date, but only when asked, and only by applying changes (not full rebuild).
ENABLE QUERY REWRITE -- anytime txn_daily is called the table is replaced with mv_txn_daily
AS
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) AS txn_day,
       currency,
       SUM(amount) AS total_amt,
       COUNT(*)    AS txn_cnt
FROM fact_txn
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency;

BEGIN
  DBMS_MVIEW.REFRESH('MV_TXN_DAILY', METHOD => 'F');  -- F = FAST  Apply refresh
END;
/

CREATE INDEX ix_mv_txnday_ccy ON mv_txn_daily (txn_day, currency);

SELECT txn_day, currency, total_amt, txn_cnt
FROM mv_txn_daily
ORDER BY txn_day, txn_cnt DESC;



ALTER SESSION SET QUERY_REWRITE_ENABLED = TRUE;

-- Your original query (no change)
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) AS txn_day,
       currency,
       SUM(amount) AS total_amt,
       COUNT(*)    AS txn_cnt
FROM fact_txn
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency
ORDER BY txn_day, txn_cnt DESC;