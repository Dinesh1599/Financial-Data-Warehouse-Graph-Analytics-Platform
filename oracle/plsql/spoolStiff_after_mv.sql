SET TIMING ON
SET PAGESIZE 50000
SET LINESIZE 200
SET SERVEROUTPUT ON

ALTER SESSION SET QUERY_REWRITE_ENABLED = TRUE;

EXPLAIN PLAN FOR
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) AS txn_day,
       currency,
       SUM(amount) AS total_amt,
       COUNT(*)    AS txn_cnt 
FROM MV_TXN_DAILY
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency;

SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\plan_daily_after_mv.txt'
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());
SPOOL OFF

ALTER SESSION SET QUERY_REWRITE_ENABLED = TRUE;
EXPLAIN PLAN FOR
SELECT t.txn_id,
       t.currency   AS txn_ccy,
       s.currency   AS src_ccy,
       d.currency   AS dest_ccy,
       t.timestamp
FROM fact_txn t
JOIN dim_account s ON s.account_id = t.src_account_id
JOIN dim_account d ON d.account_id = t.dest_account_id
WHERE (t.currency != s.currency OR t.currency != d.currency)
  AND t.timestamp BETWEEN TIMESTAMP '1970-01-01 00:00:00'
                        AND TIMESTAMP '2023-09-12 23:59:59';

SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\plan_mismatch_after_mv.txt'
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());
SPOOL OFF
