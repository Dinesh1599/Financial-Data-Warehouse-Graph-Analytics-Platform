SET TIMING ON
SET PAGESIZE 50000
SET LINESIZE 200
SET SERVEROUTPUT ON

-- Save daily summary timing
SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\baseline_daily_output.txt'
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) AS txn_day,  
       currency,
       SUM(amount) AS total_amt,
       COUNT(*)    AS txn_cnt
FROM fact_txn_old
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency
ORDER BY txn_day, txn_cnt DESC;
SPOOL OFF

-- Save currency mismatch timing
SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\baseline_mismatch_output.txt'
SELECT t.txn_id, t.currency txn_ccy, s.currency src_ccy, d.currency dest_ccy
FROM fact_txn_old t
JOIN dim_account s ON s.account_id = t.src_account_id
JOIN dim_account d ON d.account_id = t.dest_account_id
WHERE (t.currency != s.currency OR t.currency != d.currency);
SPOOL OFF


EXPLAIN PLAN FOR
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) txn_day, currency,
       SUM(amount) total_amt, COUNT(*) txn_cnt
FROM fact_txn_old
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency;

SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\plan_daily_before.txt'
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());
SPOOL OFF


EXPLAIN PLAN FOR
SELECT t.txn_id,
       t.currency   AS txn_ccy,
       s.currency   AS src_ccy,
       d.currency   AS dest_ccy,
       t.timestamp
FROM fact_txn_old t
JOIN dim_account s ON s.account_id = t.src_account_id
JOIN dim_account d ON d.account_id = t.dest_account_id
WHERE (t.currency != s.currency OR t.currency != d.currency)
  AND t.timestamp BETWEEN TIMESTAMP '1970-01-01 00:00:00'
                        AND TIMESTAMP '2023-09-12 23:59:59';

SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\plan_mismatch_before.txt'
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());
SPOOL OFF
