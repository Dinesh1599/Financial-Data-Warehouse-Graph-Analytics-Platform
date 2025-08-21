EXPLAIN PLAN FOR
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) txn_day, currency,
       SUM(amount) total_amt, COUNT(*) txn_cnt
FROM fact_txn
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency;

SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\plan_daily_before.txt'
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());
SPOOL OFF


EXPLAIN PLAN FOR
SELECT t.txn_id, t.currency txn_ccy, s.currency src_ccy, d.currency dest_ccy
FROM fact_txn t
JOIN dim_account s ON s.account_id = t.src_account_id
JOIN dim_account d ON d.account_id = t.dest_account_id
WHERE (t.currency != s.currency OR t.currency != d.currency);

SPOOL 'D:\codes\Data Engineer\FinancialTxn\oracle\perf\plan_mismatch_before.txt'
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());
SPOOL OFF

