--currency of then txn that does not match the accounts involved
SELECT t.txn_id, t.currency txn_ccy, s.currency src_ccy, d.currency dest_ccy
FROM fact_txn t
JOIN dim_account s ON s.account_id = t.src_account_id
JOIN dim_account d ON d.account_id = t.dest_account_id
WHERE (t.currency != s.currency OR t.currency != d.currency);

--daily summary of transactions per currency
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) AS txn_day,
       currency,
       SUM(amount) AS total_amt,
       COUNT(*)    AS txn_cnt
FROM fact_txn
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency
ORDER BY txn_day, txn_cnt DESC;

--Explain Plan for regular simple query
EXPLAIN PLAN FOR
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) txn_day, currency,
       SUM(amount) total_amt, COUNT(*) txn_cnt
FROM fact_txn
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Explain plan with joins
EXPLAIN PLAN FOR
SELECT t.txn_id, t.currency txn_ccy, s.currency src_ccy, d.currency dest_ccy
FROM fact_txn t
JOIN dim_account s ON s.account_id = t.src_account_id
JOIN dim_account d ON d.account_id = t.dest_account_id
WHERE (t.currency != s.currency OR t.currency != d.currency);

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);



