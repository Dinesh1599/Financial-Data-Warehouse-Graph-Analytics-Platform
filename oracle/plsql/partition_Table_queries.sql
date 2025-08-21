DESC fact_txn;

ALTER TABLE fact_txn RENAME TO fact_txn_old;


-- Adjust column list to match your actual schema.
CREATE TABLE fact_txn (
  txn_id          VARCHAR2(30) PRIMARY KEY,
  src_account_id  VARCHAR2(20) NOT NULL,
  dest_account_id VARCHAR2(20) NOT NULL,
  amount          NUMBER(18,2),
  currency        VARCHAR2(10),
  channel         VARCHAR2(30),
  timestamp       TIMESTAMP,            -- replace if needed
  status          VARCHAR2(20)
)
PARTITION BY RANGE (timestamp)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
  PARTITION p_start VALUES LESS THAN (TIMESTAMP '2020-01-01 00:00:00')
);


--Adding fact_txn_old into fact_txn
INSERT INTO fact_txn
SELECT * FROM fact_txn_old;
COMMIT;





-- This prints a ready-to-run SQL that UNION ALLs partition counts
SET PAGESIZE 0 LINESIZE 200 TRIMSPOOL ON
SELECT 'SELECT '''||partition_name||''' AS partition_name, COUNT(*) AS cnt '||
       'FROM fact_txn PARTITION ('||partition_name||')'
FROM   user_tab_partitions
WHERE  table_name = 'FACT_TXN'
ORDER  BY partition_position;

-- Copy the output, add:
-- ORDER BY partition_name;
-- …and run it to get a single result set with all partition counts.
SELECT 'P_START' AS partition_name, COUNT(*) AS cnt FROM fact_txn PARTITION (P_START);
SELECT 'SYS_P394' AS partition_name, COUNT(*) AS cnt FROM fact_txn PARTITION (SYS_P394);
SELECT * FROM fact_txn PARTITION (SYS_P396);;



SELECT * FROM fact_txn;

