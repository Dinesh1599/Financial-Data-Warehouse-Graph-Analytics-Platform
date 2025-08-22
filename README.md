# 🔗 Neo4j + Oracle Data Platform

## 📌 Project Summary
- **What it is:** a dual‑target pipeline that loads clean financial data into **Oracle** (star schema) and **Neo4j** (graph), then demonstrates **Oracle tuning** (partitioning, indexes, MV) with **before/after evidence**.
- **Why it’s relevant:** mirrors common enterprise patterns in financial data platforms; aligns with skills like **Oracle performance**, **AWR/Advisors mindset**, and **scalable data infrastructure**.
- **What you’ll see:** reproducible setup via Docker, named‑bind pandas loaders, SQL Developer workflow for baselines, and graph queries for relationship insights.

The workflow covers:
- **Data Cleaning & ETL** with Python + Pandas  
- **Data Loading** into Oracle (fact/dim schema with constraints, partitions, indexes)  
- **Graph Modeling** in Neo4j (customers, accounts, transactions)  
- **Query Performance Tuning** with Partitioning, Indexes, and Materialized Views  
- **Orchestration** with Apache Airflow (to automate the entire process end-to-end)  

This mirrors real-world Data Engineering + Oracle DBA responsibilities, with hands-on practice in performance optimization and query analysis.

---

## 🏗️ Architecture

```
                ┌───────────────┐
                │   Raw CSV     │
                │ (cust/acc/tx) │
                └──────┬────────┘
                       │
                 Python + Pandas
                       │
        ┌──────────────┴────────────────┐
        │                               │
   Oracle XE (SQL)                  Neo4j (Graph)
  - Fact/Dim Schema                 - Customer → Account → Txn
  - Partitioning, Indexes           - Country relationships
  - Materialized Views              - Fraud/AML detection patterns
        │                               │
        └──────────────┬────────────────┘
                       │
                 Apache Airflow DAG
            (Automated ETL + Loading Flow)
```

---

## ⚙️ Project Setup

### 1. Prerequisites
- **Docker Desktop** (for Oracle XE container)
- **Neo4j Desktop/Server** (local instance)
- **Python 3.10+**
- **Apache Airflow 3.0.3**
- **SQL Developer** (GUI for Oracle)

### 2. Folder Structure
```
.
├── RAW/                  # Raw CSV input
│   ├── customer/
│   ├── account/
│   └── txn/
├── clean/                # Cleaned CSV outputs
│   ├── customer/
│   ├── account/
│   └── txn/
├── oracle/
│   ├── schema/           # DDL scripts (tables, partitions, indexes)
│   ├── python/           # Load scripts with pandas + oracledb
│   └── perf/             # Baseline & tuned query results
├── neo4j/                # Cypher queries, constraints, graph model
├── airflow/              # DAGs for orchestration
└── README.md             # Documentation
```

### 3. Tech Stack

- **Databases:** Oracle XE 21c (Docker), Neo4j (local)
- **Languages/Libraries:** Python 3, pandas, `oracledb`, `python-dotenv`
- **Orchestration (optional):** Apache Airflow
- **Containers:** Docker Desktop (Windows WSL2)
- **Tools:** SQL Developer (for plans/timings), Neo4j Browser

---

## 🧹 Step 1: Data Cleaning
- Used **Python + Pandas** scripts (`script/cleanCustomer.py`, etc.)
- Handled missing values (`NaN → None`), normalized phone numbers, standardized date formats.
- Output stored in `/clean/`.

---

## 🗄️ Step 2: Oracle XE Setup
- Oracle XE 21c containerized with Docker:
  ```bash
  docker run -d -p 1521:1521 -e ORACLE_PASSWORD=oracle gvenzl/oracle-xe:21-slim
  ```
- Created **dim_customer**, **dim_account**, **fact_txn** with primary keys, FKs.
- Added **partitioning** on `fact_txn` by transaction date.
- Indexed join columns (`src_account_id`, `dest_account_id`, `customer_id`).

---

## 🕸️ Step 3: Neo4j Graph Modeling
- Modeled:
  - `(Customer)-[:OWNS]->(Account)`
  - `(Customer)-[:IN_COUNTRY]->(Country)`
  - `(Account)-[:TXN]->(Transaction)`
- Added constraints for unique IDs.
- Used Cypher queries to detect anomalies (e.g., accounts with multiple mismatched countries).

---

## 🔄 Step 4: Orchestration with Apache Airflow
- DAG automates:
  1. Cleaning raw data
  2. Loading into Oracle
  3. Loading into Neo4j
  4. Running validation queries
- Runs via `docker-compose up -d`.

---

## 🚀 Step 5: Query Performance Tuning
Two “hot queries” benchmarked:
1. **plan_mismatch** → currency mismatch in transactions  
2. **plan_daily** → daily transaction rollup  

### Results:

| Query         | Before (Base) | After Partition + Index | After MV + Index |
|---------------|---------------|--------------------------|------------------|
| plan_mismatch | ⏱️ 0.022s     | ⏱️ 0.021s                | ⏱️ 0.021s        |
| plan_daily    | ⏱️ 0.026s     | ⏱️ 0.028s                | ⏱️ 0.033s        |

**Insights:**
- Indexes significantly help join-heavy queries (plan_mismatch).
- Partitioning overhead can increase runtime if all partitions are scanned.
- Materialized Views are beneficial on **large datasets**, but add refresh cost.

**Example Image**

Explain Plan output for Transaction mismatch after partition + indexing

<img width="1110" height="668" alt="image" src="https://github.com/user-attachments/assets/d0bf8912-f287-4b75-a9f2-539d2752e97d" />

---

## 📑 Detailed Steps (A → Z)
1. **Setup Oracle XE with Docker**  
2. **Create schemas and constraints** in Oracle  
3. **Clean CSVs** with Pandas scripts  
4. **Load cleaned data** into Oracle (MERGE + executemany)  
5. **Build Neo4j constraints + load Cypher data**  
6. **Automate with Airflow DAG**  
7. **Run baseline queries & save execution plans**  
8. **Apply tuning (partitioning, indexes, MVs)**  
9. **Re-run queries & compare execution plans**  
10. **Document performance results**  

---

## 🎯 Use Case
- **Fraud/AML Detection**: Detect mismatched currencies, suspicious transaction flows.  
- **Customer 360**: Unified view across customer, accounts, and transactions.  
- **Performance Engineering**: Demonstrates Oracle RAC/Exadata-style tuning workflow (partitioning, indexing, MVs).  

---

## 🛠️ Skills Demonstrated
- Python (Pandas, oracledb, Neo4j driver)  
- SQL, PL/SQL (MERGE, Partitioning, Indexing)  
- Oracle Performance Tuning (EXPLAIN PLAN, DBMS_XPLAN)  
- Neo4j Graph Modeling (constraints, Cypher queries)  
- Apache Airflow DAGs (end-to-end orchestration)  
- Docker (Oracle XE containerization)  
- Data Engineering best practices (clean → stage → warehouse)  

---
## Query Demonstration (ORACLE)
```SQL
#Transactions with mismatched currency between source & destination
SELECT t.txn_id,
       t.currency   AS txn_ccy,
       s.currency   AS src_ccy,
       d.currency   AS dest_ccy,
       t.timestamp
FROM fact_txn t
JOIN dim_account s ON s.account_id = t.src_account_id
JOIN dim_account d ON d.account_id = t.dest_account_id
WHERE (t.currency != s.currency OR t.currency != d.currency);
```
<img width="593" height="196" alt="image" src="https://github.com/user-attachments/assets/a02ddd90-4ff1-4340-9c46-da41180713d1" />

---
```SQL
# Demonstrate total amount of transactional amount done per currency every day
SELECT TRUNC(CAST(timestamp AT LOCAL AS DATE)) AS txn_day,  
       currency,
       SUM(amount) AS total_amt,
       COUNT(*)    AS txn_cnt
FROM fact_txn
GROUP BY TRUNC(CAST(timestamp AT LOCAL AS DATE)), currency
ORDER BY txn_day, txn_cnt DESC
FETCH FIRST 20 ROWS ONLY;
```
<img width="349" height="489" alt="image" src="https://github.com/user-attachments/assets/c25c9170-c3a8-4405-b311-fb257eaee9fa" />

---
## Query Demonstration (Neo4j)
```cypher
// Count number of accounts owned by each customer
MATCH (c:Customer)
OPTIONAL MATCH (c)-[:OWNS]->(a:Account)
RETURN 
  c.customer_id AS customer_id,
  c.name        AS customer_name,
  COUNT(a)      AS accounts_owned
ORDER BY accounts_owned DESC, customer_name limit 5;
```
<img width="477" height="384" alt="image" src="https://github.com/user-attachments/assets/7b6bb344-c64d-42a2-b72f-3b3ff7b88502" />
---

```cypher
// Transactions with mismatched currency between source & destination
MATCH (s:Account)-[:TXN]->(t:Transaction)<-[:TXN]-(d:Account)
WHERE s.currency <> d.currency
RETURN t.txn_id, s.account_id, s.currency, d.account_id, d.currency;
```
<img width="726" height="375" alt="image" src="https://github.com/user-attachments/assets/e84f4591-7e76-45dd-b57d-0fc329f7f16b" />

```cypher
// Top customers by total transaction amount
MATCH (c:Customer)-[:OWNS]->(a:Account)-[:SENT]->(t:Transaction)
RETURN c.customer_id, c.name, SUM(t.amount) AS total_txn
ORDER BY total_txn DESC
LIMIT 10;
```
<img width="582" height="599" alt="image" src="https://github.com/user-attachments/assets/3f91c31e-1b7b-46be-91e1-8dec79a0fd9b" />











