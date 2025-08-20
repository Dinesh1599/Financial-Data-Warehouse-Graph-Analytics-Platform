CREATE TABLE dim_customer (
  customer_id   VARCHAR2(20) PRIMARY KEY,
  name          VARCHAR2(200),
  dob           DATE,
  kyc           VARCHAR2(20),
  email         VARCHAR2(200),
  phone         NUMBER(20),
  address       VARCHAR2(400),
  country       VARCHAR2(60)
);

select * from dim_customer;

CREATE TABLE dim_account (
  account_id    VARCHAR2(20) PRIMARY KEY,
  customer_id   VARCHAR2(20) NOT NULL,
  type          VARCHAR2(30),
  balance       NUMBER(10,2),
  currency      VARCHAR2(4),
  status        VARCHAR2(20),
  opened_at     DATE,
  branch_id     VARCHAR2(20),
  CONSTRAINT fk_acc_cust
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);

select * from dim_account;

CREATE TABLE fact_txn (
  txn_id            VARCHAR2(30) PRIMARY KEY,
  src_account_id    VARCHAR2(20) NOT NULL,
  dest_account_id   VARCHAR2(20) NOT NULL,
  amount            NUMBER(10,2),
  currency          VARCHAR2(10),
  channel           VARCHAR2(10),
  timestamp         TIMESTAMP,
  status            VARCHAR2(120)
);

