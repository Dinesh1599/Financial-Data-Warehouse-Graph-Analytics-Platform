import os
import pandas as pd
import oracledb
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv("ORACLE_APP_USER", "APPUSER")
PWD  = os.getenv("ORACLE_APP_PWD",  "apppwd")
DSN  = os.getenv("ORACLE_DSN",      "localhost/XEPDB1")

# 👇 Define your CSV paths here (relative to project root)
customer_source_path = "./clean/customer"
customer_filename = os.listdir(customer_source_path)[0]
CUSTOMER_CSV  = os.path.join(customer_source_path,customer_filename)

account_source_path = "./clean/account"
account_filename = os.listdir(account_source_path)[0]
ACCOUNT_CSV  = os.path.join(account_source_path,account_filename)

txn_source_path = "./clean/txn"
txn_filename = os.listdir(txn_source_path)[0]   
TXN_CSV  = os.path.join(txn_source_path,txn_filename)

def load_customers_debug(cur, path):
    df = pd.read_csv(path)
    df = df.where(pd.notnull(df), None)  # replace NaN with None
    rows = df.to_dict(orient="records")
    for i, row in enumerate(rows, start=1):
        try:
            cur.execute("""
                MERGE INTO dim_customer d
                USING (SELECT :customer_id AS customer_id, :name AS name,
                              TO_DATE(:dob,'YYYY-MM-DD') AS dob,
                              :kyc AS kyc, :email AS email, :phone AS phone,
                              :address AS address, :country AS country FROM dual) s
                ON (d.customer_id = s.customer_id)
                WHEN MATCHED THEN UPDATE SET
                     name=s.name, dob=s.dob, kyc=s.kyc, email=s.email,
                    phone=s.phone, address=s.address, country=s.country
                WHEN NOT MATCHED THEN INSERT (customer_id,name,dob,kyc,email,phone,address,country)
                    VALUES (s.customer_id,s.name,s.dob,s.kyc,s.email,s.phone,s.address,s.country)
            """, row)
        except Exception as e:
            print(f"❌ Error on row {i}: {row}")
            print(e)
            break

def load_customers(cur, path):
    df = pd.read_csv(path)
    df["phone"] = pd.to_numeric(df["phone"]).astype("Int64")
    df = df.where(pd.notnull(df), None)  # replace NaN with None
    rows = df.to_dict(orient="records")
    cur.executemany("""
        MERGE INTO dim_customer d
        USING (SELECT :customer_id AS customer_id, :name AS name,
                      TO_DATE(:dob,'YYYY-MM-DD') AS dob,
                      :kyc_status AS kyc, :email AS email, :phone AS phone,
                      :address AS address, :country AS country FROM dual) s
        ON (d.customer_id = s.customer_id)
        WHEN MATCHED THEN UPDATE SET
             name=s.name, dob=s.dob, kyc=s.kyc, email=s.email,
             phone=s.phone, address=s.address, country=s.country
        WHEN NOT MATCHED THEN INSERT (customer_id,name,dob,kyc,email,phone,address,country)
             VALUES (s.customer_id,s.name,s.dob,s.kyc,s.email,s.phone,s.address,s.country)
    """, rows)
   
def load_accounts(cur, path):
    df = pd.read_csv(path)
    df = df.where(pd.notnull(df), None)
    rows = df.to_dict(orient="records")
    cur.executemany("""
        MERGE INTO dim_account d
        USING (SELECT :account_id AS account_id, :customer_id AS customer_id,
                      :type AS type, :status AS status, :currency AS currency, :balance as balance,
                      TO_DATE(:opened_at,'YYYY-MM-DD"T"HH24:MI:SS') AS opened_at,
                      :branch_id AS branch_id FROM dual) s
        ON (d.account_id = s.account_id)
        WHEN MATCHED THEN UPDATE SET
             customer_id=s.customer_id, type=s.type, status=s.status,
             opened_at=s.opened_at, branch_id=s.branch_id, currency=s.currency, balance=s.balance
        WHEN NOT MATCHED THEN INSERT (account_id,customer_id,type,status,opened_at,branch_id,currency,balance)
             VALUES (s.account_id,s.customer_id,s.type,s.status,s.opened_at,s.branch_id,s.currency,s.balance)
    """, rows)

def load_txns(cur, path):
    df = pd.read_csv(path)
    df = df.where(pd.notnull(df), None)
    rows = df.to_dict(orient="records")
    cur.executemany("""
        MERGE INTO fact_txn f
        USING (SELECT :txn_id AS txn_id, :src_account_id AS src_account_id, :dst_account_id as dest_account_id,
                      :amount AS amount, :currency AS currency, :channel as channel,
                      TO_TIMESTAMP(:ts,'YYYY-MM-DD"T"HH24:MI:SS') AS timestamp,
                      :status AS status FROM dual) s
        ON (f.txn_id = s.txn_id)
        WHEN MATCHED THEN UPDATE SET
             src_account_id=s.src_account_id, dest_account_id=s.dest_account_id, amount=s.amount, currency=s.currency, channel=s.channel,
             timestamp=s.timestamp, status=s.status
        WHEN NOT MATCHED THEN INSERT (txn_id,src_account_id, dest_account_id,amount,currency, channel,timestamp,status)
             VALUES (s.txn_id,s.src_account_id, s.dest_account_id,s.amount,s.currency, s.channel,s.timestamp,s.status)
    """, rows)

def main():
    with oracledb.connect(user=USER, password=PWD, dsn=DSN) as con:
        cur = con.cursor()
        #load_customers(cur, CUSTOMER_CSV)
        #load_accounts(cur, ACCOUNT_CSV)
        load_txns(cur, TXN_CSV)
        con.commit()
        print("Data loaded successfully into Oracle!")

if __name__ == "__main__":
    main()
