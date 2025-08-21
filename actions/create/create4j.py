import pandas as pd
from neo4j import GraphDatabase as gd
import os

uri = "bolt://localhost:7687"
username = "neo4j"
password = "D!i5n9e9sh"
DB = "financialdatabase"

driver = gd.driver(uri, auth=(username, password))

try:
    with driver.session(database=DB) as session:
        result = session.run("RETURN 1 AS test")
        print("Connection successful, test result:", result.single()["test"])
except Exception as e:
    print("Connection failed:", e)
    raise

constraint_customer = """
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE
"""
constraint_account = """
CREATE CONSTRAINT account_id_unique IF NOT EXISTS
FOR (a:Account) REQUIRE a.account_id IS UNIQUE
"""
constraint_transaction = """
CREATE CONSTRAINT txn_id_unique IF NOT EXISTS
FOR (t: Transaction) REQUIRE t.txn_id IS UNIQUE
"""
constraint_branch = """
CREATE CONSTRAINT branch_id_unique IF NOT EXISTS
FOR (b: Branch) REQUIRE b.branch_id IS UNIQUE
"""
constraint_contryName = """
CREATE CONSTRAINT country_name_unique IF NOT EXISTS
FOR (c: Country) REQUIRE c.country_name IS UNIQUE
"""
constraint_channel = """
CREATE CONSTRAINT unique_channel_type IF NOT EXISTS
FOR (c: Channel) REQUIRE c.channel_type IS UNIQUE
"""
constraint_currency = """
CREATE CONSTRAINT unique_currency IF NOT EXISTS
FOR (c:Currency) REQUIRE c.currency_type IS UNIQUE
"""

# ---- Upsert (insert or update) customers
queryCustomer_Merge = """
UNWIND $rows AS row
MERGE (c:Customer {customer_id: row.id})
SET  c.name    = row.name,
     c.dob     = row.dob,
     c.kyc     = row.kyc_status,
     c.email   = row.email,
     c.phone   = row.phone,
     c.address = row.address
"""

# ---- Upsert accounts and link to owner
queryAccount_Merge = """
UNWIND $rows AS row
MERGE (a:Account {account_id: row.aid})
SET  a.type     = row.type,
     a.currency = row.currency,
     a.status   = row.status
WITH a, row
MATCH (c:Customer {customer_id: row.cid})
MERGE (c)-[:OWNS {created_at:row.createdAt}]->(a)
"""

# ---- Upsert transactions
queryTxn_Merge = """
UNWIND $rows AS row
MATCH (dest:Account   {account_id: row.d_id})
MATCH (source:Account {account_id: row.s_id})
MERGE (t:Transaction {transaction_id: row.tid})
SET  t.amount   = toFloat(row.amount),
     t.currency = row.currency,
     t.timestamp = datetime(row.ts),
     t.channel  = row.channel,
     t.status   = row.status
MERGE (t)-[:TO]->(dest)
MERGE (source)-[:SENT]->(t)
"""

# ---- Upsert Label Account to Account
queryAccountTransfers = """
UNWIND $rows AS row
MATCH (s:Account {account_id: row.s_id}),
      (d:Account {account_id: row.d_id})
MERGE (s)-[tr:TRANSFERS_TO {txn_id: row.tid}]->(d)
SET  tr.amount   = toFloat(row.amount),
     tr.currency = row.currency,
     tr.status   = row.status
"""

# ---- Upsert Branch Node and link account to branch
queryBranch_Merge = """
UNWIND $rows AS row
MERGE (b: Branch {branch_id: row.branch})
WITH b, row
MATCH(a: Account{account_id: row.aid})
MERGE (a)-[:OPENED_AT]->(b)
"""

# ---- Upsert Country Node and link Customer to Country
queryCountry_Merge = """
UNWIND $rows AS row
MERGE (c:Country {country_name: row.country})
WITH c, row
MATCH(u: Customer{customer_id:row.id})
MERGE(u)-[:IN_COUNTRY]->(c)
"""

# ---- Upsert Channel type and link txn with Channel type
queryChannelMerge = """
UNWIND $rows as row
MERGE (c:Channel {channnel_type: row.channel})
WITH c, row
MATCH (t: Transaction{transaction_id:row.tid})
MERGE (t)-[:VIA_CHANNEL]->(c)
"""

queryCurrencyMerge = """
UNWIND $rows as row
MERGE (c:Currency {currency_type: row.currency})
WITH c, row
MATCH (a: Account {account_id:row.aid})
MERGE (a)-[:USES_CURRENCY]->(c)
"""

queryDenominationMerge  = """
UNWIND $rows as row
MATCH (t: Transaction {transaction_id : row.tid}),(c:Currency {currency_type: row.currency})
MERGE (t) -[:DENOMINATED_IN]->(c)
"""

# ---- Read CSVs  TODO: Automate this

customer_source_path = "./clean/customer"
customer_filename = os.listdir(customer_source_path)[0]
customer_file  = os.path.join(customer_source_path,customer_filename)

account_source_path = "./clean/account"
account_filename = os.listdir(account_source_path)[0]
account_file  = os.path.join(account_source_path,account_filename)

txn_source_path = "./clean/txn"
txn_filename = os.listdir(txn_source_path)[0]
txn_file  = os.path.join(txn_source_path,txn_filename)

print(f"Customer:- {customer_file}, Account:-{account_file}, Transactions:- {txn_file}")

custData = pd.read_csv(customer_file)
accountData = pd.read_csv(account_file)
tnxData = pd.read_csv(txn_file)

cust_rows = [
    {
        "id": r["customer_id"],
        "name": r["name"],
        "dob": r["dob"],
        "kyc_status": r["kyc_status"],
        "email": r["email"],
        "phone": r["phone"],
        "address": r["address"],
        "country": r["country"],
    }
    for r in custData.to_dict(orient="records")
]

acc_rows = [
    {
        "aid": r["account_id"],
        "cid": r["customer_id"],      
        "type": r["type"],
        "currency": r["currency"],
        "createdAt":r["opened_at"],
        "status": r["status"],
        "branch": r["branch_id"]   
    }
    for r in accountData.to_dict(orient="records")
]

txn_rows = [
    {
        "tid":r["txn_id"],
        "s_id":r["src_account_id"],
        "d_id":r["dst_account_id"],
        "amount":r["amount"],
        "currency":r["currency"],
        "ts":r["ts"],
        "channel":r["channel"],
        "status":r["status"]
    }
    for r in tnxData.to_dict(orient="records")
]

try:
    with driver.session(database=DB) as session:
        session.run(constraint_customer)
        session.run(constraint_account)
        session.run(constraint_transaction)
        session.run(constraint_branch)
        session.run(constraint_contryName)
        session.run(constraint_channel)
        session.run(constraint_currency)

        session.run(queryCustomer_Merge, {"rows": cust_rows})
        print("Customer upsert complete")        
        
        session.run(queryCountry_Merge, {"rows": cust_rows})
        print("Country upsert complete")

        session.run(queryAccount_Merge, {"rows": acc_rows})
        print("Account upsert + relationships complete")

        session.run(queryTxn_Merge, {"rows": txn_rows})
        print("Transaction upsert + relationships complete")

        session.run(queryAccountTransfers, {"rows": txn_rows})
        print("queryAccountTransfers upsert relationships complete")
        
        session.run(queryChannelMerge, {"rows": txn_rows})
        print("queryChannelMerge upsert relationships complete")        
        
        session.run(queryBranch_Merge, {"rows": acc_rows})
        print("queryBranch_Merge upsert relationships complete")

        session.run(queryCurrencyMerge, {"rows": acc_rows})
        print("queryBranch_Merge upsert relationships complete")

        session.run(queryDenominationMerge, {"rows": txn_rows})
        print("queryBranch_Merge upsert relationships complete")

except Exception as e:
    print("General Error:", e)
finally:
    driver.close()
