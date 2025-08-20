#!/usr/bin/env python3
"""
clean_financial_data.py
-----------------------
Transform RAW CSVs (customers/accounts/transactions) into CLEAN, standardized CSVs
suitable for loading into Neo4j (or any downstream system).

Usage:
  python clean_financial_data.py \
      --customers-raw customers_raw.csv \
      --accounts-raw accounts_raw.csv \
      --transactions-raw transactions_raw.csv \
      --out-dir ./

Outputs in --out-dir:
  customers.csv, accounts.csv, transactions.csv

Notes:
- Enforces foreign keys: accounts must have valid customers; transactions must reference valid accounts
- Drops exact duplicates
- Standardizes IDs, dates, currency, amounts, status, phone, and trims/normalizes text
"""

import argparse
import pandas as pd
import numpy as np

def normalize_country(val):
    if pd.isna(val): return None
    v = str(val).strip().lower().replace('.', '')
    if v in {"usa", "us", "united states", "u s a"}:
        return "USA"
    return str(val).strip()

def clean_currency(val):
    if pd.isna(val): return None
    v = str(val).strip().upper().replace("$","")
    v = v.replace(" ", "")
    if v in {"USD","EUR"}:
        return v
    # default to USD if ambiguous/invalid
    return "USD"

def parse_amount(val):
    if pd.isna(val): return np.nan
    v = str(val).strip().upper().replace("$","").replace("USD","").replace(",","")
    try:
        return float(v)
    except:
        return np.nan

def standardize_phone(p):
    if p is None or (isinstance(p,float) and np.isnan(p)): return None
    digits = "".join([c for c in str(p) if c.isdigit()])
    if len(digits)==10:
        return f"+1{digits}"
    if len(digits)==11 and digits.startswith("1"):
        return f"+{digits}"
    return None

def tidy_str(x):
    if pd.isna(x): return None
    return " ".join(str(x).strip().split())

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["customer_id"] = df["customer_id"].str.upper().str.strip()
    df["name"] = df["name"].apply(lambda s: tidy_str(s).title() if s is not None else None)
    df["dob"] = pd.to_datetime(df["dob"], errors="coerce")
    df["kyc_status"] = df["kyc_status"].astype(str).str.strip().str.upper()
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["phone"] = df["phone"].apply(standardize_phone)
    df["address"] = df["address"].apply(tidy_str)
    df["country"] = df["country"].apply(normalize_country)

    # Deduplicate by customer_id (keep first non-null values)
    df = df.sort_values(["customer_id"]).groupby("customer_id", as_index=False).agg({
        "name":"first","dob":"first","kyc_status":"first","email":"first",
        "phone":"first","address":"first","country":"first"
    })
    return df

def clean_accounts(df: pd.DataFrame, valid_customers: set) -> pd.DataFrame:
    df = df.copy()
    df["account_id"] = df["account_id"].astype(str).str.strip().str.upper()
    df["customer_id"] = df["customer_id"].astype(str).str.strip().str.upper()
    df["type"] = df["type"].astype(str).str.strip().str.title()
    df["currency"] = df["currency"].apply(clean_currency)
    df["opened_at"] = pd.to_datetime(df["opened_at"], errors="coerce", utc=True)
    df["status"] = df["status"].astype(str).str.strip().str.upper().replace({"NONE":None})
    df["branch_id"] = df["branch_id"].astype(str).str.strip().str.upper()

    # Drop exact duplicates
    df = df.drop_duplicates()

    # Enforce foreign key to Customer
    df = df[df["customer_id"].isin(valid_customers)].copy()
    return df

def clean_transactions(df: pd.DataFrame, valid_accounts: set) -> pd.DataFrame:
    df = df.copy()
    df["txn_id"] = df["txn_id"].astype(str).str.strip().str.upper()
    df["src_account_id"] = df["src_account_id"].astype(str).str.strip().str.upper()
    df["dst_account_id"] = df["dst_account_id"].astype(str).str.strip().str.upper()
    df["amount"] = df["amount"].apply(parse_amount)
    df["currency"] = df["currency"].apply(lambda x: clean_currency(x) if str(x).strip()!="" else "USD")
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df["channel"] = df["channel"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].astype(str).str.strip().str.upper()

    # Drop exact duplicates
    df = df.drop_duplicates()

    # Enforce foreign keys to Account
    df = df[df["src_account_id"].isin(valid_accounts) & df["dst_account_id"].isin(valid_accounts)]

    # Drop rows with nulls in essential fields
    df = df.dropna(subset=["txn_id","src_account_id","dst_account_id","amount","currency","ts"])
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers-raw", required=True, help="Path to RAW customers CSV")
    ap.add_argument("--accounts-raw", required=True, help="Path to RAW accounts CSV")
    ap.add_argument("--transactions-raw", required=True, help="Path to RAW transactions CSV")
    ap.add_argument("--out-dir", required=True, help="Output directory for CLEAN CSVs")
    args = ap.parse_args()

    # Load RAW CSVs
    customers_raw = pd.read_csv(args.customers_raw)
    accounts_raw = pd.read_csv(args.accounts_raw)
    transactions_raw = pd.read_csv(args.transactions_raw)

    # Clean
    customers = clean_customers(customers_raw)
    accounts  = clean_accounts(accounts_raw, set(customers["customer_id"]))
    transactions = clean_transactions(transactions_raw, set(accounts["account_id"]))

    # Save CLEAN CSVs
    customers.to_csv(f"{args.out_dir.rstrip('/')}/customers.csv", index=False, date_format="%Y-%m-%d")
    accounts.to_csv(f"{args.out_dir.rstrip('/')}/accounts.csv", index=False, date_format="%Y-%m-%dT%H:%M:%SZ")
    transactions.to_csv(f"{args.out_dir.rstrip('/')}/transactions.csv", index=False, date_format="%Y-%m-%dT%H:%M:%SZ")

    # Simple stats printout
    print("CLEAN files written:")
    print(f"  - customers.csv:    {len(customers)} rows")
    print(f"  - accounts.csv:     {len(accounts)} rows")
    print(f"  - transactions.csv: {len(transactions)} rows")

if __name__ == "__main__":
    main()
