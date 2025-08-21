import pandas as pd
import numpy as np
import datetime
import os
import shutil


def parse_amount(val):
    if pd.isna(val): return np.nan
    v = str(val).strip().upper().replace("$","").replace("USD","").replace(",","")
    try:
        return float(v)
    except:
        return np.nan
    

def clean_currency(val):
    if pd.isna(val): return "USD"
    v = str(val).strip().upper().replace("$","")
    v = v.replace(" ", "")
    return v 
    
def clean_time(val):
    try:
        if pd.isna(val) or val ==  "":
            return "1970-01-01T00:00:00" # Default Epoch Date
        else:
            return pd.to_datetime(val, errors="coerce").strftime("%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return "1970-01-01T00:00:00" # Default Epoch Date


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["txn_id"] = df["txn_id"].astype(str).str.strip().str.upper()
    df["src_account_id"] = df["src_account_id"].astype(str).str.strip().str.upper()
    df["dst_account_id"] = df["dst_account_id"].astype(str).str.strip().str.upper()
    df["amount"] = df["amount"].apply(parse_amount)
    df["currency"] = df["currency"].apply(lambda x: clean_currency(x) if str(x).strip()!="" else "USD")
    df["ts"] = df["ts"].apply(clean_time)
    df["channel"] = df["channel"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].astype(str).str.strip().str.upper()

    df = df.drop_duplicates()

    return df

def main():
    source_path  = './RAW/txn/'
    backup_path = "./RAW/backup/txn"

    if not os.path.exists(backup_path):
        os.makedirs(backup_path)
    
    for filename in os.listdir(source_path):
        transactions_raw = pd.read_csv(source_path+filename)
        transactions = clean_transactions(transactions_raw)
        today = datetime.datetime.now().strftime("%Y%m%d") 
        transactions.to_csv(f"../clean/txn/transaction{today}.csv",index=False)
        print("CLEAN files written:")
        print(f"  - txn.csv:    {len(transactions)} rows")

        name, ext = os.path.splitext(filename)
        new_filename = f"{name}_{today}{ext}"
        source_file = os.path.join(source_path,filename)
        destination_file = os.path.join(backup_path,new_filename)
        shutil.move(source_file, destination_file)
        print(f"{filename} has been processed. Moved to {backup_path}")

if __name__ == "__main__":
    main()
