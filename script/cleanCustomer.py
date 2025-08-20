import pandas as pd
import numpy as np
import datetime
import shutil
import os

def cleanStr(x):
    if pd.isna(x): return None
    return " ".join(str(x).strip().split())

def phoneFix(p):
    if not p or (isinstance(p, float) and np.isnan(p)):return None
    digits = "".join(filter(str.isdigit, str(p)))
    return f"+{digits}" if len(digits) == 11 and digits.startswith("1") else \
        f"+1{digits}" if len(digits) == 10 else None

def normalize_country(x):
    if pd.isna(x): return None
    v = str(x).strip().lower().replace('.','')
    if v in {"usa", "us", "united states", "u s a"}:
        return "USA"
    return str(x).strip()



def clean_customers(df: pd.DataFrame)-> pd.DataFrame:
    df = df.copy()
    df['customer_id'] = df['customer_id'].str.upper().str.strip()
    df["name"] = df["name"].apply(lambda s: cleanStr(s).title() if s is not None else None)
    df["dob"] = pd.to_datetime(df["dob"], errors="coerce")
    df["kyc_status"] = df["kyc_status"].astype(str).str.strip().str.upper()
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["phone"] = df["phone"].apply(phoneFix)
    df["address"] = df["address"].apply(cleanStr)
    df["country"] = df["country"].apply(normalize_country)

    df = (
    df.sort_values(["customer_id"])
      .groupby("customer_id", as_index=False)
      .agg({
          "name": "first",
          "dob": "first",
          "kyc_status": "first",
          "email": "first",
          "phone": "first",
          "address": "first",
          "country": "first"
      })
    )
    
    return df

def main():
    source_path  = '../RAW/customer/'
    backup_path = "../RAW/backup/customer"

    if not os.path.exists(backup_path):
        os.makedirs(backup_path)

    for filename in os.listdir(source_path):
        df = pd.read_csv(source_path+filename)
        customers = clean_customers(df)
        today = datetime.datetime.now().strftime("%Y%m%d") 
        customers.to_csv(f"../clean/customer/customer_{today}.csv",index=False) 
        print("CLEAN files written:")
        print(f"  - customers.csv:    {len(customers)} rows")
        
        name, ext = os.path.splitext(filename)
        new_filename = f"{name}_{today}{ext}"
        source_file = os.path.join(source_path,filename)
        destination_file = os.path.join(backup_path,new_filename)
        shutil.move(source_file, destination_file)
        print(f"{filename} has been processed. Moved to {backup_path}")



if __name__ == "__main__":
    main()