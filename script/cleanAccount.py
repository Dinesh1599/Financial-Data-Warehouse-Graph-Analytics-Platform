import pandas as pd
import datetime
import os
import shutil


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

def clean_accounts(df:pd.DataFrame) ->pd.DataFrame:
    df = df.copy()
    df["account_id"] = df["account_id"].astype(str).str.strip().str.upper()
    df["customer_id"] = df["customer_id"].astype(str).str.strip().str.upper()
    df["type"] = df["type"].astype(str).str.strip().str.title()
    df["currency"] = df["currency"].apply(clean_currency)
    df["opened_at"] = df["opened_at"].apply(clean_time)
    df["status"] = df["status"].astype(str).str.strip().str.upper().replace({"NONE":None})
    df["branch_id"] = df["branch_id"].astype(str).str.strip().str.upper()

    df.drop_duplicates()
    return df

def main():
    source_path  = './RAW/accounts/'
    backup_path = "./RAW/backup/accounts"

    if not os.path.exists(backup_path):
        os.makedirs(backup_path)


    for filename in os.listdir(source_path):
        account_raw = pd.read_csv(source_path+filename)
        accounts = clean_accounts(account_raw)
        today = datetime.datetime.now().strftime("%Y%m%d") 
        accounts.to_csv(f"./clean/account/accounts{today}.csv",index=False)
        print("CLEAN files written:")
        print(f"  - accounts.csv:    {len(accounts)} rows")

        name, ext = os.path.splitext(filename)
        new_filename = f"{name}_{today}{ext}"
        source_file = os.path.join(source_path,filename)
        destination_file = os.path.join(backup_path,new_filename)
        shutil.move(source_file, destination_file)
        print(f"{filename} has been processed. Moved to {backup_path}")

if __name__ == "__main__":
    main()