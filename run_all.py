# run_all.py
import os, subprocess, sys
from dotenv import load_dotenv
import shutil
load_dotenv()

def run(cmd):
    print(f"\n$ {' '.join(cmd)}")
    res = subprocess.run(cmd, shell=False)
    if res.returncode != 0:
        sys.exit(res.returncode)

def main():

    # 1) CLEAN (uses your existing scripts)
    run([sys.executable, "script/cleanCustomer.py"])
    run([sys.executable, "script/cleanAccount.py"])
    run([sys.executable, "script/cleantxn.py"])

    # 2) LOAD to Neo4j (your existing loader with flags)
    run([sys.executable, "actions/create/create4j.py"])

    # 3) LOAD to Oracle (your new pandas-based loader, no args)
    run([sys.executable, "oracle/python/load_to_oracle.py"])

    print("\nAll done: cleaned and loaded into Neo4j + Oracle.")

    clean_cust_source_path = "./clean/customer"
    clean_account_source_path = "./clean/account"
    clean_txn_source_path = "./clean/txn"

    backup_clean_cust_source_path = "./clean/backup/customer"
    backup_clean_account_source_path = "./clean/backup/account"
    backup_clean_txn_source_path = "./clean/backup/txn"

    clean_cust_file_name= os.listdir(clean_cust_source_path)
    clean_account_file_name= os.listdir(clean_account_source_path)
    clean_txn_file_name= os.listdir(clean_txn_source_path)

    clean_cust_file = os.path.join(clean_cust_source_path,clean_cust_file_name[0])
    clean_account_file = os.path.join(clean_account_source_path,clean_account_file_name[0])
    clean_txn_file = os.path.join(clean_txn_source_path,clean_txn_file_name[0])    

    backup_clean_cust_file = os.path.join(backup_clean_cust_source_path,clean_cust_file_name[0])
    backup_clean_account_file = os.path.join(backup_clean_account_source_path,clean_account_file_name[0])
    backup_clean_txn_file = os.path.join(backup_clean_txn_source_path,clean_txn_file_name[0])

    shutil.move(clean_cust_file, backup_clean_cust_file)
    shutil.move(clean_account_file, backup_clean_account_file)
    shutil.move(clean_txn_file, backup_clean_txn_file)
    print("Moved Clean files to backup")


if __name__ == "__main__":
    main()
