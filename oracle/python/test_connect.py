import os
from dotenv import load_dotenv
import oracledb

load_dotenv()
user = os.getenv("ORACLE_APP_USER")
pwd  = os.getenv("ORACLE_APP_PWD")
dsn  = os.getenv("ORACLE_DSN", "localhost/XEPDB1")


with oracledb.connect(user=user, password=pwd, dsn=dsn) as con:
    cur = con.cursor()
    cur.execute("SELECT USER, SYS_CONTEXT('USERENV','CON_NAME') FROM dual")
    print(cur.fetchone())
