import pandas as pd
import cx_Oracle
from db.db_config import user, pw, dsn


con = cx_Oracle.connect(user, pw, dsn)
print("Data Version ", con.version)
