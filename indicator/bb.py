from db.db_config import *
import cx_Oracle as cx
import pandas as pd

conn = cx.connect(user, pw, dsn)
sql = "select * from mq_stock_hist where item_cd='005930' order by ymd asc"
df = pd.read_sql(sql, conn)
conn.close()

df["YMD"] = pd.to_datetime(df["YMD"])

price_df = df.loc[:, ['YMD', 'CLOSE']].copy()
price_df = price_df.rename({'YMD': 'ymd', 'CLOSE': 'close'}, axis='columns')
price_df.set_index(['ymd'], inplace=True)  # 인덱스 설정

# 볼린저 밴드 중간 밴드 만들기
price_df['center'] = price_df['close'].rolling(window=20).mean()
price_df['ub'] = price_df['center'] + 2 * price_df['close'].rolling(window=20).std()
price_df['lb'] = price_df['center'] - 2 * price_df['close'].rolling(window=20).std()

n = 20
sigma = 2

def bollinger_band(price_df, n, sigma):
    bb = price_df.copy()
    bb['center'] = price_df['close'].rolling(window=n).mean()
    bb['ub'] = bb['center'] + sigma * price_df['close'].rolling(window=n).std()
    bb['lb'] = bb['center'] - sigma * price_df['close'].rolling(window=n).std()
    return bb






