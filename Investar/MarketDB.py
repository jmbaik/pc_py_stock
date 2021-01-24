import pandas as pd
#from bs4 import BeautifulSoup
#import urllib
#from urllib.request import urlopen
import db.db_config as config
import cx_Oracle as cx
#import time
#import pandas.io.sql as sql
from datetime import datetime
#from threading import Timer
#import matplotlib.pyplot as plt

class MarketDB:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = cx.connect(config.user, config.pw, config.dsn)
        self.codes = dict()
        self.getCompanyInfo()
        
    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def getCompanyInfo(self):
        """company_info 테이블에서 읽어와서 companyData와 codes에 저장"""
        sql = "SELECT * FROM MQ_ITEMS"
        companyInfo = pd.read_sql(sql, self.conn)
        for idx in range(len(companyInfo)):
            self.codes[companyInfo['ITEM_CD'].values[idx]] = companyInfo['ITEM_NM'].values[idx]

    def getDailyPrice(self, code, startDate, endDate):
        """daily_price 테이블에서 읽어와서 데이터프레임으로 반환"""
        sql = "SELECT * FROM MQ_STOCK_HIST WHERE ITEM_CD = '{}' and YMD >= '{}' and YMD <= '{}'".format(code, startDate, endDate)
        df = pd.read_sql(sql, self.conn)
        df.index = df['YMD']
        return df



