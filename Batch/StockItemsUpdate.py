from datetime import datetime
import pandas as pd
import FinanceDataReader as fdr
from bs4 import BeautifulSoup
import db.db_config as config
import cx_Oracle as cx
import urllib, calendar, time, json
from urllib.request import urlopen
from datetime import datetime
from threading import Timer


class MqItemsUpdate:
    def __init__(self):
        self.conn = cx.connect(config.user, config.pw, config.dsn)
        self.codes = dict()
        self.ymd = datetime.now().strftime("%Y%m%d")

    def __del__(self):
        self.conn.close()

    def read_krx_code(self):
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=' \
              'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        # '회사명', '종목코드', '업종', '주요제품', '상장일', '결산월', '대표자명', '홈페이지', '지역'
        krx = krx.rename(columns={'회사명': 'item_nm', '종목코드': 'item_cd', '업종': 'upjong', '주요제품': 'products'
                ,'상장일': 'sangjangday', '결산월': 'ksmonth', '대표자명': 'ceonm', '홈페이지': 'homepage', '지역': 'region'})
        krx.item_cd = krx.item_cd.map('{:06d}'.format)
        return krx

    def merge_krx_items(self):
        krx = self.read_krx_code()
        with self.conn.cursor() as cursor:
            for idx in range(len(krx)):
                item_cd = krx.item_cd.values[idx]
                item_nm = krx.item_nm.values[idx]
                upjong = str(krx.upjong.values[idx]).replace("'", "''")
                products = str(krx.products.values[idx]).replace("'", "''")
                sangjangday = str(krx.sangjangday.values[idx]).replace("'", "''")
                ksmonth = str(krx.ksmonth.values[idx]).replace("'", "''")
                ceonm = str(krx.ceonm.values[idx]).replace("'", "''")
                homepage = str(krx.homepage.values[idx]).replace("'", "''")
                region = str(krx.region.values[idx]).replace("'", "''")
                merge_sql = f"""
                        MERGE INTO METSTOCK.MQ_ITEMS d
                        USING (
                          Select
                            '{item_cd}' as ITEM_CD,
                            '{self.ymd}' as YMD,
                            'Y' as STATUS,
                            '{item_nm}' as ITEM_NM,
                            '{upjong}' as UPJONG,
                            '{products}' as PRODUCTS,
                            '{sangjangday}' as SANGJANGDAY,
                            '{ksmonth}' as KSMONTH,
                            '{ceonm}' as CEONM,
                            '{homepage}' as HOMEPAGE,
                            '{region}' as REGION
                          From Dual) s
                        ON
                          (d.ITEM_CD = s.ITEM_CD )
                        WHEN MATCHED
                        THEN
                        UPDATE SET
                          d.YMD = s.YMD,
                          d.STATUS = s.STATUS,
                          d.ITEM_NM = s.ITEM_NM,
                          d.UPJONG = s.UPJONG,
                          d.PRODUCTS = s.PRODUCTS,
                          d.SANGJANGDAY = s.SANGJANGDAY,
                          d.KSMONTH = s.KSMONTH,
                          d.CEONM = s.CEONM,
                          d.HOMEPAGE = s.HOMEPAGE,
                          d.REGION = s.REGION,
                          d.UPD_DT = sysdate
                        WHEN NOT MATCHED
                        THEN
                        INSERT (
                          ITEM_CD, YMD, STATUS,
                          ITEM_NM, UPJONG, PRODUCTS,
                          SANGJANGDAY, KSMONTH, CEONM,
                          HOMEPAGE, REGION, REG_DT)
                        VALUES (
                          s.ITEM_CD, s.YMD, s.STATUS,
                          s.ITEM_NM, s.UPJONG, s.PRODUCTS,
                          s.SANGJANGDAY, s.KSMONTH, s.CEONM,
                          s.HOMEPAGE, s.REGION, sysdate)        
                        """
                cursor.execute(merge_sql)
                self.codes[item_cd] = item_nm
                self.conn.commit()

    def read_naver(self, code, company, pages_to_fetch):
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            with urlopen(url) as doc:
                if doc is None:
                    return None
                html = BeautifulSoup(doc, "lxml")
                pgrr = html.find("td", class_="pgRR")
                if pgrr is None:
                    return None
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                df = df.append(pd.read_html(pg_url, header=0)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.
                      format(tmnow, company, code, page, pages), end="\r")
            df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff'
                , '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})
            df['date'] = df['date'].replace('.', '-')
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close',
                                                                         'diff', 'open', 'high', 'low',
                                                                         'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
        except Exception as e:
            print('Exception occured :', str(e))
            return None
        return df

    def read_fdr(self, code, year):
        df_krx = pd.DataFrame()
        try:
            df_krx = fdr.DataReader(code, year)
        except Exception as e:
            print('Exception occured :', str(e))
            return None
        return df_krx

    def merge_krx_items_hist(self, df, num, code, company):
        with self.conn.cursor() as cursor:
            for r in df.itertuples():
                sql = f'''
                    MERGE INTO METSTOCK.MQ_STOCK_HIST d
                    USING (
                      Select
                        '{code}' as ITEM_CD,
                        '{r.Index.strftime('%Y%m%d')}' as YMD,
                        '{r.Open}' as OPEN,
                        '{r.High}' as HIGH,
                        '{r.Low}' as LOW,
                        '{r.Close}' as CLOSE,
                        '{r.Volume}' as VOL
                      From Dual) s
                    ON
                      (d.ITEM_CD = s.ITEM_CD and 
                      d.YMD = s.YMD )
                    WHEN MATCHED
                    THEN
                    UPDATE SET
                      d.OPEN = s.OPEN,
                      d.HIGH = s.HIGH,
                      d.LOW = s.LOW,
                      d.CLOSE = s.CLOSE,
                      d.VOL = s.VOL,
                      d.UPD_DT = sysdate
                    WHEN NOT MATCHED
                    THEN
                    INSERT (
                      ITEM_CD, YMD, OPEN,
                      HIGH, LOW, CLOSE,
                      VOL, REG_DT, UPD_DT)
                    VALUES (
                      s.ITEM_CD, s.YMD, s.OPEN,
                      s.HIGH, s.LOW, s.CLOSE,
                      s.VOL, sysdate, sysdate)
                '''
                cursor.execute(sql)
                self.conn.commit()

    def merge_daily_price(self, pages_to_fetch):
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.merge_krx_items_hist(df, idx, code, self.codes[code])
            print(str(idx) + " : " + str(code) + "["+self.codes[code] + "] " + datetime.now().strftime('%Y-%m-%d %H:%M'))

    def merge_daily_price_ffdr(self, d_ymd):
        for idx, code in enumerate(self.codes):
            df = self.read_fdr(code, d_ymd)
            if df is None:
                continue
            self.merge_krx_items_hist(df, idx, code, self.codes[code])
            print(str(idx) + " : " + str(code) + "["+self.codes[code] + "] " + datetime.now().strftime('%Y-%m-%d %H:%M'))

    def execute_daily(self, d_ymd):
        self.merge_krx_items()
        self.merge_daily_price_ffdr(d_ymd)
        '''        
        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1,
                                   hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17,
                                   minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0,
                                   second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds
        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ... ".format(tmnext.strftime
                                                         ('%Y-%m-%d %H:%M')))
        t.start()
        '''




