import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
import re
import cx_Oracle as cx
import db.db_config as config


class MqCorpFinUpdate:
    def __init__(self):
        self.conn = cx.connect(config.user, config.pw, config.dsn)
        self.ymd = datetime.now().strftime("%Y%m%d")

    def __del__(self):
        self.conn.close()

    def get_krx_code(self):
        sql = "select ITEM_CD, ITEM_NM from mq_items where status='Y'"
        return pd.read_sql(sql=sql, con=self.conn)

    def get_fin_url(self, item_cd):
        url_tmpl = 'https://finance.naver.com/item/coinfo.nhn?code=%s&target=finsum_more'
        url = url_tmpl % item_cd
        return url

    def get_corp_fin(self, item_cd, gb):
        url = self.get_fin_url(item_cd)
        browser = webdriver.Chrome('C:\\bin\\chromedriver_win32\\chromedriver')
        browser.implicitly_wait(10)
        # browser.maximize_window()
        browser.get(url)
        browser.switch_to.frame(browser.find_element_by_id('coinfo_cp'))
        # 재무제표 연간 클릭하기
        if gb == 'Y':
            # browser.find_elements_by_xpath('//*[@class="schtab"][1]/tbody/tr/td[3]')[0].click()
            browser.find_elements_by_xpath('//*[@id="cns_td21"]')[0].click()
        elif gb == 'Q':
            browser.find_elements_by_xpath('//*[@id="cns_td22"]')[0].click()

        html0 = browser.page_source
        html1 = BeautifulSoup(html0, 'html.parser')
        '''
        제목 -> 기업개요(삼성전자)
        title0 = html1.find('head').find('title').text
        title0.split('-')[-1]
        '''
        html22 = html1.find('table', {'class': 'gHead01 all-width', 'summary': '주요재무정보를 제공합니다.'})
        # 재무제표 영역에서 날짜 불러오기
        thread0 = html22.find('thead')
        tr0 = thread0.find_all('tr')[1]
        th0 = tr0.find_all('th')
        # 날짜부분만 따로 저장
        date = []
        for i in range(len(th0)):
            # print(''.join(re.findall('[0-9/]', th0[i].text)).replace('/', '-'))
            date.append(''.join(re.findall('[0-9/]', th0[i].text)).replace('/', '-'))

        # 재무제표 영역에서 컬럼 및 본문 데이터 수집
        tbody0 = html22.find('tbody')
        tr0 = tbody0.find_all('tr')
        # 컬럼 수집
        # <th class="bg txt">영업이익</th>
        # re.sub 치환 (정규표현식, repl, string[,count])
        col = []
        for i in range(len(tr0)):
            if '\xa0' in tr0[i].find('th').text:
                tx = re.sub('\xa0', '', tr0[i].find('th').text)
            else:
                tx = tr0[i].find('th').text
            col.append(tx)

        # 본문 데이터 수집
        td = []
        for i in range(len(tr0)):
            td0 = tr0[i].find_all('td')
            td1 = []
            for j in range(len(td0)):
                if td0[j].text == '':
                    td1.append('0')
                else:
                    td1.append(td0[j].text)

            td.append(td1)

        td2 = list(map(list, zip(*td)))
        result = pd.DataFrame(td2, columns=col, index=date)
        result.rename(columns={"ROE(%)": "ROE", "ROA(%)": "ROA", "EPS(원)": "EPS", "PER(배)": "PER"
                               , "BPS(원)": "BPS", "PBR(배)": "PBR", "현금DPS(원)": "DPS"
                               , "현금배당성향(%)": "현금배당성향", "발행주식수(보통주)": "발행주식수"}, inplace=True)
        return result

    def test_corp_fin(self, df, item_cd):
        for r in df.itertuples():
            print(r[0])
            print(r.매출액)
            print(r.ROE)

    def x_validate(self, x):
        ret = str(x)
        if 'N' in ret:
            ret = '0'
        elif ',' in ret:
            ret = ret.replace(',', '')
        return ret

    def merge_corp_fin(self, item_cd, gb):
        df = self.get_corp_fin(item_cd=item_cd, gb=gb)
        with self.conn.cursor() as cursor:
            for r in df.itertuples():
                sql = f'''
                MERGE INTO METSTOCK.MQ_CORP_FIN d
                USING (
                  Select
                    '{item_cd}' as ITEM_CD,
                    '{r[0]}' as YM,
                    '{gb}' as GB,
                    '{self.x_validate(r.매출액)}' as SALES,
                    '{self.x_validate(r.영업이익)}' as PROFIT,
                    '{self.x_validate(r.세전계속사업이익)}' as SJ_PROFIT,
                    '{self.x_validate(r.당기순이익)}' as DANGGI,
                    '{self.x_validate(r.자산총계)}' as ASSET,
                    '{self.x_validate(r.부채총계)}' as DEBT,
                    '{self.x_validate(r.자본총계)}' as TOT_CAP,
                    '{self.x_validate(r.자본금)}' as CAP,
                    '{self.x_validate(r.영업활동현금흐름)}' as SALE_CF,
                    '{self.x_validate(r.투자활동현금흐름)}' as INV_CF,
                    '{self.x_validate(r.재무활동현금흐름)}' as FIN_CF,
                    '{self.x_validate(r.CAPEX)}' as CAPEX,
                    '{self.x_validate(r.FCF)}' as FCF,
                    '{self.x_validate(r.이자발생부채)}' as ACC_DEBT,
                    '{self.x_validate(r.영업이익률)}' as SALE_PROFIT_RT,
                    '{self.x_validate(r.순이익률)}' as NET_PROFIT_RT,
                    '{self.x_validate(r.ROE)}' as ROE,
                    '{self.x_validate(r.ROA)}' as ROA,
                    '{self.x_validate(r.부채비율)}' as DEBT_RT,
                    '{self.x_validate(r.자본유보율)}' as CAP_RES_RT,
                    '{self.x_validate(r.EPS)}' as EPS,
                    '{self.x_validate(r.PER)}' as PER,
                    '{self.x_validate(r.BPS)}' as BPS,
                    '{self.x_validate(r.PBR)}' as PBR,
                    '{self.x_validate(r.DPS)}' as DPS,
                    '{self.x_validate(r.현금배당수익률)}' as CASH_DIV_RT,
                    '{self.x_validate(r.현금배당성향)}' as CASH_DIV_TEND,
                    '{self.x_validate(r.발행주식수)}' as ISSUED_STOCK,
                    'sys' as REG_ID,
                    'sys' as UPD_ID,
                    sysdate as REG_DT,
                    sysdate as UPD_DT
                  From Dual) s
                ON
                  (d.ITEM_CD = s.ITEM_CD and 
                  d.YM = s.YM and 
                  d.GB = s.GB )
                WHEN MATCHED
                THEN
                UPDATE SET
                  d.SALES = s.SALES,
                  d.PROFIT = s.PROFIT,
                  d.SJ_PROFIT = s.SJ_PROFIT,
                  d.DANGGI = s.DANGGI,
                  d.ASSET = s.ASSET,
                  d.DEBT = s.DEBT,
                  d.TOT_CAP = s.TOT_CAP,
                  d.CAP = s.CAP,
                  d.SALE_CF = s.SALE_CF,
                  d.INV_CF = s.INV_CF,
                  d.FIN_CF = s.FIN_CF,
                  d.CAPEX = s.CAPEX,
                  d.FCF = s.FCF,
                  d.ACC_DEBT = s.ACC_DEBT,
                  d.SALE_PROFIT_RT = s.SALE_PROFIT_RT,
                  d.NET_PROFIT_RT = s.NET_PROFIT_RT,
                  d.ROE = s.ROE,
                  d.ROA = s.ROA,
                  d.DEBT_RT = s.DEBT_RT,
                  d.CAP_RES_RT = s.CAP_RES_RT,
                  d.EPS = s.EPS,
                  d.PER = s.PER,
                  d.BPS = s.BPS,
                  d.PBR = s.PBR,
                  d.DPS = s.DPS,
                  d.CASH_DIV_RT = s.CASH_DIV_RT,
                  d.CASH_DIV_TEND = s.CASH_DIV_TEND,
                  d.ISSUED_STOCK = s.ISSUED_STOCK,
                  d.REG_ID = s.REG_ID,
                  d.UPD_ID = s.UPD_ID,
                  d.REG_DT = s.REG_DT,
                  d.UPD_DT = s.UPD_DT
                WHEN NOT MATCHED
                THEN
                INSERT (
                  ITEM_CD, YM, GB,
                  SALES, PROFIT, SJ_PROFIT,
                  DANGGI, ASSET, DEBT,
                  TOT_CAP, CAP, SALE_CF,
                  INV_CF, FIN_CF, CAPEX,
                  FCF, ACC_DEBT, SALE_PROFIT_RT,
                  NET_PROFIT_RT, ROE, ROA,
                  DEBT_RT, CAP_RES_RT, EPS,
                  PER, BPS, PBR,
                  DPS, CASH_DIV_RT, CASH_DIV_TEND,
                  ISSUED_STOCK, REG_ID, UPD_ID,
                  REG_DT, UPD_DT)
                VALUES (
                  s.ITEM_CD, s.YM, s.GB,
                  s.SALES, s.PROFIT, s.SJ_PROFIT,
                  s.DANGGI, s.ASSET, s.DEBT,
                  s.TOT_CAP, s.CAP, s.SALE_CF,
                  s.INV_CF, s.FIN_CF, s.CAPEX,
                  s.FCF, s.ACC_DEBT, s.SALE_PROFIT_RT,
                  s.NET_PROFIT_RT, s.ROE, s.ROA,
                  s.DEBT_RT, s.CAP_RES_RT, s.EPS,
                  s.PER, s.BPS, s.PBR,
                  s.DPS, s.CASH_DIV_RT, s.CASH_DIV_TEND,
                  s.ISSUED_STOCK, s.REG_ID, s.UPD_ID,
                  s.REG_DT, s.UPD_DT)
                '''
                cursor.execute(sql)
                self.conn.commit()

    def execute_corp_fin(self):
        krx = self.get_krx_code()
        for r in krx.itertuples():
            item_cd = r.ITEM_CD
            try:
                time.sleep(0.5)
                self.merge_corp_fin(item_cd=item_cd, gb='Y')
                time.sleep(0.5)
                self.merge_corp_fin(item_cd=item_cd, gb='Q')
                print("%s merge_corp_fin process end ", item_cd)

            except Exception as err:
                print("%s merge_corp_fin 에러 발생 : %s", item_cd, str(err))

    def test_execute_corp_fin(self, item_cd):
        try:
            self.merge_corp_fin(item_cd=item_cd, gb='Y')
            self.merge_corp_fin(item_cd=item_cd, gb='Q')
            print("%s merge_corp_fin process end ", item_cd)
        except Exception as err:
            print("%s merge_corp_fin 에러 발생 : %s", item_cd, str(err))


if __name__ == '__main__':
    corp_fin = MqCorpFinUpdate()
    '''
    df = corp_fin.get_corp_fin('005930', 'Y')
    corp_fin.test_corp_fin(df, '005930')
    '''
    # corp_fin.merge_corp_fin(item_cd='060720', gb="Q")
    # print(corp_fin.get_krx_code().head())
    # corp_fin.test_execute_corp_fin('000020')
    corp_fin.execute_corp_fin()
    # corp_fin.merge_corp_fin(item_cd='000020', gb='Q')

