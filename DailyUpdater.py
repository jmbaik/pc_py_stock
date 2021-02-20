import pandas as pd
from Batch.StockItemsUpdate import *

# 종목 업데이트
items = MqItemsUpdate()
'''
items.merge_krx_items()
codes = items.codes
'''
items.execute_daily('2021-02-01')

