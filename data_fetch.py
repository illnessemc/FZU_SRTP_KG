import pandas as pd
import tushare as ts
import os

ts.set_token("f8a65c06d4193356d730640695418b9214465586e5d9a79846557774")
pro = ts.pro_api()

df = pro.stock_company(exchange='SZSE', fields='ts_code,chairman,manager,secretary,reg_capital,setup_date,province')
##print(df)

os.makedirs("data_raw", exist_ok=True)
df.to_csv("data_raw/SZSE.csv", index=False,encoding='utf-8')