import pandas as pd
import tushare as ts
import os

ts.set_token("f8a65c06d4193356d730640695418b9214465586e5d9a79846557774")
pro = ts.pro_api()

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
data_dir = os.path.join(project_root, 'data_raw')

os.makedirs(data_dir, exist_ok=True)

#### 股票基本信息

df_basic = pro.stock_basic(fields='ts_code,name,area,industry,market,exchange,list_date,delist_date')

csv_path = os.path.join(data_dir, 'stock_basic.csv')

df_basic.to_csv(csv_path, index=False)

#### 将所有股票代码保存到codes.csv方便后续调用

df_basic = pd.read_csv(csv_path, dtype=str)
df_basic = df_basic[df_basic['exchange'] != 'BSE']
df_codes = df_basic[['ts_code', 'name']].dropna().drop_duplicates()

allowed_codes = set(df_codes['ts_code'])  ###所有沪深代码

config_dir = os.path.join(project_root, 'config')
os.makedirs(config_dir, exist_ok=True)

csv_path = os.path.join(config_dir, 'codes.csv')
df_codes.to_csv(csv_path, index=False)

#### 上市公司管理层信息

df_basic = pro.stk_managers(fileds='ts_code,name,edu,lev,begin_date,end_date')
df_basic = df_basic[df_basic['ts_code'].isin(allowed_codes)]

csv_path = os.path.join(data_dir, 'stk_managers.csv')
df_basic.to_csv(csv_path, index=False)

#### 上市公司主营业务及其盈利情况

#### 股票市场每日前十大成交数据

basic = pro.hsgt_top10()
