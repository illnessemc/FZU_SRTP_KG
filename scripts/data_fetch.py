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
df_basic=df_basic[df_basic['exchange'] != 'BSE']
csv_path = os.path.join(data_dir, 'stock_basic.csv')

df_basic.to_csv(csv_path, index=False,encoding='utf-8')

#### 将所有股票代码保存到codes.csv方便后续调用

df_basic = pd.read_csv(csv_path, dtype=str)

df_codes = df_basic[['ts_code', 'name']].dropna().drop_duplicates()

allowed_codes = set(df_codes['ts_code'])  ###所有沪深代码

config_dir = os.path.join(project_root, 'config')
os.makedirs(config_dir, exist_ok=True)

csv_path = os.path.join(config_dir, 'codes.csv',)
df_codes.to_csv(csv_path, index=False,encoding='utf-8')

### 上市公司管理层信息

df_basic = pro.stk_managers(fileds='ts_code,name,edu,lev,begin_date,end_date')
df_basic = df_basic[df_basic['ts_code'].isin(allowed_codes)]

csv_path = os.path.join(data_dir, 'stk_managers.csv',)
df_basic.to_csv(csv_path, index=False,encoding='utf-8')

### 上市公司主营业务及其盈利情况

config_dir = os.path.join(project_root, 'config')
codes_path = os.path.join(config_dir, 'codes.csv')
df_codes = pd.read_csv(codes_path, dtype=str)
code_list = df_codes['ts_code'].dropna().unique().tolist()

fina_path=os.path.join(data_dir, 'fina_mainbz')
os.makedirs(fina_path, exist_ok=True)

for item in code_list:
    try:
        df_basic=pro.fina_mainbz(ts_code=item)
        if (len(df_basic)>0):
            outpath=f"{fina_path}/{item}.csv"
            df_basic.to_csv(outpath, index=False, encoding='utf-8')
    except:
        continue

### 股票市场每日前十大成交数据
hsgt_top10_path=os.path.join(data_dir, 'hsgt_top10')
os.makedirs(hsgt_top10_path, exist_ok=True)
start_date = "20240101"
end_date   = "20250801"
trade_cal = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
trade_dates = trade_cal[trade_cal['is_open'] == 1]['cal_date'].tolist()
for item in trade_dates:
    try:
        df_baisc=pro.hsgt_top10(trade_date=item)
        if (len(df_baisc)>0):
            outpath=f"{hsgt_top10_path}/{item}_hsgt_top10.csv"
            df_baisc.to_csv(outpath, index=False, encoding='utf-8')
    except:
        continue
