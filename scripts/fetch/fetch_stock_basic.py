import pandas as pd
import tushare as ts
import os
#####接口调用
ts.set_token("f8a65c06d4193356d730640695418b9214465586e5d9a79846557774")
pro = ts.pro_api()

######文件目录获取
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
data_dir_ = os.path.join(project_root, "data")
data_dir=os.path.join(data_dir_,"data_raw")

config_dir=os.path.join(project_root,"config")

os.makedirs(data_dir_,exist_ok=True)
os.makedirs(data_dir, exist_ok=True)
os.makedirs(config_dir, exist_ok=True)

#####调用接口
df = pro.stock_basic(fields="ts_code,name,area,industry,market,exchange,list_date,delist_date")
print("[INFO] 原始 stock_basic 行数：", len(df))

#####过滤北交所
df = df[df["exchange"] != "BSE"]

#####轻度清洗
df = df.astype(str)  # 防止某些列是 float/NaN
df["delist_date"] = df["delist_date"].replace(["nan", "None"], "").fillna("")  # 保险一步
df.loc[df["delist_date"].str.strip() == "", "delist_date"] = "None"

#####保存 stock_basic
stock_basic_path = os.path.join(data_dir, "stock_basic.csv")
df.to_csv(stock_basic_path, index=False, encoding="utf-8-sig")
print("[OK] 已保存轻度清洗的 stock_basic ->", stock_basic_path)

#####保存codes 股票代码和名字对应
df_codes = df[["ts_code", "name"]].dropna().drop_duplicates()
codes_path = os.path.join(config_dir, "codes.csv")
df_codes.to_csv(codes_path, index=False, encoding="utf-8-sig")


