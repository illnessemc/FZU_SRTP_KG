import pandas as pd
import tushare as ts
import os
from time import sleep
from merge_stk_manager import merge_stk_manager
#####接口调用
ts.set_token("f8a65c06d4193356d730640695418b9214465586e5d9a79846557774")
pro = ts.pro_api()

######文件目录获取
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
data_dir = os.path.join(project_root, "data")
data_raw_dir=os.path.join(data_dir,"data_raw")
config_dir=os.path.join(project_root,"config")

os.makedirs(data_dir,exist_ok=True)
os.makedirs(data_raw_dir, exist_ok=True)

mangers_dir=os.path.join(data_raw_dir,"stk_managers")
os.makedirs(mangers_dir, exist_ok=True)

#####读取codes.csv
codes_path = os.path.join(config_dir, "codes.csv")
df_codes = pd.read_csv(codes_path, dtype=str)
codes = df_codes["ts_code"].tolist()
print("需要获取管理层数据的公司数量：", len(codes))

######抓取每个公司的高管数据

def fill_empty_date(col): #####将不存在的日期用None保存
    col = col.astype(str)
    col = col.replace(["nan", "NaN"], "")
    col = col.str.strip()
    col[col == ""] = "None"
    return col

for code in codes:
    try:
        df = pro.stk_managers(ts_code=code)
        if df is None or len(df) == 0:
            continue
        date_cols = [c for c in ["begin_date", "end_date","ann_date"] if c in df.columns]
        for c in date_cols:
            df[c] = fill_empty_date(df[c])
        df.to_csv(f"{mangers_dir}/{code}.csv", index=False, encoding="utf-8-sig")
        print(f"[OK] {code} 记录数: {len(df)}")
    except Exception as e:
        print(f"[ERR] {code} {e}")
    sleep(0.4)

#####合并数据
print("[INFO] 所有公司管理层数据抓取完成，开始合并…")
merge_stk_manager()
print("[DONE] fetch + merge 全部完成")