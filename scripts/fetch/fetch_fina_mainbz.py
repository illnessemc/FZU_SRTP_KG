import pandas as pd
import tushare as ts
import os
from time import sleep
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

fina_mainbz_dir=os.path.join(data_raw_dir,"fina_mainbz")
os.makedirs(fina_mainbz_dir, exist_ok=True)

#####读取codes.csv
codes_path = os.path.join(config_dir, "codes.csv")
df_codes = pd.read_csv(codes_path, dtype=str)
codes = df_codes["ts_code"].tolist()
print("需要获取管理层数据的公司数量：", len(codes))

#####抓取每个公司的主营业务
def fill_empty_date(col): #####将不存在的日期用None保存
    col = col.astype(str)
    col = col.replace(["nan", "NaN"], "")
    col = col.str.strip()
    col[col == ""] = "None"
    return col

all_dfs = []

for code in codes:
    try:
        df = pro.fina_mainbz(ts_code=code)
        if df is not None and len(df) > 0:
            # 轻度清洗 end_date
            if "end_date" in df.columns:
                df["end_date"] = (df["end_date"])
            # 保存单文件
            out_file = os.path.join(fina_mainbz_dir, f"{code}.csv")
            df.to_csv(out_file, index=False, encoding="utf-8-sig")
            print(f"[OK] {code} 主营业务记录 {len(df)}")
            all_dfs.append(df)
    except Exception as e:
        print(f"[ERR] {code} -> {e}")
    sleep(1.2)  # 限流保护

#####合并所有数据位一个文件
merged_path = os.path.join(data_raw_dir, "fina_mainbz_all.csv")

if all_dfs:
    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.fillna("None")
    df_all.to_csv(merged_path, index=False, encoding="utf-8-sig")

    print(f"[DONE] 已合并 {len(all_dfs)} 家公司的主营业务数据 -> {merged_path}")
    print("总记录数：", len(df_all))
else:
    print("[WARN] 没有获取到任何 fina_mainbz 数据")