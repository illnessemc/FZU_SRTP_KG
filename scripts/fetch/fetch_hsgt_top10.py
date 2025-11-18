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
data_raw_dir=os.path.join(project_root,"data_raw")
config_dir=os.path.join(project_root,"config")

os.makedirs(data_raw_dir, exist_ok=True)

hsgt_top10_dir=os.path.join(data_raw_dir,"hsgt_top10")
os.makedirs(hsgt_top10_dir, exist_ok=True)

#####读取codes.csv
codes_path = os.path.join(config_dir, "codes.csv")
df_codes = pd.read_csv(codes_path, dtype=str)
codes = df_codes["ts_code"].tolist()
print("需要获取管理层数据的公司数量：", len(codes))


#####获取成交日期
start_date = "20240101"   # 你要的开始日期
end_date = "20251101"     # 修改成你需要的结束日期

trade_cal = pro.trade_cal(exchange="SSE",
                          start_date=start_date,
                          end_date=end_date)

trade_dates = trade_cal[trade_cal["is_open"] == 1]["cal_date"].tolist()

print("需要获取的交易日数量：", len(trade_dates))
print(trade_dates)
#####获取每日成交前十
def fill_empty_date(col): #将空值用None
    col = col.astype(str)
    col = col.replace(["nan", "NaN"], "")
    col = col.str.strip()
    col[col == ""] = "None"
    return col


all_dfs = []

for d in trade_dates:
    try:
        df = pro.hsgt_top10(trade_date=d)

        if df is not None and len(df) > 0:

            # 清洗 trade_date
            if "trade_date" in df.columns:
                df["trade_date"] = fill_empty_date(df["trade_date"])

            # 保存单日文件
            out_file = os.path.join(hsgt_top10_dir, f"{d}.csv")
            df.to_csv(out_file, index=False, encoding="utf-8-sig")

            print(f"[OK] {d} 记录 {len(df)}")

            # 加入合并列表
            all_dfs.append(df)

    except Exception as e:
        print(f"[ERR] {d} -> {e}")

    sleep(1.2)  # 限制访问速度（最多 60 次/分钟）


merged_path = os.path.join(data_raw_dir, "hsgt_top10_all.csv")

if all_dfs:
    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.fillna("None")
    df_all.to_csv(merged_path, index=False, encoding="utf-8-sig")
    print(f"[DONE] 已合并保存到：{merged_path}")
    print("总记录数：", len(df_all))
else:
    print("[WARN] 没有获取到任何 hsgt_top10 数据")