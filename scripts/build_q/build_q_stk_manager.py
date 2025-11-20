import os
import pandas as pd

# 路径设置
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
data_raw_dir = os.path.join(data_dir, "data_raw")
output_dir = os.path.join(data_dir, "data_quadruples")
config_dir    = os.path.join(project_root, "config")

os.makedirs(output_dir, exist_ok=True)

# 读取数据并且映射
mgr_path   = os.path.join(data_raw_dir, "stk_managers_all.csv")
codes_path = os.path.join(config_dir, "codes.csv")

df_mgr   = pd.read_csv(mgr_path, dtype=str).fillna("None")
df_codes = pd.read_csv(codes_path, dtype=str).fillna("None")

# ts_code -> 公司名称 映射
code2name = dict(zip(df_codes["ts_code"], df_codes["name"]))

quadruples = []

def add_q(head, relation, tail, time):
    quadruples.append([head, relation, tail, time])


#####遍历数据构建两类四元组
for _, row in df_mgr.iterrows():
    ts_code =row["ts_code"]

    company = code2name.get(ts_code, ts_code)
    manager=row["name"]
    title = row["title"]
    lev = row["lev"]
    begin= row["begin_date"]
    end= row["end_date"]

    if company == "None" or manager == "None" or begin == "None":
        continue

    time_str = f"{begin}-" if end == "None" else f"{begin}-{end}"

    if title != "None":
        add_q(company, title, manager, time_str)

    if lev != "None":
        add_q(company, lev, manager, time_str)

# 保存四元组
out_path = os.path.join(output_dir, "managers_quadruples.csv")
df_q = pd.DataFrame(quadruples, columns=["head", "relation", "tail", "time"])
df_q.to_csv(out_path, index=False, encoding="utf-8-sig")

print("管理层四元组已生成 ->", out_path)
print("四元组数量：", len(df_q))
print(df_q.head())
