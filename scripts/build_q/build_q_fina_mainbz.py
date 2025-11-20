import os
import pandas as pd

# ========== 路径设置 ==========
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
data_raw_dir = os.path.join(data_dir, "data_raw")
output_dir = os.path.join(data_dir, "data_quadruples")
config_dir    = os.path.join(project_root, "config")

os.makedirs(output_dir, exist_ok=True)

# 读取 fina_mainbz 汇总数据
fina_path  = os.path.join(data_raw_dir, "fina_mainbz_all.csv")
codes_path = os.path.join(config_dir, "codes.csv")

df_fina  = pd.read_csv(fina_path, dtype=str).fillna("None")
df_codes = pd.read_csv(codes_path, dtype=str).fillna("None")

# ts_code -> 公司名称映射
code2name = dict(zip(df_codes["ts_code"], df_codes["name"]))

quadruples = []


def add_q(head, relation, tail, time):
    quadruples.append([head, relation, tail, time])

#####遍历数构建四元组

for _, row in df_fina.iterrows():

    ts_code = row["ts_code"]
    company = code2name.get(ts_code, ts_code)
    bz_item=row["bz_item"]
    bz_sales=row["bz_sales"]
    bz_profit=row["bz_profit"]
    bz_cost=row["bz_cost"]
    end_date = row["end_date"]
    if bz_item == "None":
        continue
    time_str = end_date
    if (bz_item != "None"):
        add_q(company, "主营业务", bz_item, time_str)
    if (bz_sales != "None"):
        add_q(company,"收入",bz_sales, time_str)
    if (bz_profit != "None"):
        add_q(company,"利润",bz_profit, time_str)
    if (bz_cost != "None"):
        add_q(company,"成本",bz_cost, time_str)
# 保存四元组
out_path = os.path.join(output_dir, "fina_mainbz_quadruples.csv")
df_q = pd.DataFrame(quadruples, columns=["head", "relation", "tail", "time"])
df_q.to_csv(out_path, index=False, encoding="utf-8-sig")

print("✔ 主营业务四元组已生成 ->", out_path)
print("数量：", len(df_q))
print(df_q.head())
