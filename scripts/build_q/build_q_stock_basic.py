import os
import pandas as pd

# 路径设置
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
data_raw_dir = os.path.join(data_dir, "data_raw")
output_dir = os.path.join(data_dir, "data_quadruples")

os.makedirs(data_dir,exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

# 读取 stock_basic
basic_path = os.path.join(data_raw_dir, "stock_basic.csv")
df = pd.read_csv(basic_path, dtype=str).fillna("None")

quadruples = []

def add_q(head, relation, tail, time):
    quadruples.append([head, relation, tail, time])

# 构建三类四元组
for _, row in df.iterrows():

    company = row["name"]      # head 用公司名称
    industry = row["industry"]
    market = row["market"]
    area = row["area"]
    time = row["list_date"] if row["list_date"] != "None" else "None"

    # 1. 行业
    if industry != "None":
        add_q(company, "industry", industry, time)

    # 2. 市场（主板/创业板…）
    if market != "None":
        add_q(company, "market", market, time)

    # 3. 地区（深圳/北京/云南等）
    if area != "None":
        add_q(company, "area", area, time)

# 保存四元组
out_path = os.path.join(output_dir, "basic_quadruples.csv")
df_q = pd.DataFrame(quadruples, columns=["head", "relation", "tail", "time"])
df_q.to_csv(out_path, index=False, encoding="utf-8-sig")

print("✔ 已生成 stock_basic 四元组 →", out_path)
print("四元组数量：", len(df_q))
print(df_q.head())
