import os
import pandas as pd


# ========== 路径设置 ==========
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
data_raw_dir = os.path.join(data_dir, "data_raw")
output_dir = os.path.join(data_dir, "data_quadruples")
config_dir = os.path.join(project_root, "config")

os.makedirs(output_dir, exist_ok=True)

hsgt_top10_path = os.path.join(data_raw_dir, "hsgt_top10_all.csv")
codes_path = os.path.join(config_dir, "codes.csv")

# ========== 读取数据 ==========
df_hsgt = pd.read_csv(hsgt_top10_path, dtype=str).fillna("None")
df_codes = pd.read_csv(codes_path, dtype=str).fillna("None")

# ts_code -> 公司名称
code2name = dict(zip(df_codes["ts_code"], df_codes["name"]))

# 数值字段转 float（用于排序）
for col in ["close", "change", "amount"]:
    df_hsgt[col] = pd.to_numeric(df_hsgt[col], errors="coerce")

quadruples = []

def add_q(head, relation, tail, time):
    quadruples.append([head, relation, tail, time])


# ========== 核心逻辑：按交易日生成 TOP 排名 ==========
for trade_date, g in df_hsgt.groupby("trade_date"):

    # --- 1. 收盘价 TOP ---
    g_close = g.sort_values("close", ascending=False)
    for i, row in enumerate(g_close.itertuples(), start=1):
        if i > 10:
            break
        company = code2name.get(row.ts_code, row.name)
        add_q(company, "收盘价", f"TOP{i}", trade_date)

    # --- 2. 涨跌额 TOP ---
    g_change = g.sort_values("change", ascending=False)
    for i, row in enumerate(g_change.itertuples(), start=1):
        if i > 10:
            break
        company = code2name.get(row.ts_code, row.name)
        add_q(company, "涨跌额", f"TOP{i}", trade_date)

    # --- 3. 成交金额 TOP ---
    g_amount = g.sort_values("amount", ascending=False)
    for i, row in enumerate(g_amount.itertuples(), start=1):
        if i > 10:
            break
        company = code2name.get(row.ts_code, row.name)
        add_q(company, "成交金额", f"TOP{i}", trade_date)

    # --- 4. 资金排名（Tushare 原始 rank） ---
    for row in g.itertuples():
        if row.rank != "None":
            company = code2name.get(row.ts_code, row.name)
            add_q(company, "资金排名", f"TOP{int(row.rank)}", trade_date)


# ========== 保存结果 ==========
out_path = os.path.join(output_dir, "hsgt_top10_quadruples.csv")
df_q = pd.DataFrame(quadruples, columns=["head", "relation", "tail", "time"])
df_q = df_q.drop_duplicates()
df_q.to_csv(out_path, index=False, encoding="utf-8-sig")

print("✔ 已按 TOP 排名重生成 hsgt_top10 四元组 →", out_path)
print("四元组数量：", len(df_q))
print(df_q.head(10))
