import os
import pandas as pd

# ========== 路径设置 ==========
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
data_quad_dir = os.path.join(data_dir, "data_quadruples")
os.makedirs(data_quad_dir, exist_ok=True)

# ===== 只合并结构图的三个文件 =====
file_names = [
    "basic_quadruples.csv",
    "managers_quadruples.csv",
    "fina_mainbz_quadruples.csv",
]

paths = [os.path.join(data_quad_dir, fn) for fn in file_names]

all_dfs = []

print("准备合并以下【结构图】四元组文件：")
for p in paths:
    if os.path.exists(p):
        print("  [OK] ", p)
        df = pd.read_csv(p, dtype=str).fillna("None")

        # 确保列名一致
        df = df[["head", "relation", "tail", "time"]]

        # 基础过滤
        df = df[
            (df["head"] != "None") &
            (df["relation"] != "None") &
            (df["tail"] != "None") &
            (df["time"] != "None") &
            (df["head"].str.strip() != "") &
            (df["relation"].str.strip() != "") &
            (df["tail"].str.strip() != "") &
            (df["time"].str.strip() != "")
        ]

        all_dfs.append(df)
    else:
        print("  [MISS] 找不到文件：", p)

if not all_dfs:
    print("❌ 没有发现任何可以合并的结构图四元组文件，请检查路径。")
else:
    # 纵向拼接
    df_all = pd.concat(all_dfs, ignore_index=True)

    # 去重
    before = len(df_all)
    df_all = df_all.drop_duplicates()
    after = len(df_all)

    out_path = os.path.join(data_quad_dir, "structure_quadruples_month.csv")
    df_all.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\n✔ 结构图四元组合并完成 → {out_path}")
    print(f"  合并前总条数：{before}")
    print(f"  去重后总条数：{after}")
    print("\n示例数据：")
    print(df_all.head())
