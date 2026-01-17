import os
import pandas as pd

# ================== 路径设置 ==================
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
quad_dir = os.path.join(data_dir, "data_quadruples")

model_data_dir = os.path.join(data_dir, "data_for_model")
out_dir = os.path.join(model_data_dir, "STRUCTURE")
os.makedirs(out_dir, exist_ok=True)

# =====【1】输入文件：结构图合并后的四元组 =====
in_path = os.path.join(quad_dir, "structure_quadruples_month.csv")

# 输出
time2id_path = os.path.join(out_dir, "time2id.txt")
entity2id_path = os.path.join(out_dir, "entity2id.txt")
relation2id_path = os.path.join(out_dir, "relation2id.txt")

train_path = os.path.join(out_dir, "train.txt")
valid_path = os.path.join(out_dir, "valid.txt")
test_path  = os.path.join(out_dir, "test.txt")
stat_path  = os.path.join(out_dir, "stat.txt")

# ================== 读取四元组 ==================
df = pd.read_csv(in_path, dtype=str).fillna("None")

df = df[
    (df["head"] != "None") &
    (df["relation"] != "None") &
    (df["tail"] != "None") &
    (df["time"] != "None")
].copy()

# ================== 1) time2id（按月份排序） ==================
# time 是 YYYYMM
times = sorted(df["time"].unique(), key=lambda x: int(x))
time2id = {t: i for i, t in enumerate(times)}  # 0..T-1

with open(time2id_path, "w", encoding="utf-8") as f:
    for t, i in time2id.items():
        f.write(f"{t}\t{i}\n")

# ================== 2) relation2id ==================
relations = list(pd.unique(df["relation"]))
relation2id = {r: i for i, r in enumerate(relations)}

with open(relation2id_path, "w", encoding="utf-8") as f:
    for r, i in relation2id.items():
        f.write(f"{r}\t{i}\n")

# ================== 3) entity2id ==================
entities = list(pd.unique(pd.concat([df["head"], df["tail"]], ignore_index=True)))
entity2id = {e: i for i, e in enumerate(entities)}

with open(entity2id_path, "w", encoding="utf-8") as f:
    for e, i in entity2id.items():
        f.write(f"{e}\t{i}\n")

# ================== ID 化四元组 ==================
df_ids = pd.DataFrame({
    "head": df["head"].map(entity2id),
    "relation": df["relation"].map(relation2id),
    "tail": df["tail"].map(entity2id),
    "time": df["time"].map(time2id),
})

# ================== 8:1:1 按时间划分 ==================
df_ids["time"] = df_ids["time"].astype(int)
df_ids = df_ids.sort_values("time").reset_index(drop=True)

T_max = df_ids["time"].max()
train_end = int(T_max * 0.8)
valid_end = int(T_max * 0.9)

df_train = df_ids[df_ids["time"] <= train_end]
df_valid = df_ids[(df_ids["time"] > train_end) & (df_ids["time"] <= valid_end)]
df_test  = df_ids[df_ids["time"] > valid_end]

def save_txt(df, path):
    df.astype(int).to_csv(path, sep="\t", header=False, index=False)

save_txt(df_train, train_path)
save_txt(df_valid, valid_path)
save_txt(df_test, test_path)

# ================== stat.txt（ ==================
with open(stat_path, "w", encoding="utf-8") as f:
    f.write(f"{len(entity2id)}\t{len(relation2id)}\t{len(time2id)}\n")

# ================== 打印信息 ==================
print("✔ 结构图数据集构建完成")
print("输入：", in_path)
print("输出目录：", out_dir)
print("实体数 =", len(entity2id))
print("关系数 =", len(relation2id))
print("时间步数 =", len(time2id))
print("train / valid / test =", len(df_train), len(df_valid), len(df_test))
