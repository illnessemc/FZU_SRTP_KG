import os
import pandas as pd

# ================== 路径设置 ==================
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
quad_dir = os.path.join(data_dir, "data_quadruples")
modle_data_dir = os.path.join(data_dir, "data_for_model")
out_dir = os.path.join(modle_data_dir, "HSGT_TOP10")
os.makedirs(out_dir, exist_ok=True)

in_path = os.path.join(quad_dir, "hsgt_top10_quadruples.csv")

time2id_path = os.path.join(out_dir, "time2id.txt")
entity2id_path = os.path.join(out_dir, "entity2id.txt")
relation2id_path = os.path.join(out_dir, "relation2id.txt")
out_quad_path = os.path.join(out_dir, "hsgt_top10_ids.csv")

# 8:1:1 划分输出
train_path = os.path.join(out_dir, "train.txt")
valid_path = os.path.join(out_dir, "valid.txt")
test_path  = os.path.join(out_dir, "test.txt")

# ================== 读取四元组 ==================
df = pd.read_csv(in_path, dtype=str).fillna("None")

# 基础过滤
df = df[
    (df["head"] != "None") &
    (df["relation"] != "None") &
    (df["tail"] != "None") &
    (df["time"] != "None")
].copy()

# ================== 1) time2id（按日期排序，交易日序） ==================
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

# ================== 3) entity2id（head + tail） ==================
entities = list(pd.unique(pd.concat([df["head"], df["tail"]], ignore_index=True)))
entity2id = {e: i for i, e in enumerate(entities)}

with open(entity2id_path, "w", encoding="utf-8") as f:
    for e, i in entity2id.items():
        f.write(f"{e}\t{i}\n")

# ================== 输出ID化四元组（全量） ==================
df_ids = pd.DataFrame({
    "head": df["head"].map(entity2id),
    "relation": df["relation"].map(relation2id),
    "tail": df["tail"].map(entity2id),
    "time": df["time"].map(time2id),
})

df_ids.to_csv(out_quad_path, index=False, encoding="utf-8-sig")

# ================== 4) 8:1:1 按时间划分 train/valid/test ==================
df_ids["time"] = df_ids["time"].astype(int)
df_ids = df_ids.sort_values("time").reset_index(drop=True)

T_max = df_ids["time"].max()
train_end = int(T_max * 0.8)
valid_end = int(T_max * 0.9)

df_train = df_ids[df_ids["time"] <= train_end]
df_valid = df_ids[(df_ids["time"] > train_end) & (df_ids["time"] <= valid_end)]
df_test  = df_ids[df_ids["time"] > valid_end]

def save_txt(df, path):
    df.astype(int).to_csv(
        path,
        sep="\t",
        header=False,
        index=False
    )
save_txt(df_train, train_path)
save_txt(df_valid, valid_path)
save_txt(df_test, test_path)

# ================== 5) 生成 stat.txt（GDELT/RE-GCN 标准格式） ==================
stat_path = os.path.join(out_dir, "stat.txt")

num_entities = len(entity2id)
num_relations = len(relation2id)
num_timestamps = len(time2id)

with open(stat_path, "w", encoding="utf-8") as f:
    f.write(f"{num_entities}\t{num_relations}\t{num_timestamps}\n")

# ================== 打印信息 ==================
print(" hsgt_top10 数据集索引文件生成完成（一次运行）")
print("输入四元组：", in_path)
print("输出目录：", out_dir)
print("time2id 数量 T =", len(time2id), "时间范围：", times[0], "→", times[-1])
print("relation2id 数量 =", len(relation2id), "关系：", relations)
print("entity2id 数量 =", len(entity2id))
print("ID化四元组输出：", out_quad_path)

print("\n 8:1:1 按时间划分完成（追加）")
print("time 范围：0 →", T_max, "| train_end =", train_end, "| valid_end =", valid_end)
print("train：", train_path, "条数 =", len(df_train))
print("valid：", valid_path, "条数 =", len(df_valid))
print("test ：", test_path,  "条数 =", len(df_test))

print("\n stat.txt（标准三数字格式）已生成：", stat_path)
print("内容：", num_entities, num_relations, num_timestamps)

print("\n示例（train 前5行）：")
print(df_train.head())
