import os
import re
import pandas as pd


# ========== 路径 ==========
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
quad_dir = os.path.join(data_dir, "data_quadruples")

in_path = os.path.join(quad_dir, "total_quadruples.csv")
out_path = os.path.join(quad_dir, "total_quadruples_train.csv")

# ========== 正则 ==========
DATE_RE  = re.compile(r"^\d{8}$")           # YYYYMMDD
RANGE_RE = re.compile(r"^(\d{8})-(\d{8})$") # YYYYMMDD-YYYYMMDD
OPEN_RE  = re.compile(r"^(\d{8})-$")        # YYYYMMDD-


OPEN_END_YYYYMM = "202601"

def norm(x):
    if x is None:
        return "None"
    s = str(x).strip()
    return s if s and s.lower() != "nan" else "None"

def yyyymm_iter(start_yyyymm: str, end_yyyymm: str):
    """生成闭区间 [start_yyyymm, end_yyyymm] 的所有 YYYYMM"""
    sy, sm = int(start_yyyymm[:4]), int(start_yyyymm[4:6])
    ey, em = int(end_yyyymm[:4]), int(end_yyyymm[4:6])

    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        yield f"{y:04d}{m:02d}"
        m += 1
        if m == 13:
            m = 1
            y += 1

def expand_time(t: str):
    t = norm(t)
    if t == "None":
        return []

    # 1) 单点日期：保留 YYYYMMDD
    if DATE_RE.match(t):
        return [t]

    # 2) 闭区间：按 YYYYMM 展开
    m = RANGE_RE.match(t)
    if m:
        start = m.group(1)
        end = m.group(2)
        start_yyyymm = start[:6]
        end_yyyymm = end[:6]
        return list(yyyymm_iter(start_yyyymm, end_yyyymm))

    # 3) 开区间：按 YYYYMM 展开到 202601
    m = OPEN_RE.match(t)
    if m:
        start = m.group(1)
        start_yyyymm = start[:6]
        return list(yyyymm_iter(start_yyyymm, OPEN_END_YYYYMM))

    return []


df = pd.read_csv(in_path, dtype=str).fillna("None")

rows = []
for _, row in df.iterrows():
    head = row.get("head", "None")
    rel  = row.get("relation", "None")
    tail = row.get("tail", "None")
    t    = row.get("time", "None")

    if head == "None" or rel == "None" or tail == "None":
        continue

    ts = expand_time(t)
    for tt in ts:
        rows.append([head, rel, tail, tt])

df_out = pd.DataFrame(rows, columns=["head", "relation", "tail", "time"])
df_out = df_out.drop_duplicates()

df_out.to_csv(out_path, index=False, encoding="utf-8-sig")

print("✔ 已生成训练用四元组：区间按月展开、单点保留 YYYYMMDD")
print("输出文件：", out_path)
print("总行数：", len(df_out))
print(df_out.head(10))
