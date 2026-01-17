import os
import re
import pandas as pd

# ========== 路径设置 ==========
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

data_dir = os.path.join(project_root, "data")
data_raw_dir = os.path.join(data_dir, "data_raw")
output_dir = os.path.join(data_dir, "data_quadruples")
config_dir = os.path.join(project_root, "config")

os.makedirs(output_dir, exist_ok=True)

# ========== 读取数据 ==========
mgr_path = os.path.join(data_raw_dir, "stk_managers_all.csv")
codes_path = os.path.join(config_dir, "codes.csv")

df_mgr = pd.read_csv(mgr_path, dtype=str).fillna("None")
df_codes = pd.read_csv(codes_path, dtype=str).fillna("None")

# ts_code -> 公司名称 映射
code2name = dict(zip(df_codes["ts_code"], df_codes["name"]))

quadruples = []

def add_q(head, relation, tail, time):
    quadruples.append([head, relation, tail, time])


DATE_RE = re.compile(r"^\d{8}$")
END_YYYYMM = "202506"

def date_to_yyyymm(d):
    if d is None:
        return None
    d = str(d).strip()
    if not DATE_RE.match(d):
        return None
    return d[:6]

def iter_months(start_yyyymm, end_yyyymm):
    if start_yyyymm is None or end_yyyymm is None:
        return
    if len(start_yyyymm) != 6 or len(end_yyyymm) != 6:
        return
    sy, sm = int(start_yyyymm[:4]), int(start_yyyymm[4:6])
    ey, em = int(end_yyyymm[:4]), int(end_yyyymm[4:6])
    if not (1 <= sm <= 12 and 1 <= em <= 12):
        return

    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        yield f"{y:04d}{m:02d}"
        m += 1
        if m == 13:
            m = 1
            y += 1

# ========== 遍历数据构建四元组（按月展开） ==========
skipped_bad_date = 0

for _, row in df_mgr.iterrows():
    ts_code = row.get("ts_code", "None")
    company = code2name.get(ts_code, ts_code)

    manager = row.get("name", "None")
    title = row.get("title", "None")
    lev = row.get("lev", "None")

    begin = row.get("begin_date", "None")
    end = row.get("end_date", "None")

    if company == "None" or manager == "None" or begin in ("None", "", "nan", "NaN"):
        continue

    begin_m = date_to_yyyymm(begin)
    if begin_m is None:
        skipped_bad_date += 1
        continue

    # 闭区间：取 end 的月份；开区间：展开到 END_YYYYMM
    if end not in ("None", "", "nan", "NaN"):
        end_m = date_to_yyyymm(end)
        if end_m is None:
            # end 非法：按开区间处理（到 END_YYYYMM）
            end_m = END_YYYYMM
    else:
        end_m = END_YYYYMM

    # 若 begin 在 end 之后，跳过
    if int(begin_m) > int(end_m):
        continue

    for month in iter_months(begin_m, end_m):
        if title != "None":
            add_q(company, title, manager, month)
        if lev != "None":
            add_q(company, lev, manager, month)

# ========== 保存 ==========
out_path = os.path.join(output_dir, "managers_quadruples.csv")
df_q = pd.DataFrame(quadruples, columns=["head", "relation", "tail", "time"])
df_q = df_q.drop_duplicates()
df_q.to_csv(out_path, index=False, encoding="utf-8-sig")

print("✔ 管理层（月粒度，区间已按月展开）四元组生成完成 →", out_path)
print("四元组数量：", len(df_q))
print(df_q.head())
