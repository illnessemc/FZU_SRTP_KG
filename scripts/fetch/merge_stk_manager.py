import os
import pandas as pd
import glob

def merge_stk_manager():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    data_raw_dir = os.path.join(project_root, "data_raw")
    managers_dir = os.path.join(data_raw_dir, "stk_managers")
    out_path = os.path.join(data_raw_dir, "stk_managers_all.csv")
    print(managers_dir)
    files = glob.glob(os.path.join(managers_dir, "*.csv"))
    print("发现管理层单公司文件数：", len(files))
    all_dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str)
            all_dfs.append(df)
        except Exception as e:
            print(f"[ERR] 文件读取失败 {f}: {e}")
    if not all_dfs:
        print("[WARN] 没有发现可合并的文件")
        return

    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.fillna("None")
    df_all.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[OK] 已合并保存到: {out_path}")
    print("总记录数：", len(df_all))
    print(df_all.head())

# 让这个文件单独运行时也能用
if __name__ == "__main__":
    merge_stk_manager()