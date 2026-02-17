from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
OUT = Path("data/processed/amedas_nirasaki_hourly.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

# raw配下のCSVを全部読む（ファイル名は YYYY-MM 付き推奨）
files = sorted(RAW_DIR.glob("amedas_nirasaki_*.csv"))
if not files:
    raise SystemExit(f"No csv files found in {RAW_DIR} (pattern: amedas_nirasaki_*.csv)")

dfs = []
for f in files:
    df = pd.read_csv(f)
    df["__source"] = f.name
    dfs.append(df)

all_df = pd.concat(dfs, ignore_index=True)

# 単位つき列名を内部名へ
rename_map = {
    "降水量(mm)": "降水量",
    "気温(℃)": "気温",
    "露点温度(℃)": "露点温度",
    "蒸気圧(hPa)": "蒸気圧",
    "湿度(％)": "湿度",
    "平均風速(m/s)": "風速",
    "日照時間(h)": "日照時間",
    "降雪(cm)": "降雪",
    "積雪(cm)": "積雪",
}
all_df = all_df.rename(columns=rename_map)

# 日付・時刻の型
all_df["日付"] = pd.to_datetime(all_df["日付"], errors="coerce").dt.strftime("%Y-%m-%d")
all_df["時刻"] = pd.to_numeric(all_df["時刻"], errors="coerce")

# 数値列
num_cols = ["降水量","気温","露点温度","蒸気圧","湿度","風速","日照時間","降雪","積雪"]
for c in num_cols:
    if c in all_df.columns:
        all_df[c] = pd.to_numeric(all_df[c], errors="coerce")

# 最低限の列だけ残す（必要なら追加OK）
keep = ["日付","時刻","降水量","気温","露点温度","蒸気圧","湿度","風速","風向","日照時間","降雪","積雪"]
keep = [c for c in keep if c in all_df.columns]
all_df = all_df[keep].dropna(subset=["日付","時刻"]).copy()

# 重複除去（同じ日付×時刻が複数あったら最後を採用）
all_df = all_df.sort_values(["日付","時刻"]).drop_duplicates(subset=["日付","時刻"], keep="last")

all_df.to_csv(OUT, index=False, encoding="utf-8")
print("saved:", OUT, "rows:", len(all_df))
