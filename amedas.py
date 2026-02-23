# -*- coding: utf-8 -*-
from io import StringIO

import os
import time
import calendar

import requests
import pandas as pd

# =========================
# ★設定（ここだけ触ればOK）
# =========================

YEAR = 2026
MONTH = 2  # ← 月1回運用なら、ここだけ変えて実行（例：2月なら 2）

PREC_NO = 49
BLOCK_NO = 1024
BASE_URL = "https://www.data.jma.go.jp/stats/etrn/view/hourly_a1.php"

OUTPUT_CSV = "data/processed/amedas_nirasaki_hourly.csv"

COLUMNS = [
    "時刻", "降水量(mm)", "気温(℃)", "露点温度(℃)", "蒸気圧(hPa)", "湿度(％)",
    "平均風速(m/s)", "風向", "日照時間(h)", "降雪(cm)", "積雪(cm)"
]

def harmonize_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({"": pd.NA, "///": pd.NA})

    pairs = {
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

    for unit_col, plain_col in pairs.items():
        if unit_col in df.columns:
            if plain_col not in df.columns:
                df[plain_col] = pd.NA
            df[plain_col] = df[plain_col].fillna(df[unit_col])

    drop_cols = [c for c in pairs.keys() if c in df.columns]
    return df.drop(columns=drop_cols)

def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def to_int_hour(x):
    """
    '01' / '1' / 1 / '24' などを 0-23 の int に寄せる
    """
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return pd.NA
    try:
        h = int(float(s))
    except ValueError:
        return pd.NA

    # 気象庁の表で 24 が出る場合は 0 に寄せる（念のため）
    if h == 24:
        h = 0
    return h


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    ・必須の「日付」「時刻」を正規化
    ・空文字 / '///' を欠損へ
    """
    # 欠損表現を統一
    df = df.replace({"": pd.NA, "///": pd.NA})

    # 日付（YYYY-MM-DD）に寄せる
    df["日付"] = pd.to_datetime(df["日付"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 時刻を int 化
    df["時刻"] = df["時刻"].apply(to_int_hour).astype("Int64")

    # 必須キーが欠損の行は落とす（ここが壊れると後工程が全部崩れるので）
    df = df.dropna(subset=["日付", "時刻"])

    # 並び順を安定化
    df = df.sort_values(["日付", "時刻"]).reset_index(drop=True)
    return df


def scrape_one_day_http(year: int, month: int, day: int) -> pd.DataFrame:
    """
    1日分をHTTPで取得して、COLUMNSに揃えたDataFrame（日付列なし）を返す
    """
    url = f"{BASE_URL}?prec_no={PREC_NO}&block_no={BLOCK_NO}&year={year}&month={month}&day={day}&view=p1"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; mariko-amedas-bot/1.0)"}

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    if not tables:
        return pd.DataFrame(columns=COLUMNS)

    df = tables[0].copy()

    # --- MultiIndex列を1段に落とす（2段目を採用） ---
    df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]

    # --- 列名をあなたのスキーマに揃える ---
    df = df.rename(columns={
        "時": "時刻",
        "降水量 (mm)": "降水量(mm)",
        "気温 (℃)": "気温(℃)",
        "露点 温度 (℃)": "露点温度(℃)",
        "蒸気圧 (hPa)": "蒸気圧(hPa)",
        "湿度 (％)": "湿度(％)",
        "平均風速 (m/s)": "平均風速(m/s)",
        "風向": "風向",
        "日照 時間 (h)": "日照時間(h)",
        "降雪 (cm)": "降雪(cm)",
        "積雪 (cm)": "積雪(cm)",
    })

    # 欠損表現をNAに
    df = df.replace({"": pd.NA, "///": pd.NA})

    # 列が欠ける可能性に備えて、足りない列はNAで補完
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLUMNS]

    return df

def upsert_to_processed_csv(df_new: pd.DataFrame, output_csv: str) -> None:
    """
    既存CSVがあれば読み込み、追記し、日付+時刻で重複排除して保存
    """
    ensure_parent_dir(output_csv)

    if os.path.exists(output_csv):
        df_old = pd.read_csv(output_csv, encoding="utf-8-sig")
        # 既存側にも日付・時刻がある前提（あなたのStreamlitがそうなってる）
        df_old = normalize_df(df_old)
        df_new = normalize_df(df_new)
        df_old = harmonize_schema(df_old)
        df_new = harmonize_schema(df_new)

        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = normalize_df(df_new)

    # 「日付+時刻」で最新行を採用
    df_all = (
        df_all.drop_duplicates(subset=["日付", "時刻"], keep="last")
              .sort_values(["日付", "時刻"])
              .reset_index(drop=True)
    )

    df_all.to_csv(output_csv, index=False, encoding="utf-8-sig")

from datetime import date, timedelta

def get_latest_date_from_csv(csv_path: str):
    if not os.path.exists(csv_path):
        return None
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty or "日付" not in df.columns:
        return None
    # normalize_df は日付を YYYY-MM-DD 文字列に揃える
    df = normalize_df(df)
    latest_date_str = df["日付"].dropna().max()
    if pd.isna(latest_date_str):
        return None
    return date.fromisoformat(str(latest_date_str))

def update_incremental(output_csv: str = OUTPUT_CSV, max_days: int = 220) -> dict:
    """
    既存CSVの最終日の翌日〜今日までを取得してupsert
    - 放置されていても max_days ぶんまで巻き戻して復旧可能
    """
    latest_date = get_latest_date_from_csv(output_csv)
    today = date.today()

    if latest_date is None:
        # 初回は「直近max_days日」にしておくと半年放置でも死なない
        start = today - timedelta(days=max_days)
    else:
        start = latest_date + timedelta(days=1)
        # 念のため取りすぎ防止
        if (today - start).days > max_days:
            start = today - timedelta(days=max_days)

    if start > today:
        return {"message": "Already up to date", "latest_date": str(latest_date), "today": str(today)}

    dfs = []
    cur = start
    while cur <= today:
        y, m, d = cur.year, cur.month, cur.day
        try:
            df_day = scrape_one_day_http(y, m, d)
            if not df_day.empty:
                df_day.insert(0, "日付", f"{y}-{m:02d}-{d:02d}")
                dfs.append(df_day)
        except Exception:
            pass
        time.sleep(0.8)
        cur += timedelta(days=1)

    if dfs:
        df_new = pd.concat(dfs, ignore_index=True)
        upsert_to_processed_csv(df_new, output_csv)

    latest_after = get_latest_date_from_csv(output_csv)
    return {
        "start": str(start),
        "end": str(today),
        "fetched_days": len(dfs),
        "latest_after": str(latest_after) if latest_after else None,
    }

def main() -> None:
    days_in_month = calendar.monthrange(YEAR, MONTH)[1]
    dfs = []

    print(f"\n📅 {YEAR}年{MONTH}月のデータ取得開始…（{days_in_month}日分）")

    for day in range(1, days_in_month + 1):
        print(f"取得中: {YEAR}年{MONTH}月{day}日")
        try:
            df_day = scrape_one_day_http(YEAR, MONTH, day)
            if df_day.empty:
                print(f"⚠️ {day}日：取得0行（後で取り直し推奨）")
                continue

            df_day.insert(0, "日付", f"{YEAR}-{MONTH:02d}-{day:02d}")
            dfs.append(df_day)

        except Exception as e:
            print(f"🚨 エラー（{day}日）: {e}")
            continue

        time.sleep(0.8)  # アクセス控えめに

    if not dfs:
        print("取得データがありませんでした")
        return

    df_new = pd.concat(dfs, ignore_index=True)
    upsert_to_processed_csv(df_new, OUTPUT_CSV)
    print(f"✅ 保存完了: {OUTPUT_CSV}")

if __name__ == "__main__":
    print(update_incremental())
