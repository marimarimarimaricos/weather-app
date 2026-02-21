# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import calendar
from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =========================
# ★設定（ここだけ触ればOK）
# =========================
CHROMEDRIVER_PATH = "/Users/matsumotomariko/Desktop/プログラミング学習/python学習/chromedriver-mac-arm64/chromedriver"

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


def to_int_hour(x) -> pd.Int64Dtype:
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


def scrape_one_day(driver: webdriver.Chrome, year: int, month: int, day: int) -> list[list[str]]:
    """
    1日分をスクレイピングして、[日付, 時刻, ...] の行リストを返す
    """
    url = f"{BASE_URL}?prec_no={PREC_NO}&block_no={BLOCK_NO}&year={year}&month={month}&day={day}&view=p1"
    driver.get(url)

    daily_rows: list[list[str]] = []
    table = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "tablefix1"))
    )
    rows = table.find_elements(By.TAG_NAME, "tr")

    for row in rows[2:]:  # 先頭2行はヘッダ想定
        cells = row.find_elements(By.TAG_NAME, "td")
        row_data = [cell.text.strip() for cell in cells]

        if len(row_data) == len(COLUMNS):
            daily_rows.append([f"{year}-{month:02d}-{day:02d}"] + row_data)

    return daily_rows


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


def main() -> None:
    days_in_month = calendar.monthrange(YEAR, MONTH)[1]
    all_data: list[list[str]] = []

    print(f"\n📅 {YEAR}年{MONTH}月のデータ取得開始…（{days_in_month}日分）")
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.options import Options

    options = Options()
    # options.add_argument("--headless=new")  # 画面出さないならON
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        for day in range(1, days_in_month + 1):
            print(f"取得中: {YEAR}年{MONTH}月{day}日")
            try:
                daily_rows = scrape_one_day(driver, YEAR, MONTH, day)

                if len(daily_rows) == 0:
                    print(f"⚠️ {day}日：取得0行（後で取り直し推奨）")
                    continue

                # 24行じゃない日もありえる（欠測/観測休止/ページ仕様等）
                if len(daily_rows) != 24:
                    print(f"⚠️ {day}日：有効行数 {len(daily_rows)}/24（取得分は保存）")

                all_data.extend(daily_rows)

            except Exception as e:
                print(f"🚨 エラー（{day}日）: {e}")
                # 失敗日は「空行で埋めない」：キー（日付+時刻）が壊れるので後で取り直せる方が安全
                continue

            time.sleep(0.8)  # ちょい優しめ

    finally:
        driver.quit()

    if not all_data:
        print("⚠️ データが取れなかったので終了。")
        return

    df_new = pd.DataFrame(all_data, columns=["日付"] + COLUMNS)

    # processed へ upsert
    upsert_to_processed_csv(df_new, OUTPUT_CSV)

    print(f"\n✅ 保存完了: {OUTPUT_CSV}")
    print("（同じ日を取り直しても、日付+時刻キーで上書きされます）")


if __name__ == "__main__":
    main()
