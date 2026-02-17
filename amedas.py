# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import time
from datetime import datetime, timedelta
import calendar

# ★設定
CHROMEDRIVER_PATH = "/Users/matsumotomariko/Desktop/python学習/chromedriver-mac-arm64/chromedriver"
KEY_PATH = "/Users/matsumotomariko/Desktop/python学習/sheets-key.json"
SPREADSHEET_ID = "1ZnY0hCu1L1nUKF87DZNKqrq6WYlu3FtMtNOdgYMx1T8"

YEAR = 2024
PREC_NO = 49
BLOCK_NO = 1024
BASE_URL = "https://www.data.jma.go.jp/stats/etrn/view/hourly_a1.php"

columns = [
    "時刻", "降水量(mm)", "気温(℃)", "露点温度(℃)", "蒸気圧(hPa)", "湿度(％)",
    "平均風速(m/s)", "風向", "日照時間(h)", "降雪(cm)", "積雪(cm)"
]

# 認証
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_file(KEY_PATH, scopes=scopes)
gc = gspread.authorize(credentials)
sh = gc.open_by_key(SPREADSHEET_ID)

# Chromeドライバ準備
service = Service(executable_path=CHROMEDRIVER_PATH)

for month in range(1, 13):
    days_in_month = (datetime(YEAR, month % 12 + 1, 1) - timedelta(days=1)).day
    all_data = []
    print(f"\n📅 {month}月のデータ取得開始…")

    for day in range(1, days_in_month + 1):
        url = f"{BASE_URL}?prec_no={PREC_NO}&block_no={BLOCK_NO}&year={YEAR}&month={month}&day={day}&view=p1"
        print(f"取得中: {YEAR}年{month}月{day}日")

        driver = webdriver.Chrome(service=service)
        driver.get(url)
        daily_rows = []

        try:
            table = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "tablefix1"))
            )
            rows = table.find_elements(By.TAG_NAME, "tr")

            for row in rows[2:]:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = [cell.text.strip() for cell in cells]

                if len(row_data) == len(columns):
                    daily_rows.append([f"{YEAR}-{month:02d}-{day:02d}"] + row_data)

            if len(daily_rows) == 24:
                all_data.extend(daily_rows)
            else:
                print(f"⚠️ スキップ: {day}日（有効行数: {len(daily_rows)}/24）")
                all_data.append([f"{YEAR}-{month:02d}-{day:02d}"] + [""] * len(columns))

        except Exception as e:
            print(f"🚨 エラー（{day}日）: {e}")
            all_data.append([f"{YEAR}-{month:02d}-{day:02d}"] + [""] * len(columns))

        finally:
            driver.quit()
            time.sleep(1.5)

    if not all_data:
        print(f"⚠️ {month}月はデータなし。スキップ。")
        continue

    df = pd.DataFrame(all_data, columns=["日付"] + columns)

    sheet_title = f"{month}月"
    try:
        worksheet = sh.worksheet(sheet_title)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=sheet_title, rows="1000", cols="20")

    set_with_dataframe(worksheet, df)
    print(f"✅ {sheet_title}シートに出力完了！")
