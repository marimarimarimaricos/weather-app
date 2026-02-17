import sqlite3
import csv

# 欠損値処理
def parse_value(value):
    return None if value.strip() == '///' or value.strip() == '' else value.strip()

# DB接続
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# テーブル作成（すでにある場合はスキップ）
cursor.execute('''
CREATE TABLE IF NOT EXISTS weather (
    date TEXT,
    hour INTEGER,
    precipitation_mm REAL,
    temperature_c REAL,
    dew_point_c REAL,
    vapor_pressure_hpa REAL,
    humidity_percent REAL,
    wind_speed_ms REAL,
    wind_direction TEXT,
    sunshine_hours REAL,
    snowfall_cm REAL,
    snow_depth_cm REAL,
    PRIMARY KEY (date, hour)
)
''')

# 1〜12月のCSVをループで読み込み
for month in range(1, 13):
    csv_file = f"amedas-test - {month}月.csv"
    print(f"📥 {csv_file} を取り込み中…")

    with open(csv_file, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            if row['時刻'].strip() == '':
                continue # 時刻が空欄の行はスキップ

            data = (
                row['日付'],
                int(row['時刻']),
                parse_value(row['降水量(mm)']),
                parse_value(row['気温(℃)']),
                parse_value(row['露点温度(℃)']),
                parse_value(row['蒸気圧(hPa)']),
                parse_value(row['湿度(％)']),
                parse_value(row['平均風速(m/s)']),
                parse_value(row['風向']),
                parse_value(row['日照時間(h)']),
                parse_value(row['降雪(cm)']),
                parse_value(row['積雪(cm)'])
            )
            cursor.execute('''
                INSERT OR REPLACE INTO weather VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data)

            # 保存して接続を閉じる
conn.commit()
conn.close()