import sqlite3
import pandas as pd

# DB接続
conn = sqlite3.connect('weather_data.db')

# データをDataFrameで読み込み（最初の10行だけ）
df = pd.read_sql_query("SELECT * FROM weather LIMIT 10", conn)

# 表示
print(df)

conn.close()
