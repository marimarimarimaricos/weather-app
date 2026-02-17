import streamlit as st
import pandas as pd
import sqlite3
import math

from daily_average import get_daily_averages

st.title("🍇 気象データビューア")

# DB接続
conn = sqlite3.connect('weather_data.db')
df = pd.read_sql_query("SELECT * FROM weather", conn)
conn.close()

# 📅 日付変換・日本語ラベル
df["日付_dt"] = pd.to_datetime(df["date"])

# 日本語カラム名への変換
japanese_columns = {
    "date": "日付",
    "hour": "時刻",
    "precipitation_mm": "降水量",
    "temperature_c": "気温",
    "dew_point_c": "露点温度",
    "vapor_pressure_hpa": "蒸気圧",
    "humidity_percent": "湿度",
    "wind_speed_ms": "風速",
    "wind_direction": "風向",
    "sunshine_hours": "日照時間",
    "snowfall_cm": "降雪",
    "snow_depth_cm": "積雪"
}
df = df.rename(columns=japanese_columns)

# 📆 最新年月のデフォルト
latest_date = df["日付_dt"].max()
latest_year = latest_date.year
latest_month = latest_date.month

# 🎛️ 年月セレクトボックス
年リスト = sorted(df["日付_dt"].dt.year.unique())
月リスト = sorted(df["日付_dt"].dt.month.unique())
年月リスト = sorted(set(f"{y}年 {m}月" for y in 年リスト for m in 月リスト))
選択年月 = st.selectbox("表示する年月を選択", 年月リスト, index=len(年月リスト) - 1)
選択年, 選択月 = map(int, 選択年月.replace("年", "").replace("月", "").split())

# 🕓 時間帯（プルダウン）
時間リスト = list(range(0, 24))
col1, col2 = st.columns(2)
with col1:
    開始時刻 = st.selectbox("開始時刻", 時間リスト, index=0)
with col2:
    終了時刻 = st.selectbox("終了時刻", 時間リスト, index=23)

# ✅ 日別平均表示
日別表示 = st.checkbox("🔄 1日ごとの平均表示に切り替え")

# 📋 表示項目選択
元項目リスト = ["気温", "降水量", "湿度", "日照時間", "風速", "露点温度", "蒸気圧", "降雪", "積雪"]

st.markdown("#### 表示項目を選択")

col_button1, col_button2 = st.columns([1, 1])
全選択 = col_button1.button("✅ すべて選択")
全解除 = col_button2.button("🚫 すべて解除")

if "表示項目" not in st.session_state:
    st.session_state["表示項目"] = 元項目リスト.copy()

if 全選択:
    st.session_state["表示項目"] = 元項目リスト.copy()
elif 全解除:
    st.session_state["表示項目"] = []

cols = st.columns(3)
選択項目 = []
for i, 項目 in enumerate(元項目リスト):
    with cols[i % 3]:
        checked = 項目 in st.session_state["表示項目"]
        if st.checkbox(項目, value=checked, key=f"chk_{項目}"):
            選択項目.append(項目)

# 🔍 データ絞り込み
if 開始時刻 <= 終了時刻:
    filtered_df = df[
        (df["日付_dt"].dt.year == 選択年) &
        (df["日付_dt"].dt.month == 選択月) &
        (df["時刻"].between(開始時刻, 終了時刻))
    ].copy()
else:
    filtered_df = df[
        (df["日付_dt"].dt.year == 選択年) &
        (df["日付_dt"].dt.month == 選択月) &
        ((df["時刻"] >= 開始時刻) | (df["時刻"] <= 終了時刻))
    ].copy()

filtered_df["日付"] = filtered_df["日付_dt"].dt.strftime("%Y-%m-%d")

# 📈 表示
if 日別表示:
    st.write("📈 1日ごとの平均・最高・最低気温など")
    daily_df = get_daily_averages(filtered_df)

    # 選択された元項目に基づいてカラムを抽出
    表示カラム = ["日付"] + [col for col in daily_df.columns if any(col.startswith(f"{項目}（") for 項目 in 選択項目)]
    st.dataframe(daily_df[表示カラム])
else:
    表示カラム = ["日付", "時刻"] + [col for col in 選択項目 if col in df.columns]
    st.write(f"💡 {len(filtered_df)} 件のデータが見つかりました")
    st.dataframe(filtered_df[表示カラム])