import streamlit as st
import pandas as pd
from pathlib import Path
from streamlit_echarts import st_echarts

from daily_average import get_daily_averages

# ==============================
# 設定
# ==============================
BASE_TEMP_DEFAULT = 10  # GDDの基準温度（℃）
GDD_START_MMDD = "04-01"  # 4/1固定

st.set_page_config(page_title="🍇 気象データビューア", layout="centered")
# タイトルはヘッダー側で表示するので、Streamlit標準の巨大タイトルは出さない
# st.title("🍇 気象データビューア")

# ==============================
# データ読み込み（CSV運用に寄せる）
# ==============================
# 推奨：data/processed/amedas_nirasaki_hourly.csv を“正本”として1つだけ読む
DATA_CSV = Path("data/processed/amedas_nirasaki_hourly.csv")

@st.cache_data(show_spinner=False)
def load_hourly_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        st.error(f"CSVが見つかりません: {path}")
        st.stop()

    # ユーザーのCSVヘッダー（単位つき）を吸収して、アプリ内部の列名に統一
    df0 = pd.read_csv(path)

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
    df0 = df0.rename(columns=rename_map)

    # 必須列チェック
    must = {"日付", "時刻"}
    if not must.issubset(set(df0.columns)):
        st.error("CSVの列名が想定と違うよ（最低限 '日付' と '時刻' が必要）")
        st.write(list(df0.columns))
        st.stop()

    # 日付
    df0["日付_dt"] = pd.to_datetime(df0["日付"], errors="coerce")

    return df0

# 読み込み
df = load_hourly_csv(DATA_CSV)

# 数値列が文字列（object）になっていると、日別集計で mean が失敗するので数値化
numeric_cols = [
    "時刻",
    "降水量",
    "気温",
    "露点温度",
    "蒸気圧",
    "湿度",
    "風速",
    "日照時間",
    "降雪",
    "積雪",
]
for c in numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# 最新年月
latest_date = df["日付_dt"].max()
latest_year = int(latest_date.year)
latest_month = int(latest_date.month)

# 年リスト（UIで使用）
year_list = sorted(df["日付_dt"].dt.year.dropna().unique())
if not year_list:
    st.error("CSVにデータがありません")
    st.stop()

default_year_index = year_list.index(latest_year) if latest_year in year_list else len(year_list) - 1

# ==============================
# 便利関数
# ==============================

def last_day_of_month(year: int, month: int) -> pd.Timestamp:
    return (pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)).normalize()


def add_gdd_columns(daily_df: pd.DataFrame, base_temp: float = BASE_TEMP_DEFAULT) -> pd.DataFrame:
    """daily_df（1日1行・気温（平均）あり）に GDD と 累積GDD を追加する。

    - 4/1以降のみを対象（固定）
    - 累積は年ごとにリセット
    """
    out = daily_df.copy()
    out["日付_dt"] = pd.to_datetime(out["日付"], errors="coerce")
    out = out.dropna(subset=["日付_dt"]).sort_values("日付_dt")

    # 4/1以降に絞る（年をまたぐ場合でも、各年で4/1以降のみを対象にする）
    mmdd = out["日付_dt"].dt.strftime("%m-%d")
    out = out[mmdd >= GDD_START_MMDD].copy()

    # 日GDD
    out["GDD"] = (out["気温（平均）"] - float(base_temp)).clip(lower=0)

    # 年ごとに累積
    out["累積GDD"] = out.groupby(out["日付_dt"].dt.year)["GDD"].cumsum()

    return out


def filter_by_time_window(src: pd.DataFrame, year: int, month: int | None, start_hour: int, end_hour: int) -> pd.DataFrame:
    """年（＋月オプション）と時間帯で絞り込み。"""
    base = src[src["日付_dt"].dt.year == year]
    if month is not None:
        base = base[base["日付_dt"].dt.month == month]

    if start_hour <= end_hour:
        base = base[base["時刻"].between(start_hour, end_hour)].copy()
    else:
        base = base[((base["時刻"] >= start_hour) | (base["時刻"] <= end_hour))].copy()

    base["日付"] = base["日付_dt"].dt.strftime("%Y-%m-%d")
    return base


# ==============================
# UI（理想アプリ寄せ）
# ==============================

# --- 見た目（ヘッダー/カード/余白） ---
CSS = """
<style>
/* 画面全体を“白いアプリ”寄せ（ユーザーがダークテーマでも強制的に明るく見せる） */
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
  background: #f6f7fb !important;
  color: #111 !important;
}

.block-container {padding-top: 0.6rem; padding-bottom: 2rem; max-width: 820px;}

.app-header{
  position: sticky; top: 0; z-index: 999;
  background: rgba(255,255,255,0.98);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid rgba(0,0,0,0.08);
  padding: 0.55rem 0.25rem;
  margin-bottom: 0.6rem;
}
.header-inner{display:flex; align-items:center; gap:0.6rem;}
.hamburger{
  font-size: 1.2rem; line-height: 1; color:#111;
  padding: 0.15rem 0.5rem; border-radius: 10px;
  border: 1px solid rgba(0,0,0,0.12);
}
.title{font-weight: 800; font-size: 1.05rem; color:#111;}

.section-title{font-size: 1.9rem; font-weight: 900; margin: 0.1rem 0 0.4rem; color:#111;}

.card{background:#fff; border:1px solid rgba(0,0,0,0.08); border-radius: 18px; padding: 14px 14px 10px; box-shadow: 0 1px 2px rgba(0,0,0,0.06); margin: 12px 0;}
.card-title{font-weight: 900; margin-bottom: 8px; color:#111;}
.card-note{color: rgba(0,0,0,0.60); font-size: 0.85rem; margin-top: 6px;}

.mini{background:#fff; border:1px solid rgba(0,0,0,0.08); border-radius: 16px; padding: 12px; box-shadow: 0 1px 2px rgba(0,0,0,0.06);} 
.mini-label{color: rgba(0,0,0,0.60); font-size: 0.85rem; font-weight: 800;}
.mini-value{font-size: 1.7rem; font-weight: 900; margin-top: 4px; color:#111;}

/* サイドバー/メニュー等を隠す */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ヘッダー
st.markdown(
    """
    <div class="app-header">
      <div class="header-inner">
        <div class="hamburger">☰</div>
        <div class="title">🍇韮崎アメダスビューア</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="section-title">統計</div>', unsafe_allow_html=True)

gdd_year = int(st.selectbox("年", year_list, index=default_year_index, key="year_select"))

# ▼追加：前年表示トグル（年選択の直下）
show_prev = st.toggle("前年データを重ねて表示", value=False, key="toggle_prev")
prev_year = gdd_year - 1

# 選んだ年の「その年に存在する最新月」まで表示（2025が途中でもOK）
mmax = df.loc[df["日付_dt"].dt.year == gdd_year, "日付_dt"].dt.month.max()
latest_month_for_year = int(mmax) if pd.notna(mmax) else 12

gdd_upto_month = latest_month_for_year
gdd_start_hour, gdd_end_hour = 0, 23
base_temp = float(BASE_TEMP_DEFAULT)


# 年のデータを取得（時間帯はここで適用）
year_df_all = filter_by_time_window(df, int(gdd_year), None, int(gdd_start_hour), int(gdd_end_hour))
end_dt = last_day_of_month(int(gdd_year), int(gdd_upto_month))
year_df_all = year_df_all[year_df_all["日付_dt"] <= end_dt].copy()

# 気象データ（日別集計）は「年全体」で作る
year_df_all["日付"] = year_df_all["日付_dt"].dt.strftime("%Y-%m-%d")
daily_all = get_daily_averages(year_df_all)

# 日照時間・降水量（日計）をmerge（ここも年全体でOK）
_sum_targets = {}
if "日照時間" in year_df_all.columns:
    _sum_targets["日照時間"] = "sum"
if "降水量" in year_df_all.columns:
    _sum_targets["降水量"] = "sum"

if _sum_targets:
    daily_sum = year_df_all.groupby("日付").agg(_sum_targets).reset_index()
    rename_map = {}
    if "日照時間" in daily_sum.columns:
        rename_map["日照時間"] = "日照時間（日計）"
    if "降水量" in daily_sum.columns:
        rename_map["降水量"] = "降水量（日計）"
    if rename_map:
        daily_sum = daily_sum.rename(columns=rename_map)
        daily_all = daily_all.merge(daily_sum[["日付"] + list(rename_map.values())], on="日付", how="left")

# --- 前年比トグルがオンのとき ---

prev_daily_all = None
prev_end_dt = None

if show_prev and (prev_year in year_list):
    prev_end_dt = pd.Timestamp(prev_year, end_dt.month, end_dt.day)

    prev_year_df_all = filter_by_time_window(
        df, prev_year, None, int(gdd_start_hour), int(gdd_end_hour)
    )
    prev_year_df_all = prev_year_df_all[prev_year_df_all["日付_dt"] <= prev_end_dt].copy()
    prev_year_df_all["日付"] = prev_year_df_all["日付_dt"].dt.strftime("%Y-%m-%d")

    prev_daily_all = get_daily_averages(prev_year_df_all)

    _sum_targets_prev = {}
    if "日照時間" in prev_year_df_all.columns:
        _sum_targets_prev["日照時間"] = "sum"
    if "降水量" in prev_year_df_all.columns:
        _sum_targets_prev["降水量"] = "sum"

    if _sum_targets_prev:
        prev_daily_sum = prev_year_df_all.groupby("日付").agg(_sum_targets_prev).reset_index()

        prev_rename_map = {}
        if "日照時間" in prev_daily_sum.columns:
            prev_rename_map["日照時間"] = "日照時間（日計）"
        if "降水量" in prev_daily_sum.columns:
            prev_rename_map["降水量"] = "降水量（日計）"

        if prev_rename_map:
            prev_daily_sum = prev_daily_sum.rename(columns=prev_rename_map)

            prev_daily_all = prev_daily_all.merge(
                prev_daily_sum[["日付"] + list(prev_rename_map.values())],
                on="日付",
                how="left"
            )

# --- GDDは4/1以降だけ ---
start_dt = pd.Timestamp(year=int(gdd_year), month=4, day=1)
year_df_gdd = year_df_all[year_df_all["日付_dt"] >= start_dt].copy()

show_gdd = not year_df_gdd.empty

# X軸用（12/1形式に統一）

if show_gdd:
    # GDD（4/1固定）
    gdd_df = add_gdd_columns(daily_all, base_temp=base_temp)

# ECharts（streamlit-echarts）
USE_ECHARTS = True

def _echarts_line(dates, series: dict, height: int = 260):
    if not USE_ECHARTS:
        return None

    x = [pd.to_datetime(d).strftime("%m/%d").lstrip("0").replace("/0", "/") for d in dates]

    echarts_series = []
    for name, y in series.items():
        echarts_series.append({
            "name": name,
            "type": "line",
            "showSymbol": False,
            "data": [None if pd.isna(v) else float(v) for v in y],
        })

    option = {
        "tooltip": {"trigger": "axis"},
        "legend": {"data": list(series.keys())},
        "grid": {"left": 40, "right": 15, "top": 20, "bottom": 40},
        "xAxis": {"type": "category", "data": x},
        "yAxis": {"type": "value", "scale": False},
        "series": echarts_series,
    }
    return option


def _echarts_bar(dates, y, height: int = 260):
    if not USE_ECHARTS:
        return None

    x = [pd.to_datetime(d).strftime("%m/%d").lstrip("0").replace("/0", "/") for d in dates]
    option = {
        "tooltip": {"trigger": "axis"},
        "grid": {"left": 40, "right": 15, "top": 20, "bottom": 40},
        "xAxis": {"type": "category", "data": x},
        "yAxis": {"type": "value", "min": 0},
        "series": [{
            "type": "bar",
            "data": [None if pd.isna(v) else float(v) for v in y],
        }],
    }
    return option

def _echarts_bar_multi(dates, series: dict, height: int = 260):
    if not USE_ECHARTS:
        return None

    x = [pd.to_datetime(d).strftime("%m/%d").lstrip("0").replace("/0", "/") for d in dates]

    echarts_series = []
    for name, y in series.items():
        echarts_series.append({
            "name": name,
            "type": "bar",
            "data": [None if pd.isna(v) else float(v) for v in y],
        })

    option = {
        "tooltip": {"trigger": "axis"},
        "legend": {"data": list(series.keys())},
        "grid": {"left": 40, "right": 15, "top": 20, "bottom": 40},
        "xAxis": {"type": "category", "data": x},
        "yAxis": {"type": "value", "min": 0},
        "series": echarts_series,
    }
    return option

def _align_prev_to_x(x_all, prev_df: pd.DataFrame, col: str):
    """今年x_allに対して、前年prev_dfのcolをMM-DDで揃えて返す（欠損はNone）"""
    if prev_df is None or prev_df.empty or (col not in prev_df.columns):
        return None

    x_keys = pd.to_datetime(x_all).dt.strftime("%m-%d")
    prev_keys = pd.to_datetime(prev_df["日付"], errors="coerce").dt.strftime("%m-%d")

    m = dict(zip(prev_keys, prev_df[col]))
    return [m.get(k) for k in x_keys]

x_all = pd.to_datetime(daily_all["日付"])

# 気温
st.markdown('<div class="card"><div class="card-title">気温の推移（最高・最低）</div>', unsafe_allow_html=True)
temp_series = {
    f"最高気温（{gdd_year}）": daily_all["気温（最高）"],
    f"最低気温（{gdd_year}）": daily_all["気温（最低）"],
}

if show_prev and (prev_daily_all is not None):
    y_prev_max = _align_prev_to_x(x_all, prev_daily_all, "気温（最高）")
    y_prev_min = _align_prev_to_x(x_all, prev_daily_all, "気温（最低）")
    if y_prev_max is not None:
        temp_series[f"最高気温（{prev_year}）"] = y_prev_max
    if y_prev_min is not None:
        temp_series[f"最低気温（{prev_year}）"] = y_prev_min

opt_temp = _echarts_line(x_all, temp_series)
if USE_ECHARTS:
    st_echarts(options=opt_temp, height="260px")
else:
    st.line_chart(daily_all.set_index(x_all)[["気温（最高）", "気温（最低）"]])
st.markdown('</div>', unsafe_allow_html=True)

# 日照
st.markdown('<div class="card"><div class="card-title">日照時間（h/日）</div>', unsafe_allow_html=True)
sun_col = "日照時間（日計）" if "日照時間（日計）" in daily_all.columns else "日照時間"

sun_series = {f"日照（{gdd_year}）": daily_all[sun_col]}
if show_prev and (prev_daily_all is not None):
    y_prev_sun = _align_prev_to_x(x_all, prev_daily_all, sun_col)
    if y_prev_sun is not None:
        sun_series[f"日照（{prev_year}）"] = y_prev_sun

opt_sun = _echarts_bar_multi(x_all, sun_series)

if USE_ECHARTS:
    st_echarts(options=opt_sun, height="260px")
else:
    st.bar_chart(daily_all.set_index(x_all)["日照時間"])
st.markdown('</div>', unsafe_allow_html=True)

# 降水量
st.markdown('<div class="card"><div class="card-title">降水量（mm/日）</div>', unsafe_allow_html=True)
# 降水量（mm/日）も日合計で表示
rain_col = "降水量（日計）" if "降水量（日計）" in daily_all.columns else "降水量"

rain_series = {f"降水（{gdd_year}）": daily_all[rain_col]}
if show_prev and (prev_daily_all is not None):
    y_prev_rain = _align_prev_to_x(x_all, prev_daily_all, rain_col)
    if y_prev_rain is not None:
        rain_series[f"降水（{prev_year}）"] = y_prev_rain

opt_rain = _echarts_bar_multi(x_all, rain_series)

if USE_ECHARTS:
    st_echarts(options=opt_rain, height="260px")
else:
    st.bar_chart(daily_all.set_index(x_all)[rain_col])
st.markdown('</div>', unsafe_allow_html=True)

# GDD
st.markdown('<div class="card"><div class="card-title">GDD（有効積算温度）- 基準温度{:.1f}℃、4月1日〜</div>'.format(float(base_temp)), unsafe_allow_html=True)

if show_gdd:
    gdd_df = add_gdd_columns(daily_all, base_temp=base_temp)
    x_gdd = pd.to_datetime(gdd_df["日付"])

    gdd_series = {f"累積GDD（{gdd_year}）": gdd_df["累積GDD"]}

    if show_prev and (prev_daily_all is not None):
        prev_gdd_df = add_gdd_columns(prev_daily_all, base_temp=base_temp)
        # x_gdd にMM-DDで揃える
        x_keys = pd.to_datetime(x_gdd).dt.strftime("%m-%d")
        prev_keys = pd.to_datetime(prev_gdd_df["日付"]).dt.strftime("%m-%d")
        prev_map = dict(zip(prev_keys, prev_gdd_df["累積GDD"]))
        prev_aligned = [prev_map.get(k) for k in x_keys]
        gdd_series[f"累積GDD（{prev_year}）"] = prev_aligned

    opt_gdd = _echarts_line(x_gdd, gdd_series)
    if USE_ECHARTS:
        st_echarts(options=opt_gdd, height="260px")
    else:
        st.line_chart(gdd_df.set_index(x_gdd)["累積GDD"])
else:
    st.caption("GDDは4/1以降のデータが入ると自動で表示されます。")

st.markdown('<div class="card-note">※ GDDは 04-01 以降を対象に、日GDD = max(0, 日平均気温 − Tb) を年ごとに累積しています。</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)



# 追加ミニカード：最高/最低（理想アプリ寄せ）
if not daily_all.empty and ("気温（最高）" in daily_all.columns) and ("気温（最低）" in daily_all.columns):
    max_temp = float(pd.to_numeric(daily_all["気温（最高）"], errors="coerce").max())
    min_temp = float(pd.to_numeric(daily_all["気温（最低）"], errors="coerce").min())
    t1, t2 = st.columns(2)
    with t1:
        st.markdown(
            f"""
            <div class='mini'>
              <div class='mini-label'>最高気温（最大）</div>
              <div class='mini-value'>{max_temp:.1f} ℃</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with t2:
        st.markdown(
            f"""
            <div class='mini'>
              <div class='mini-label'>最低気温（最小）</div>
              <div class='mini-value'>{min_temp:.1f} ℃</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ミニカード：日付/当日GDD/累積GDD
if show_gdd and (not gdd_df.empty):
    last_row = gdd_df.iloc[-1]
    m1, m2, m3 = st.columns(3)
    # 日付表示を 12/31 形式に
    _d = pd.to_datetime(last_row["日付"], errors="coerce")
    d_label = _d.strftime("%m/%d").lstrip("0").replace("/0", "/") if pd.notna(_d) else str(last_row["日付"])
    with m1:
        st.markdown(
            f"""
            <div class='mini'>
              <div class='mini-label'>日付</div>
              <div class='mini-value'>{d_label}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m2:
        st.markdown(
            f"""
            <div class='mini'>
              <div class='mini-label'>当日GDD</div>
              <div class='mini-value'>{float(last_row['GDD']):.1f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            f"""
            <div class='mini'>
              <div class='mini-label'>累積GDD</div>
              <div class='mini-value'>{float(last_row['累積GDD']):.1f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# 開発用：元データ
with st.expander("（開発用）元データを見る", expanded=False):
    ym_periods = df["日付_dt"].dt.to_period("M")
    ym_list = sorted(ym_periods.dropna().unique())
    ym_labels = [f"{p.year}年 {p.month}月" for p in ym_list]

    latest_label = f"{latest_year}年 {latest_month}月"
    default_index = ym_labels.index(latest_label) if latest_label in ym_labels else len(ym_labels) - 1

    sel_ym = st.selectbox("表示する年月を選択", ym_labels, index=default_index)
    sel_y, sel_m = map(int, sel_ym.replace("年", "").replace("月", "").split())

    hours2 = list(range(0, 24))
    rc1, rc2 = st.columns(2)
    with rc1:
        raw_start = st.selectbox("開始時刻", hours2, index=0, key="raw_start")
    with rc2:
        raw_end = st.selectbox("終了時刻", hours2, index=23, key="raw_end")

    raw_df = filter_by_time_window(df, sel_y, sel_m, raw_start, raw_end)
    st.write(f"💡 {len(raw_df)} 件")
    cols_show = [c for c in ["日付", "時刻", "気温", "降水量", "日照時間", "湿度", "風速"] if c in raw_df.columns]
    st.dataframe(raw_df[cols_show], use_container_width=True)
