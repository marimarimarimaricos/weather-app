import streamlit as st
import pandas as pd
from pathlib import Path

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
    st.error("DBにデータがありません")
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
        <div class="title">🍇 ワイン葡萄栽培日記</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="section-title">統計</div>', unsafe_allow_html=True)

# 年のみ（区画は一旦削除）
gdd_year = st.selectbox("年", year_list, index=default_year_index)

with st.expander("詳細設定", expanded=False):
    month_list = list(range(1, 13))
    default_m = latest_month if int(gdd_year) == latest_year else 12
    gdd_upto_month = st.selectbox("どこまで表示する？（月末まで）", month_list, index=month_list.index(default_m))

    hours = list(range(0, 24))
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        gdd_start_hour = st.selectbox("開始時刻", hours, index=0, key="gdd_start")
    with c2:
        gdd_end_hour = st.selectbox("終了時刻", hours, index=23, key="gdd_end")
    with c3:
        base_temp = st.number_input("基準温度Tb（℃）", min_value=-5.0, max_value=20.0, value=float(BASE_TEMP_DEFAULT), step=0.5)

    st.markdown("---")
    st.markdown("**GDDステージ（右軸表示用）**  ※数値が空なら表示しません")
    STAGE_ORDER = [
        "収穫",
        "成熟期",
        "ヴェレゾン",
        "果粒肥大",
        "結実",
        "満開",
        "開花開始",
        "展葉期",
        "萌芽",
    ]
    stage_inputs = {}
    cols_stage = st.columns(3)
    for i, name in enumerate(STAGE_ORDER):
        with cols_stage[i % 3]:
            s = st.text_input(f"{name}（GDD）", value="", key=f"stage_{name}")
            s = s.strip()
            if s:
                try:
                    stage_inputs[name] = float(s)
                except ValueError:
                    st.caption("数字で入力してね")
    # stage_inputs: dict[str, float]


# expanderを開かない場合のデフォルト
if "gdd_upto_month" not in locals():
    gdd_upto_month = latest_month if int(gdd_year) == latest_year else 12
if "gdd_start_hour" not in locals():
    gdd_start_hour, gdd_end_hour = 0, 23
if "base_temp" not in locals():
    base_temp = float(BASE_TEMP_DEFAULT)

# 年の4/1〜選択月末までのデータを取得
year_df = filter_by_time_window(df, int(gdd_year), None, int(gdd_start_hour), int(gdd_end_hour))
start_dt = pd.Timestamp(year=int(gdd_year), month=4, day=1)
end_dt = last_day_of_month(int(gdd_year), int(gdd_upto_month))
year_df = year_df[(year_df["日付_dt"] >= start_dt) & (year_df["日付_dt"] <= end_dt)].copy()

if year_df.empty:
    st.warning("この期間のデータがありません（DBに4/1以降のデータが入っているか確認してね）")
    st.stop()

# 日別集計のために日付文字列を用意
year_df["日付"] = year_df["日付_dt"].dt.strftime("%Y-%m-%d")
daily_all = get_daily_averages(year_df)

# 日照時間・降水量は「日合計」の方がアプリ表示として自然なので、元データから日合計を作って差し替える
_sum_targets = {}
if "日照時間" in year_df.columns:
    _sum_targets["日照時間"] = "sum"
if "降水量" in year_df.columns:
    _sum_targets["降水量"] = "sum"
if _sum_targets:
    daily_sum = year_df.groupby("日付").agg(_sum_targets).reset_index()
    # 日照時間
    if "日照時間" in daily_sum.columns:
        daily_all = daily_all.merge(daily_sum[["日付", "日照時間"]], on="日付", how="left", suffixes=("", "_sum"))
        daily_all["日照時間（日計）"] = daily_all["日照時間"]
    # 降水量
    if "降水量" in daily_sum.columns:
        # merge済みの場合に備え再度mergeはしない（列がないならmerge）
        if "降水量" not in daily_all.columns or daily_all["降水量"].isna().all():
            daily_all = daily_all.merge(daily_sum[["日付", "降水量"]], on="日付", how="left")
        daily_all["降水量（日計）"] = daily_all["降水量"]

# X軸用（12/1形式に統一）
# x_all は上で作成済み

# GDD（4/1固定）
gdd_df = add_gdd_columns(daily_all, base_temp=base_temp)

# Plotlyでカード内グラフ（未インストール時はフォールバック）
try:
    import plotly.graph_objects as go
    USE_PLOTLY = True
except ImportError:
    USE_PLOTLY = False

if USE_PLOTLY:
    PLOTLY_CONFIG = {
        "displayModeBar": True,
        "displaylogo": False,
        "scrollZoom": False,
        "doubleClick": "reset"
    }

def _fig_line(dates, series: dict, height: int = 260):
    if not USE_PLOTLY:
        return None
    fig = go.Figure()
    for name, y in series.items():
        fig.add_trace(go.Scatter(x=dates, y=y, mode="lines", name=name))
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="top", y=-0.22, x=0),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        font=dict(color="#111"),
        dragmode="pan",
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(0,0,0,0.08)",
        tickformat="%m/%d",
        tickfont=dict(color="#444", size=12),
        title_font=dict(color="#444"),
        fixedrange=True,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(0,0,0,0.08)",
        tickfont=dict(color="#444", size=12),
        title_font=dict(color="#444")
    )
    fig.update_yaxes(fixedrange=True)
    return fig


def _fig_bar(dates, y, height: int = 260, y_dtick: float | None = None, y_range: tuple[float, float] | None = None):
    if not USE_PLOTLY:
        return None
    fig = go.Figure()
    fig.add_trace(go.Bar(x=dates, y=y, marker_color="#6ea8fe"))
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=10, b=10),
        showlegend=False,
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        font=dict(color="#111"),
        dragmode="pan",
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(0,0,0,0.08)",
        tickformat="%m/%d",
        tickfont=dict(color="#444", size=12),
        title_font=dict(color="#444"),
        fixedrange=True,
    )
    y_kwargs = dict(showgrid=True, gridcolor="rgba(0,0,0,0.08)", fixedrange=True)

    if y_dtick is not None:
        y_kwargs["dtick"] = y_dtick
        y_kwargs["tick0"] = 0
    if y_range is not None:
        y_kwargs["range"] = list(y_range)
    y_kwargs.update({
        "tickfont": dict(color="#444", size=12),
        "title_font": dict(color="#444")
    })
    fig.update_yaxes(**y_kwargs)
    return fig


x_all = pd.to_datetime(daily_all["日付"])

# 気温
st.markdown('<div class="card"><div class="card-title">気温の推移（最高・最低）</div>', unsafe_allow_html=True)
fig_temp = _fig_line(
    x_all,
    {
        "最高気温": daily_all["気温（最高）"],
        "最低気温": daily_all["気温（最低）"],
    },
)
if USE_PLOTLY:
    st.plotly_chart(fig_temp, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.line_chart(daily_all.set_index(x_all)[["気温（最高）", "気温（最低）"]])
st.markdown('</div>', unsafe_allow_html=True)

# 日照
st.markdown('<div class="card"><div class="card-title">日照時間（h/日）</div>', unsafe_allow_html=True)
# 日照（h/日）は日合計で表示（0h,4h,8h...）
_sun_col = "日照時間（日計)" if "日照時間（日計)" in daily_all.columns else ("日照時間（日計）" if "日照時間（日計）" in daily_all.columns else "日照時間（平均）")
_sun_max = float(pd.to_numeric(daily_all[_sun_col], errors="coerce").max()) if _sun_col in daily_all.columns else 0.0
_sun_top = (int((_sun_max + 3.999) // 4) * 4) if _sun_max > 0 else 16
fig_sun = _fig_bar(x_all, daily_all[_sun_col], y_dtick=4, y_range=(0, max(4, _sun_top)))
if USE_PLOTLY:
    st.plotly_chart(fig_sun, use_container_width=True, config=PLOTLY_CONFIG)
else:
    # フォールバック（軸の細かい調整は不可）
    st.bar_chart(daily_all.set_index(x_all)[_sun_col]) 
st.markdown('</div>', unsafe_allow_html=True)

# 降水量
st.markdown('<div class="card"><div class="card-title">降水量（mm/日）</div>', unsafe_allow_html=True)
# 降水量（mm/日）も日合計で表示
_rain_col = "降水量（日計)" if "降水量（日計)" in daily_all.columns else ("降水量（日計）" if "降水量（日計）" in daily_all.columns else "降水量（平均）")
fig_rain = _fig_bar(x_all, daily_all[_rain_col])
if USE_PLOTLY:
    st.plotly_chart(fig_rain, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.bar_chart(daily_all.set_index(x_all)[_rain_col]) 
st.markdown('</div>', unsafe_allow_html=True)

# GDD
st.markdown('<div class="card"><div class="card-title">GDD（有効積算温度）- 基準温度{:.1f}℃、4月1日〜</div>'.format(float(base_temp)), unsafe_allow_html=True)
x_gdd = pd.to_datetime(gdd_df["日付"])
fig_gdd = _fig_line(x_gdd, {"累積GDD": gdd_df["累積GDD"]})

# ステージ表示（右軸＋横線）
if USE_PLOTLY and fig_gdd is not None:
    # stage_inputs は詳細設定で作られている（空なら表示しない）
    stage_vals = []
    stage_texts = []
    for name, val in sorted(stage_inputs.items(), key=lambda kv: kv[1], reverse=True):
        stage_vals.append(val)
        stage_texts.append(name)
        fig_gdd.add_hline(y=val, line_width=1, line_dash="dot", line_color="rgba(0,0,0,0.25)")

    if stage_vals:
        fig_gdd.update_layout(
            yaxis2=dict(
                overlaying="y",
                side="right",
                tickmode="array",
                tickvals=stage_vals,
                ticktext=stage_texts,
                showgrid=False,
                zeroline=False,
            )
        )
        # 左右の余白を少し増やす
        fig_gdd.update_layout(margin=dict(l=10, r=30, t=10, b=10))
if USE_PLOTLY:
    st.plotly_chart(fig_gdd, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.line_chart(gdd_df.set_index(x_gdd)["累積GDD"])
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
if not gdd_df.empty:
    last_row = gdd_df.iloc[-1]
    m1, m2, m3 = st.columns(3)
    # 日付表示を 12/31 形式に
    _d = pd.to_datetime(last_row["日付"], errors="coerce")
    d_label = _d.strftime("%-m/%-d") if pd.notna(_d) else str(last_row["日付"])
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
