def get_daily_averages(df):
    grouped = df.groupby("日付").agg({
        "気温": ["mean", "max", "min"],
        "降水量": "mean",
        "湿度": "mean",
        "日照時間": "mean",
        "風速": "mean",
        "露点温度": "mean",
        "蒸気圧": "mean",
        "降雪": "mean",
        "積雪": "mean"
    }).reset_index()

    rename_map = {
        ("気温", "mean"): "気温（平均）",
        ("気温", "max"): "気温（最高）",
        ("気温", "min"): "気温（最低）",
        ("降水量", "mean"): "降水量（平均）",
        ("湿度", "mean"): "湿度（平均）",
        ("日照時間", "mean"): "日照時間（平均）",
        ("風速", "mean"): "風速（平均）",
        ("露点温度", "mean"): "露点温度（平均）",
        ("蒸気圧", "mean"): "蒸気圧（平均）",
        ("降雪", "mean"): "降雪（平均）",
        ("積雪", "mean"): "積雪（平均）"
    }

    grouped.columns = [
    "日付" if isinstance(col, str) and col == "日付" or (isinstance(col, tuple) and col[0] == "日付") else rename_map.get(col, f"{col[0]}_{col[1]}")
    for col in grouped.columns
]
    return grouped
