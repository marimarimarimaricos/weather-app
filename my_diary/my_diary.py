from datetime import datetime

# 日本語の曜日リスト
weekdays_ja = ["月", "火", "水", "木", "金", "土", "日"]

diary1 = {
    "title": "わりとていねいな土曜日",
    "content": "今日は折越さんが庭で作業。せとはご飯を嫌がる。庭のローズマリーを煎茶に入れて飲んだら美味しかった。",
    "mood": "sad",
    "date": datetime.strptime("2025-04-19","%Y-%m-%d")
    }

diary2 = {
    "title": "雨の日曜日",
    "content": "一日中家の中でごろごろ。",
    "mood": "lazy",
    "date": datetime.strptime("2025-04-06", "%Y-%m-%d")
    }

diary3 = {
    "title": "やる気が出た月曜日",
    "content": "朝からPythonでfor文！",
    "mood": "motivated",
    "date": datetime.strptime("2025-04-07", "%Y-%m-%d")
    }

diary4 = {
    "title": "へろへろの火曜日",
    "content": "漢方薬局へ行った",
    "mood": "sad",
    "date": datetime.strptime("2025-04-08", "%Y-%m-%d")
    }

# リストにまとめる
diary_list = [diary1, diary2, diary3, diary4]

# 対応する日本語の曜日を取得


for diary in diary_list:
    # 曜日番号を取得（0:月曜〜6:日曜）
    weekday_num = diary["date"].weekday()
    weekday = weekdays_ja[weekday_num]
    date_str = diary["date"].strftime("%Y年%m月%d日") + f" ({weekday})"

    print("📅", date_str)
    print("📝", diary["title"])
    print("😌", diary["mood"])

    # 気分に応じたコメント
    if diary["mood"] == "happy":
        print("今日は気分いいね！")
    elif diary["mood"] == "sad":
        print("ちょっとゆっくりしよう")
    elif diary["mood"] == "chill":
        print("ゆったり気分だね〜")
    elif diary["mood"] == "motivated":
        print("やる気まんまんですごーい！")
    elif diary["mood"] == "lazy":
        print("今日は一日パジャマでGO")
    else:
        print("どんな気分も大事にしよう！")
    
    print("-"*30)