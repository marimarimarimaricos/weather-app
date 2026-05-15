import time
import requests
from bs4 import BeautifulSoup
import random
from datetime import datetime

WEBHOOK_URL = 'https://discord.com/api/webhooks/1366654784412712960/ef5nc4DhYfr2Bjo8JrYTI4Po8Blv9FhSvBu30cOTHQST-tmuAbXogTdjVqnjTeovZztg'
BASE_URL = 'https://ozawakokoro.jp/'
EXPECTED_TEXT = '現在、初診予約受付を停止しております'  # ← 改行なしバージョン！

def send_discord(message):
    data = {"content": message}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code != 204:
        print(f"Discord通知エラー: {response.status_code}")
    else:
        print("✅ Discordに通知しました")

def check_site():
    try:
        cache_buster = random.randint(1, 999999)
        url_with_buster = f"{BASE_URL}?t={cache_buster}"

        headers = {
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0'
        }

        response = requests.get(url_with_buster, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        # 🎯 <dd class="sml"> のテキストを直接取得
        dd = soup.select_one('dd.sml')
        if not dd:
            print("❌ <dd class='sml'> が見つかりませんでした")
            return

        dd_text = dd.get_text(separator="").strip()  # 改行無視して連結＆前後空白削除

        print(f"📄 現在の表示内容:「{dd_text}」")

        if dd_text != EXPECTED_TEXT:
            send_discord("【ALERT】ozawakokoroの「初診予約受付停止」の文言が変更されたかも！")
        else:
            print("✅ 変更なし（正常）")

    except Exception as e:
        print(f"❌ エラー発生: {e}")

# 7:00〜22:00の間、1時間に1回だけチェック
last_checked_hour = None

while True:
    now = datetime.now()
    current_hour = now.hour

    if 7 <= current_hour <= 22:
        if last_checked_hour != current_hour:
            print(f"🕒 {now.strftime('%Y-%m-%d %H:%M:%S')} にチェック実行")
            check_site()
            last_checked_hour = current_hour
    else:
        print(f"🌙 {now.strftime('%H:%M')} は対象時間外（スキップ）")

    time.sleep(60)
