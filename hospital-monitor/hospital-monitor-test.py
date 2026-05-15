
import time
import requests
from bs4 import BeautifulSoup
import random

WEBHOOK_URL = 'https://discord.com/api/webhooks/1366654784412712960/ef5nc4DhYfr2Bjo8JrYTI4Po8Blv9FhSvBu30cOTHQST-tmuAbXogTdjVqnjTeovZztg'
BASE_URL = 'https://www.comfonavi.com/about/'
TARGET_PHRASE = 'こんにちは！'

def send_discord(message):
    data = {"content": message}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code != 204:
        print(f"Discord通知エラー: {response.status_code}")

def check_site():
    try:
        cache_buster = random.randint(1, 100000)
        url_with_buster = f"{BASE_URL}?t={cache_buster}"  # ← これで毎回違うURLにする
        headers = {
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0'
        }
        response = requests.get(url_with_buster, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text()

        if TARGET_PHRASE not in page_text:
            send_discord("【ALERT】comfonaviの「こんにちは！」が変更されたかも！")
            print("⚠️ アラート送信！（Discord）")
        else:
            print("✅ 変更なし（正常）")

    except Exception as e:
        print(f"エラー: {e}")

# 本番用はもっと長くすべき（ここは5秒でテスト）
while True:
    check_site()
    time.sleep(5)
