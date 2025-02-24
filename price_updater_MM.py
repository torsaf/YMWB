"""
Функции модуля:
- read_sklad_csv: получает данные о ценах на товары MM из файла sklad.csv.
- mm_price_update: обновляет цены на MM.
"""

import os
import requests
import csv
import json
from dotenv import load_dotenv
from notifiers import get_notifier

# Загрузка переменных окружения
load_dotenv()

telegram_got_token = os.getenv('telegram_got_token')
telegram_chat_id = os.getenv('telegram_chat_id')
telegram_got_token_error = os.getenv('telegram_got_token_error')
telegram_chat_id_error = os.getenv('telegram_chat_id_error')
telegram = get_notifier('telegram')


def mm_price_update(mm_data):
    mm_token = os.getenv('mm_token')
    url_mm = 'https://api.megamarket.tech/api/merchantIntegration/v1/offerService/manualPrice/save'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    payload = {
        "meta": {},
        "data": {
            "token": mm_token,
            "prices": mm_data
        }
    }

    # Логирование отправляемого payload
    # print("Отправляемые данные:", json.dumps(payload, indent=4))
    # Преобразуем payload в строку JSON
    payload_json = json.dumps(payload)
    # Выполняем запрос
    response = requests.post(url_mm, headers=headers, data=payload_json, timeout=10)
    if response.status_code != 200:
        message = f"😨 Ошибка при обновлении цен MM. Статус-код: {response.status_code}, Тело ответа: {response.text}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)


def read_sklad_csv(file_path):
    mm_data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["MM"].strip():
                offer = {
                    "offerId": row["MM"],
                    "price": int(row["Цена"]),
                    "isDeleted": False
                }
                mm_data.append(offer)
    return mm_data

# Блок ниже нужен для тестирования, если нужно поработать без основного кода.
# if __name__ == "__main__":
#     # Путь к файлу sklad_prices.csv
#     file_path = 'sklad_prices.csv'
#
#     # Чтение данных из CSV файла
#     mm_data = read_sklad_csv(file_path)
#
#     # Обновление цен на MegaMarket
#     mm_price_update(mm_data)
