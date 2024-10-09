"""
Функции модуля:
- oz_price_update: обновляет данные о ценах на OZON.
"""


import os
import csv
import requests
import json
import pandas as pd
from dotenv import load_dotenv
from notifiers import get_notifier

# Загрузка переменных окружения
load_dotenv()

telegram_got_token = os.getenv('telegram_got_token')
telegram_chat_id = os.getenv('telegram_chat_id')
telegram_got_token_error = os.getenv('telegram_got_token_error')
telegram_chat_id_error = os.getenv('telegram_chat_id_error')
telegram = get_notifier('telegram')


# Функция для обновления цен на Ozon
def oz_price_update(oz_data):
    ozon_client_id = os.getenv('ozon_client_ID')
    ozon_api_key = os.getenv('ozon_API_key')
    url_ozon = 'https://api-seller.ozon.ru/v1/product/import/prices'

    headers = {
        'Client-Id': ozon_client_id,
        'Api-Key': ozon_api_key,
        'Content-Type': 'application/json'
    }

    payload = {
        "prices": oz_data
    }

    # Логирование отправляемого payload
    # print("Отправляемые данные:", json.dumps(payload, indent=4))

    # Преобразуем payload в строку JSON и выполняем запрос
    payload_json = json.dumps(payload)
    response = requests.post(url_ozon, headers=headers, data=payload_json)

    if response.status_code != 200:
        message = f"😨 Ошибка при обновлении цен Ozon. Статус-код: {response.status_code}, Текст ошибки: {response.text}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)
    # else:
    #     print("Цены успешно обновлены на Ozon!")


def read_sklad_csv(file_path):
    mm_data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["OZ"].strip():
                offer = {
                    "offer_id": row["OZ"],  # Исправлено на offer_id
                    "price": str(row["Цена"]),  # Преобразуем в строку
                    "old_price": str(row["Цена до скидки"]) if row["Цена до скидки"] else "0",  # Если есть скидка
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
#     oz_data = read_sklad_csv(file_path)
#     print(oz_data)
#
#     # Обновление цен на MegaMarket
#     oz_price_update(oz_data)
