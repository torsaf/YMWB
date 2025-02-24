"""
Функции модуля:
- read_sklad_csv: получает данные о ценах на товары YM из файла sklad.csv.
- ym_price_update: обновляет цены на YM.
"""

import os
import requests
import csv
from dotenv import load_dotenv
from notifiers import get_notifier

# Загрузка переменных окружения
load_dotenv()

telegram_got_token = os.getenv('telegram_got_token')
telegram_chat_id = os.getenv('telegram_chat_id')
telegram_got_token_error = os.getenv('telegram_got_token_error')
telegram_chat_id_error = os.getenv('telegram_chat_id_error')
telegram = get_notifier('telegram')


def ym_price_update(ym_data):
    ym_token = os.getenv('ym_token')
    businessId = os.getenv('businessId')
    url_ym = f'https://api.partner.market.yandex.ru/businesses/{businessId}/offer-prices/updates'
    headers = {
        "Authorization": f"Bearer {ym_token}",
        "Content-Type": "application/json"
    }
    response = requests.post(url_ym, headers=headers, json=ym_data, timeout=10)
    if response.status_code != 200:
        message = f"😨 Ошибка при обновлении цен YM. Статус-код: {response.status_code}, Тело ответа: {response.text}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)


def read_sklad_csv(file_path):
    ym_data = {"offers": []}
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["YM"].strip():
                offer = {
                    "offerId": row["YM"],
                    "price": {
                        "value": int(row["Цена"]),
                        "currencyId": "RUR",
                        "discountBase": int(row["Цена до скидки"])
                    }
                }
                ym_data["offers"].append(offer)
    return ym_data

# Блок ниже нужен для тестирования, если нужно поработать без основного кода.
# if __name__ == "__main__":
#     # Путь к файлу sklad_prices.csv
#     file_path = 'sklad_prices.csv'
#
#     # Чтение данных из CSV файла
#     ym_data = read_sklad_csv(file_path)
#
#     # Обновление цен на Yandex Market
#     ym_price_update(ym_data)

