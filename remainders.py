"""
Функции модуля:
- gen_sklad: получает данные о запасах из файла sklad_prices и фильтрует их для дальнейшего обновления.
- wb_update: обновляет данные о запасах на складе Wildberries.
- ym_update: обновляет данные о запасах на складе Yandex.Market.
- mm_update: обновляет данные о запасах на складе MegaMarket.
"""

import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from notifiers import get_notifier
import pandas as pd
import json
import time
import gspread

load_dotenv()

telegram_got_token = os.getenv('telegram_got_token')
telegram_chat_id = os.getenv('telegram_chat_id')
telegram_got_token_error = os.getenv('telegram_got_token_error')
telegram_chat_id_error = os.getenv('telegram_chat_id_error')
telegram = get_notifier('telegram')


def gen_sklad():
    # Параметры pandas для отображения всех столбцов
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    # Чтение и обработка файла Wildberries
    wb_file = 'sklad_prices_wildberries.csv'
    wb_data = pd.read_csv(wb_file, dtype={'WB Barcode': str})
    wb_data['WB Barcode'] = wb_data['WB Barcode'].str.strip()
    wb_data = wb_data.dropna(subset=['WB Barcode', 'Наличие'])
    wb_final = [{"sku": row['WB Barcode'], "amount": int(row['Наличие'])} for _, row in wb_data.iterrows()]

    # Чтение и обработка файла Yandex
    ym_file = 'sklad_prices_yandex.csv'
    ym_data = pd.read_csv(ym_file, dtype={'YM': str})
    ym_data['YM'] = ym_data['YM'].str.strip()
    ym_data = ym_data.dropna(subset=['YM', 'Наличие'])
    current_time = datetime.now(timezone.utc).isoformat()
    ym_final = [
        {
            "sku": str(row['YM']),
            "items": [{"count": int(row['Наличие']), "updatedAt": current_time}]
        }
        for _, row in ym_data.iterrows()
    ]

    # Чтение и обработка файла Megamarket
    mm_file = 'sklad_prices_megamarket.csv'
    mm_data = pd.read_csv(mm_file, dtype={'MM': str})
    mm_data['MM'] = mm_data['MM'].str.strip()
    mm_data = mm_data.dropna(subset=['MM', 'Наличие'])
    mm_final = [
        {
            "offerId": str(row['MM']),
            "quantity": int(row['Наличие'])
        }
        for _, row in mm_data.iterrows()
    ]

    # Чтение и обработка файла Ozon
    oz_file = 'sklad_prices_ozon.csv'
    oz_data = pd.read_csv(oz_file, dtype={'OZ': str})
    # oz_data['OZ'] = oz_data['OZ'].str.strip().str.rstrip('.0')
    oz_data = oz_data.dropna(subset=['OZ', 'Наличие'])
    warehouse_id = 1020002115578000  # Здесь должен быть актуальный идентификатор склада
    oz_final = [
        {
            "offer_id": str(row['OZ']),
            "product_id": int(row['OZ']),  # Здесь предполагается, что `OZ` также является product_id
            "stock": int(row['Наличие']),
            "warehouse_id": warehouse_id
        }
        for _, row in oz_data.iterrows()
    ]

    return wb_final, ym_final, mm_final, oz_final


def oz_update(oz_data):
    ozon_client_id = os.getenv('ozon_client_ID')
    ozon_api_key = os.getenv('ozon_API_key')
    url_ozon = 'https://api-seller.ozon.ru/v2/products/stocks'

    headers = {
        'Client-Id': ozon_client_id,
        'Api-Key': ozon_api_key,
        'Content-Type': 'application/json'
    }

    # Формируем тело запроса с данными остатков
    payload = {
        "stocks": oz_data
    }

    # Логируем данные перед отправкой
    # print("Отправляемые данные на Ozon:")
    # print(json.dumps(payload, indent=4, ensure_ascii=False))

    # Отправляем запрос на обновление остатков
    response = requests.post(url_ozon, headers=headers, json=payload)
    if response.status_code != 200:
        message = f"😨 Ошибка при обновлении склада Ozon. Статус-код: {response.status_code}, Текст ошибки: {response.text}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)


def wb_update(wb_data):
    wb_api_token = os.getenv('wb_token')
    warehouse_id = int(os.getenv('warehouseId'))
    # url_wb = f'https://suppliers-api.wildberries.ru/api/v3/stocks/{warehouse_id}'
    url_wb = f'https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}'
    headers = {
        'Authorization': wb_api_token,
        'stocks': 'application/json'
    }
    params = {'warehouseId': warehouse_id, 'stocks': wb_data}
    response = requests.put(url_wb, headers=headers, json=params)
    if response.status_code != 204:
        message = f"😨 Ошибка при обновлении склада WB. Статус-код: {response.status_code}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)


def ym_update(ym_data):
    ym_token = os.getenv('ym_token')
    campaign_id = os.getenv('campaign_id')
    url_ym = f'https://api.partner.market.yandex.ru/campaigns/{campaign_id}/offers/stocks'
    headers = {"Authorization": f"Bearer {ym_token}"}
    stock_data = {"skus": ym_data}
    response = requests.put(url_ym, headers=headers, json=stock_data)
    if response.status_code != 200:
        message = f"😨 Ошибка при обновлении склада YM. Статус-код: {response.status_code}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)


def mm_update(mm_data):
    mm_token = os.getenv('mm_token')
    url_mm = 'https://api.megamarket.tech/api/merchantIntegration/v1/offerService/stock/update'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    payload = {
        "meta": {},
        "data": {
            "token": mm_token,
            "stocks": mm_data
        }
    }
    # Преобразуем payload в строку JSON
    payload_json = json.dumps(payload)
    # Выполняем запрос
    response = requests.post(url_mm, headers=headers, data=payload_json)
    if response.status_code != 200:
        message = f"😨 Ошибка при обновлении склада MM. Статус-код: {response.status_code}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)
