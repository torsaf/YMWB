"""
Функции модуля:
- gen_sklad: получает данные о запасах из файла sklad_prices и фильтрует их для дальнейшего обновления.
- wb_update: обновляет данные о запасах на складе Wildberries.
- ym_update: обновляет данные о запасах на складе Yandex.Market.

"""

import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from notifiers import get_notifier
import pandas as pd
import gspread

load_dotenv()

telegram_got_token = os.getenv('telegram_got_token')
telegram_chat_id = os.getenv('telegram_chat_id')
telegram = get_notifier('telegram')


def gen_sklad():
    # Установить параметры pandas для полного вывода всех столбцов
    pd.set_option('display.max_columns', None)  # отображать все столбцы
    pd.set_option('display.width', 1000)  # установить ширину вывода
    # Путь к вашему CSV файлу
    file_path = 'sklad_prices.csv'
    # Загрузить данные из CSV файла в DataFrame, указав dtype для WB Barcode как строка
    sklad = pd.read_csv(file_path, dtype={'WB Barcode': str})
    # Определить необходимые столбцы
    desired_columns = ['YM', 'Модель', 'Наличие', 'WB Barcode']
    # Отобрать только необходимые столбцы
    sklad_filtered = sklad.loc[:, desired_columns]
    # Очистка значений в 'WB Barcode': удаление пробелов и преобразование строк в целые числа, если нужно
    sklad_filtered['WB Barcode'] = sklad_filtered['WB Barcode'].str.strip()
    # Фильтровать и очищать строки
    ym_frame = sklad_filtered[['YM', 'Наличие']].dropna(subset=['Наличие'])
    ym_frame = ym_frame.dropna(subset=['YM'])
    ym_frame = ym_frame[ym_frame['YM'].astype(str).str.strip() != '']
    wb_frame = sklad_filtered[['WB Barcode', 'Наличие']].dropna(subset=['Наличие'])
    wb_frame = wb_frame.dropna(subset=['WB Barcode'])

    wb_final = []
    if not wb_frame.empty:
        for index, row in wb_frame.iterrows():
            sku = row['WB Barcode']
            amount = row['Наличие']
            wb_final.append({"sku": sku, "amount": int(amount)})

    ym_final = []
    current_time = datetime.now(timezone.utc).isoformat()
    if not ym_frame.empty:
        for index, row in ym_frame.iterrows():
            sku = str(row['YM'])
            count = int(row['Наличие'])
            item = {
                "sku": sku,
                "items": [{"count": count, "updatedAt": current_time}]
            }
            ym_final.append(item)

    return wb_final, ym_final


def wb_update(wb_data):
    wb_api_token = os.getenv('wb_token')
    warehouse_id = int(os.getenv('warehouseId'))
    url_wb = f'https://suppliers-api.wildberries.ru/api/v3/stocks/{warehouse_id}'
    headers = {
        'Authorization': wb_api_token,
        'stocks': 'application/json'
    }
    params = {'warehouseId': warehouse_id, 'stocks': wb_data}
    response = requests.put(url_wb, headers=headers, json=params)
    if response.status_code != 204:
        message = f"😨 Ошибка при обновлении склада WB. Статус-код: {response.status_code}"
        telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message)


def ym_update(ym_data):
    ym_token = os.getenv('ym_token')
    campaign_id = os.getenv('campaign_id')
    url_ym = f'https://api.partner.market.yandex.ru/campaigns/{campaign_id}/offers/stocks'
    headers = {"Authorization": f"Bearer {ym_token}"}
    stock_data = {"skus": ym_data}
    response = requests.put(url_ym, headers=headers, json=stock_data)
    if response.status_code != 200:
        message = f"😨 Ошибка при обновлении склада YM. Статус-код: {response.status_code}"
        telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message)
