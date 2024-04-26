"""
Функции модуля:
- gen_sklad: получает данные о запасах из Google Таблицы и фильтрует их для дальнейшего обновления.
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
    gc = gspread.service_account(filename='my-python-397519-3688db4697d6.json')
    sh = gc.open("КАЗНА")
    worksheet_name = "СКЛАД"
    worksheet = sh.worksheet(worksheet_name)
    data = worksheet.get('A1:Q')
    data = [row for row in data if any(cell.strip() for cell in row)]
    data = [[cell.strip() for cell in row] for row in data]
    filtered_data = [row for row in data[1:] if 'SKL' in row]
    columns = data[0]
    data = filtered_data
    sklad = pd.DataFrame(data, columns=columns)
    desired_columns = ['Артикул', 'Статус', 'Модель', 'Наличие', 'WB']
    sklad = sklad.loc[:, desired_columns]
    sklad_filtered = sklad[sklad['Статус'] == 'На складе']
    ym_frame = sklad_filtered[sklad_filtered['Наличие'].notna()][['Артикул', 'Наличие']].dropna(subset=['Наличие'])
    wb_frame = sklad_filtered[sklad_filtered['WB'].notna()][['WB', 'Наличие']].dropna(subset=['Наличие'])
    wb_final = []
    if not wb_frame.empty:
        for index, row in wb_frame.iterrows():
            sku = row['WB']
            amount = row['Наличие']
            wb_final.append({"sku": sku, "amount": int(amount)})

    ym_final = []
    current_time = datetime.now(timezone.utc).isoformat()
    if not ym_frame.empty:
        for index, row in ym_frame.iterrows():
            sku = str(row['Артикул'])
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
