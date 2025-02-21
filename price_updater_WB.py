"""
Функции модуля:
- wb_price_update: обновляет данные о ценах на Wildberries.
"""

import requests
import os
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


# def wb_price_update(wb_data):
#     print(wb_data)
#     wb_api_token = os.getenv('wb_token')
#     url_wb = 'https://discounts-prices-api.wildberries.ru/api/v2/upload/task'
#     headers = {
#         'Authorization': wb_api_token,
#         'Content-Type': 'application/json'
#     }
#
#     response = requests.post(url_wb, headers=headers, json=wb_data)
#     if response.status_code != 200:
#         # Исключаем ошибку 400 с текстом "No goods for process"
#         if response.status_code == 400 and "No goods for process" in response.json().get('errorText', ''):
#             pass
#         else:
#             message = f"😨 Ошибка при обновлении цен WB. Статус-код: {response.status_code}, Тело ответа: {response.text}"
#             telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)

def wb_price_update(wb_data):
    wb_api_token = os.getenv('wb_token')
    url_wb = 'https://discounts-prices-api.wildberries.ru/api/v2/upload/task'
    headers = {
        'Authorization': wb_api_token,
        'Content-Type': 'application/json'
    }

    response = requests.post(url_wb, headers=headers, json=wb_data)

    if response.status_code != 200:
        # Исключаем логические ошибки 208 и определённые случаи 400
        error_text = response.json().get('errorText', '')
        if response.status_code == 208:
            return
        elif response.status_code == 400:
            if "No goods for process" in error_text:
                return
            elif "specified prices and discounts are already set" in error_text.lower():  # Приводим к нижнему регистру
                return
        # Если ошибка критическая, отправляем уведомление
        message = f"😨 Ошибка при обновлении цен WB. Статус-код: {response.status_code}, Тело ответа: {response.text}"
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error, message=message)




def load_data(file_path):
    df = pd.read_csv(file_path)
    df = df[['WB Артикул', 'Цена до скидки']]
    df = df.dropna(subset=['WB Артикул'])
    df['WB Артикул'] = df['WB Артикул'].astype(int)
    df['Цена до скидки'] = df['Цена до скидки'].astype(int)
    return df


def create_wb_data(df):
    wb_data_list = []
    for _, row in df.iterrows():
        wb_data_list.append({
            "nmID": int(row['WB Артикул']),
            "price": int(row['Цена до скидки']),
            "discount": 16
        })
    return {"data": wb_data_list}


# Блок ниже нужен для тестирования, если нужно поработать без основного кода.
# if __name__ == "__main__":
#     # Считываем данные из файла sklad_prices.csv
#     df = load_data('sklad_prices.csv')
#
#     # Создаем данные для WB API
#     wb_data = create_wb_data(df)
#
#     # Вызов функции для обновления цен
#     wb_price_update(wb_data)
