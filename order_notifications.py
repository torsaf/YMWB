"""
Модуль order_notifications предназначен для уведомления в Telegram о новых заказов с платформ Yandex.Market и Wildberries,

Функции модуля:
- get_orders_yandex_market: получает заказы с платформы Yandex.Market, используя API.
- get_orders_wildberries: получает заказы с платформы Wildberries, используя API.
- get_orders_megamarket: получает заказы с платформы Megamarket, используя API.
- write_order_id_to_file: записывает ID заказов в файл для отслеживания уже обработанных заказов.
- notify_about_new_orders: отправляет уведомления в Telegram о новых заказах с указанных платформ.

"""

import requests
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from dotenv import load_dotenv
from notifiers import get_notifier
from datetime import datetime, timedelta
import time
import json

load_dotenv()

telegram_got_token = os.getenv('telegram_got_token')  # тут прописан ID телеграм бота
telegram_chat_id = os.getenv('telegram_chat_id')  # тут прописан ID общего чата
telegram = get_notifier('telegram')


def update_stock(articul):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)

    # Подключаемся к Google Sheets
    gc = gspread.service_account(filename='my-python-397519-3688db4697d6.json')
    sh = gc.open("КАЗНА")
    worksheet_name = "СКЛАД"
    worksheet = sh.worksheet(worksheet_name)

    # Загружаем данные из таблицы
    data = worksheet.get_all_values()

    # Создаём DataFrame
    columns = data[0]
    sklad = pd.DataFrame(data[1:], columns=columns)

    # Преобразование данных в числовые значения где необходимо
    sklad['Наличие'] = pd.to_numeric(sklad['Наличие'], errors='coerce').fillna(0).astype(int)

    # Восстанавливаем числовой тип артикулов
    sklad['Арт мой'] = sklad['Арт мой'].apply(lambda x: int(x) if pd.notna(x) and x != '' else '')
    sklad['Арт UM'] = sklad['Арт UM'].apply(lambda x: int(x) if pd.notna(x) and x != '' else '')

    # Приведение артикула к строковому формату только для поиска
    articul = str(articul)

    # Фильтрация строк только с UNT и статусом 'Товар в UM'
    filtered_sklad = sklad[((sklad['Поставщик'] == 'SKL') & (sklad['Статус'] == 'На складе')) |
                           ((sklad['Поставщик'] == 'UNT') & (sklad['Статус'] == 'Товар в UM'))]

    # Поиск строки по артикулу
    matched_rows = filtered_sklad[filtered_sklad['Арт мой'].astype(str) == articul]

    if not matched_rows.empty:
        # Извлечение индекса строки
        row_index = matched_rows.index[0]

        # Определяем склад
        supplier = sklad.at[row_index, 'Поставщик']
        stock_status = sklad.at[row_index, 'Статус']

        if supplier == 'SKL' and stock_status == 'На складе':
            sklad_name = "Наш склад"
        elif supplier == 'UNT' and stock_status == 'Товар в UM':
            sklad_name = "UM"
        else:
            sklad_name = "Неизвестный склад"

        # Извлечение данных о товаре
        previous_quantity = int(sklad.at[row_index, 'Наличие'])  # Приводим к int
        sklad.at[row_index, 'Наличие'] -= 1
        updated_quantity = int(sklad.at[row_index, 'Наличие'])  # Приводим к int
        product_name = sklad.loc[row_index, 'Модель']

        # Если это Series, извлекаем первое значение
        if isinstance(product_name, pd.Series):
            product_name = product_name.iloc[0]

        # Убираем лишние пробелы
        product_name = str(product_name).strip()

        # Формирование сообщения для Telegram
        message = (
            f"✅ Бот вычел со склада\n\n"
            f"Товар: \"{product_name}\"\n"
            f"Артикул: {articul}\n"
            f"Было: {previous_quantity}, стало: {updated_quantity}.\n"
            f"Склад: {sklad_name}"
        )

        # Отправляем сообщение в Telegram
        telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message)

        # Убираем NaN и бесконечные значения перед обновлением в Google Sheets
        sklad = sklad.replace([float('inf'), float('-inf')], 0).fillna(0)

        # Ограничиваем данные для обновления до диапазона A:H
        updated_data = sklad.iloc[:, :8].values.tolist()
        worksheet.update(values=updated_data, range_name='A2:H')


def get_orders_yandex_market():
    campaign_id = os.getenv('campaign_id')
    ym_token = os.getenv('ym_token')
    url_ym = f'https://api.partner.market.yandex.ru/campaigns/{campaign_id}/orders'
    headers = {"Authorization": f"Bearer {ym_token}"}
    params = {
        "fake": "false",
        "status": "PROCESSING",
        "substatus": "STARTED"
    }
    response = requests.get(url_ym, headers=headers, params=params, timeout=10)
    if response.status_code == 200:
        orders_data = response.json().get('orders', [])  # Получаем список заказов из ключа 'orders'
        return orders_data  # Возвращаем список заказов


def get_orders_wildberries():
    wb_api_token = os.getenv('wb_token')
    url = 'https://suppliers-api.wildberries.ru/api/v3/orders/new'
    # url = 'https://content-api-sandbox.wildberries.ru/api/v3/orders/new'
    headers = {'Authorization': wb_api_token}
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        orders = response.json().get('orders', [])
        return orders


def get_orders_megamarket():
    mm_token = os.getenv('mm_token')
    url_mm = 'https://api.megamarket.tech/api/market/v1/partnerService/order/new'
    headers = {"Authorization": f"Bearer {mm_token}"}
    response = requests.get(url_mm, headers=headers, timeout=10)
    if response.status_code == 200:
        orders_data = response.json().get('data', {}).get('shipments', [])
        return orders_data




def get_orders_ozon():
    url = "https://api-seller.ozon.ru/v3/posting/fbs/unfulfilled/list"
    headers = {
        'Client-Id': os.getenv('ozon_client_ID'),
        'Api-Key': os.getenv('ozon_API_key'),
        'Content-Type': 'application/json'
    }

    # Устанавливаем диапазон времени за последний год
    cutoff_from = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"  # Год назад
    cutoff_to = datetime.utcnow().isoformat() + "Z"  # Сегодня

    payload = {
        "dir": "ASC",  # Сортировка по возрастанию
        "filter": {
            "cutoff_from": cutoff_from,  # Начало диапазона времени
            "cutoff_to": cutoff_to,      # Конец диапазона времени
            "status": "awaiting_packaging",  # Пример статуса (необработанные отправления)
            "delivery_method_id": [],  # Опционально
            "provider_id": [],         # Опционально
            "warehouse_id": []         # Опционально
        },
        "limit": 100,  # Максимальное количество возвращаемых элементов за один запрос
        "offset": 0,   # Начальный индекс
        "with": {
            "analytics_data": True,  # Включить аналитические данные
            "barcodes": True,        # Включить штрихкоды
            "financial_data": True,  # Включить финансовые данные
            "translit": True         # Включить транслитерацию
        }
    }
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    if response.status_code == 200:
        # Фильтруем заказы с нужным статусом 'awaiting_packaging'
        orders = response.json().get("result", {}).get("postings", [])
        filtered_orders = [order for order in orders if order.get("status") == "awaiting_packaging"]
        return filtered_orders
    else:
        print(f"Ошибка при получении заказов с Ozon: {response.status_code}, {response.text}")
        return []


def write_order_id_to_file(order_id, filename):
    # Проверяем существует ли файл
    if not os.path.exists(filename):
        # Если файл не существует, создаем его и записываем в него ID заказа
        with open(filename, 'w') as file:
            file.write(str(order_id) + '\n')
        return True
    else:
        # Если файл существует, читаем его содержимое
        with open(filename, 'r') as file:
            existing_ids = set(file.read().splitlines())

        # Проверяем, есть ли уже такой ID в файле
        if str(order_id) not in existing_ids:
            # Если нет, добавляем его в файл
            with open(filename, 'a') as file:
                file.write(str(order_id) + '\n')
            return True
        else:
            # Если ID уже существует в файле, ничего не делаем и возвращаем False
            return False


# Путь к файлу, куда вы хотите записывать ID заказов
file_path = 'order_ids.txt'


# Функция, которая берет название товара из файла, когда есть заказ с WB
def get_product(nmId):
    # Путь к вашему CSV файлу
    file_path = 'sklad_prices_wildberries.csv'
    # Загрузить данные из CSV файла в DataFrame
    sklad = pd.read_csv(file_path)
    # Найти строку, где "WB Артикул" соответствует nmId
    product_row = sklad[sklad['WB Артикул'] == nmId]
    # Если совпадение найдено, вернуть значение из столбца "Модель"
    if not product_row.empty:
        return product_row.iloc[0]['Модель']
    else:
        return None


def notify_about_new_orders(orders, platform, supplier):
    if not orders:
        pass
        # message = f"Новых заказов на {platform} от {supplier} нет."
        # telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message)
    else:
        for order in orders:
            # Запись ID заказа в файл перед добавлением товаров в сообщение
            order_id = order.get('posting_number') if supplier == 'Ozon' else order.get('id')
            # Запись ID заказа в файл перед добавлением товаров в сообщение
            if write_order_id_to_file(order_id, file_path):
                message = f"📦 Новый заказ на *{platform}*:\n\n"
                message += f"ID заказа: {order_id}\n"
                if supplier == 'Yandex.Market':
                    # Добавляем дату отгрузки
                    shipment_date = next(
                        (shipment.get('shipmentDate') for shipment in order.get('delivery', {}).get('shipments', [])),
                        'Не указано'
                    )
                    message += f"Дата отгрузки: {shipment_date}\n"
                    for item in order.get('items', []):
                        # Артикул товара
                        offer_id = item.get('offerId', 'Не указан')
                        # Имя товара
                        offer_name = item.get('offerName', 'Не указано')
                        # Цена товара
                        subsidy_amount = next(
                            (subsidy.get('amount') for subsidy in item.get('subsidies', []) if
                             subsidy.get('type') == 'SUBSIDY'), 0
                        )
                        price = int(item.get('buyerPrice', 0))
                        total_price = int(subsidy_amount + price)
                        # Добавляем информацию о товаре
                        message += f"Артикул: {offer_id}\n"
                        message += f"Товар: {offer_name}\n"
                        message_minus_odin = offer_id
                        message += f"Цена: {total_price} р.\n"
                elif supplier == 'Wildberries':
                    message += f"Артикул: {order.get('article')} \n"
                    message += f"Товар: {get_product(order.get('nmId'))} \n"
                    message += f"Цена: {str(order.get('convertedPrice'))[:-2]} р.\n"
                    message_minus_odin = order.get('article')
                elif supplier == 'MegaMarket':
                    for shipment in order.get('shipments', []):
                        for item in shipment.get('items', []):
                            message += f"Товар: {item.get('itemName')}\nЦена: {item.get('price')} р.\n"


                elif supplier == 'Ozon':  # Добавляем поддержку Ozon
                    shipment_date_raw = order.get('shipment_date')  # Получаем дату отгрузки
                    # Преобразуем дату из ISO 8601 в формат DD.MM.YYYY
                    if shipment_date_raw:
                        shipment_date = datetime.strptime(shipment_date_raw, "%Y-%m-%dT%H:%M:%SZ").strftime("%d.%m.%Y")
                        message += f"Дата отгрузки: {shipment_date}\n"
                    else:
                        shipment_date = "Не указана"
                        message += f"Дата отгрузки: {shipment_date}\n"

                    for product in order.get('products', []):
                        message += f"Артикул: {product['offer_id']}\n"
                        message += f"Товар: {product['name']}\n"  # Получаем название товара
                        # Округляем цену до целого числа, чтобы убрать ".0000"
                        price = int(float(product['price']))
                        message += f"Цена: {price} р.\n"  # Форматируем цену
                        message_minus_odin = product.get('offer_id')


                message += '\n'
                telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message, parse_mode='markdown')
                time.sleep(5)
                # Затем вычитаем товар со склада
                if message_minus_odin:  # Если товар определён
                    update_stock(message_minus_odin)
                message1 = '📦'
                telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message1)
                time.sleep(5)


def check_for_new_orders():
    orders_yandex_market = get_orders_yandex_market()
    notify_about_new_orders(orders_yandex_market, "Yandex.Market", "Yandex.Market")

    orders_wildberries = get_orders_wildberries()
    notify_about_new_orders(orders_wildberries, "Wildberries", "Wildberries")

    orders_megamarket = get_orders_megamarket()
    notify_about_new_orders(orders_megamarket, "Megamarket", "Megamarket")

    orders_ozon = get_orders_ozon()  # Получаем заказы с Ozon
    notify_about_new_orders(orders_ozon, "Ozon", "Ozon")  # Уведомляем о новых заказах с Ozon

# check_for_new_orders()