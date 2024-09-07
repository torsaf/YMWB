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
import pandas as pd
from dotenv import load_dotenv
from notifiers import get_notifier
import json

load_dotenv()

telegram_got_token = os.getenv('telegram_got_token')  # тут прописан ID телеграм бота
telegram_chat_id = os.getenv('telegram_chat_id')  # тут прописан ID общего чата
telegram = get_notifier('telegram')


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
    response = requests.get(url_ym, headers=headers, params=params)
    if response.status_code == 200:
        orders_data = response.json().get('orders', [])  # Получаем список заказов из ключа 'orders'
        return orders_data  # Возвращаем список заказов


def get_orders_wildberries():
    wb_api_token = os.getenv('wb_token')
    url = 'https://suppliers-api.wildberries.ru/api/v3/orders/new'
    headers = {'Authorization': wb_api_token}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        orders = response.json().get('orders', [])
        return orders

def get_orders_megamarket():
    mm_token = os.getenv('mm_token')
    url_mm = 'https://api.megamarket.tech/api/market/v1/partnerService/order/new'
    headers = {"Authorization": f"Bearer {mm_token}"}
    response = requests.get(url_mm, headers=headers)
    if response.status_code == 200:
        orders_data = response.json().get('data', {}).get('shipments', [])
        return orders_data



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
    file_path = 'sklad_prices.csv'
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
            if write_order_id_to_file(order.get('id'), file_path):
                message = f"📦 Новый заказ на {platform}:\n\n"
                message += f"ID заказа: {order.get('id')}\n"
                if supplier == 'Yandex.Market':
                    for item in order.get('items', []):
                        # Это цена со всеми скидками
                        subsidy_amount = next((subsidy.get('amount') for subsidy in item.get('subsidies', []) if
                                               subsidy.get('type') == 'SUBSIDY'), 0)
                        # Это цена скидки яндекса, которая плюсуется ниже с ценой со всеми скидками и получаем нужное значение.
                        price = int(item.get('buyerPrice'))
                        message += f"Товар: {item.get('offerName')}\nЦена: {int(subsidy_amount + price)} р.\n"
                        # message += f"Товар: {item.get('offerName')}\nЦена: {item.get('subsidies')} р.\n"
                elif supplier == 'Wildberries':
                    message += f"Артикул: {order.get('article')} \n"
                    message += f"Товар: {get_product(order.get('nmId'))} \n"
                    message += f"Цена: {str(order.get('convertedPrice'))[:-2]} р.\n"
                elif supplier == 'MegaMarket':
                    for shipment in order.get('shipments', []):
                        for item in shipment.get('items', []):
                            message += f"Товар: {item.get('itemName')}\nЦена: {item.get('price')} р.\n"
                message += '\n'
                telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message)


def check_for_new_orders():
    orders_yandex_market = get_orders_yandex_market()
    notify_about_new_orders(orders_yandex_market, "Yandex.Market", "Yandex.Market")

    orders_wildberries = get_orders_wildberries()
    notify_about_new_orders(orders_wildberries, "Wildberries", "Wildberries")

    orders_megamarket = get_orders_megamarket()
    notify_about_new_orders(orders_megamarket, "Megamarket", "Megamarket")
