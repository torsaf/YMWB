"""
Модуль order_notifications предназначен для получения новых заказов с маркетплейсов
Yandex.Market, Wildberries и Ozon, уведомления о них в Telegram и автоматического
вычитания остатков со склада.

Функции модуля:

- get_orders_yandex_market:
    Получает заказы с платформы Yandex.Market через API.

- get_orders_wildberries:
    Получает заказы с Wildberries по API v3.

- get_orders_ozon:
    Получает необработанные заказы с платформы Ozon через API v3.

- write_order_id_to_file:
    Записывает ID заказов в файл, чтобы не обрабатывать их повторно.

- update_stock:
    Автоматически уменьшает остаток товара в базе данных или Google Sheets (для Sklad).

- notify_about_new_orders:
    Формирует сообщение о заказе и отправляет его в Telegram. Также вызывает вычитание со склада.

- check_for_new_orders:
    Централизованно вызывает сбор заказов с платформ и уведомления по ним.
"""

import os
import sqlite3
import requests
import pandas as pd
import gspread

from datetime import datetime, timedelta
from dotenv import load_dotenv
from logger_config import logger
from notifiers import get_notifier
from web_app import choose_best_supplier_for_row


# Загрузка переменных окружения из .env
load_dotenv(dotenv_path=os.path.join("System", ".env"))

# Настройка Telegram-уведомлений
telegram_got_token = os.getenv('telegram_got_token')
telegram_chat_id = os.getenv('telegram_chat_id')
telegram = get_notifier('telegram')

# --- Счётчик заказов с ежедневным сбросом ---
counter_file = "System/order_counter.txt"

def write_order_to_gsheets(platform, order_id, items_to_update, rrc_price, supplier_fixed):


    """
    Добавляет заказ в таблицу КАЗНА (ВБ / ЯМ / ОЗ).
    Записываем: ID, товар, артикул, ОПТ, РРЦ, статус.
    Все данные берём из базы marketplace — как в update_stock.
    """

    # Определяем лист
    sheet_map = {"wildberries": "ВБ", "yandex": "ЯМ", "ozon": "ОЗ"}
    ws_name = sheet_map.get(platform.lower())
    if ws_name is None:
        logger.error(f"❌ Неизвестная платформа: {platform}")
        return

    # Подключение Google Sheets
    gc = gspread.service_account(filename="System/my-python-397519-3688db4697d6.json")
    sh = gc.open("КАЗНА")
    ws = sh.worksheet(ws_name)

    # Функция поиска строки вставки
    def find_insert_row(ws):
        column_a = ws.col_values(1)
        for row_num, value in enumerate(column_a[1:], start=2):
            if str(value).strip():
                return max(2, row_num - 1)
        return 2

    insert_row = find_insert_row(ws)

    # Берём основной товар заказа (первый)
    item_art, _ = items_to_update[0]

    # Читаем данные из базы
    product_name = ""
    supplier_name = ""
    opt_price_value = 0
    rrc_price_value = int(str(rrc_price).replace(" р.", "")) if rrc_price else 0

    try:
        conn = sqlite3.connect("System/marketplace_base.db")
        df_item = pd.read_sql_query(
            "SELECT * FROM marketplace WHERE Sklad = ?",
            conn,
            params=(str(item_art),)
        )

        if not df_item.empty:
            row0 = df_item.iloc[0]

            product_name = row0.get("Модель")

            real_opt = int(row0.get("Опт", 0))
            # Используем поставщика, выбранного в update_stock
            opt_price_value = real_opt if supplier_fixed and supplier_fixed.lower() == "sklad" else 0


    except Exception as e:
        logger.error(f"❌ Ошибка чтения товара для таблицы: {e}")
    finally:
        conn.close()

    today_iso = datetime.now().strftime("%Y-%m-%d")

    if ws_name == "ВБ":
        ws.update(
            f"A{insert_row}",
            [[today_iso]],
            value_input_option="USER_ENTERED"
        )
        ws.update(f"C{insert_row}", [[order_id]])
        ws.update(f"D{insert_row}", [[product_name]])
        ws.update(f"F{insert_row}", [[opt_price_value]])
        ws.update(f"G{insert_row}", [[rrc_price_value]])
        ws.update(f"M{insert_row}", [["На сборке"]])

    elif ws_name == "ЯМ":
        ws.update(f"A{insert_row}", [["FBS"]])  # Колонка A = FBS
        ws.update(
            f"B{insert_row}",
            [[today_iso]],
            value_input_option="USER_ENTERED"
        )
        ws.update(f"C{insert_row}", [[order_id]])
        ws.update(f"D{insert_row}", [[product_name]])
        ws.update(f"F{insert_row}", [[opt_price_value]])
        ws.update(f"G{insert_row}", [[rrc_price_value]])
        ws.update(f"M{insert_row}", [["На сборке"]])

    elif ws_name == "ОЗ":
        ws.update(
            f"A{insert_row}",
            [[today_iso]],
            value_input_option="USER_ENTERED"
        )
        ws.update(f"B{insert_row}", [[order_id]])  # ID → колонка B
        ws.update(f"C{insert_row}", [[product_name]])  # Название → колонка C
        ws.update(f"E{insert_row}", [[opt_price_value]])  # Опт → колонка E
        ws.update(f"F{insert_row}", [[rrc_price_value]])  # РРЦ → колонка F
        ws.update(f"L{insert_row}", [["На сборке"]])  # Статус → колонка L

    ws.insert_row([], insert_row)

    logger.success(f"📄 Заказ записан в '{ws_name}', строка {insert_row}")



def get_today_order_number():
    today = datetime.now().strftime("%Y-%m-%d")

    # Если файла нет — создаём с первым заказом
    if not os.path.exists(counter_file):
        with open(counter_file, "w") as f:
            f.write(f"{today}|1")
        return 1

    # Файл есть — читаем данные
    with open(counter_file, "r") as f:
        data = f.read().strip()

    # Если файл был пустой или повреждён — заново создаем
    if not data or "|" not in data:
        with open(counter_file, "w") as f:
            f.write(f"{today}|1")
        return 1

    saved_date, saved_num = data.split("|")
    saved_num = int(saved_num)

    # Новый день — сбрасываем счётчик
    if saved_date != today:
        with open(counter_file, "w") as f:
            f.write(f"{today}|1")
        return 1

    # Та же дата — увеличиваем
    new_num = saved_num + 1
    with open(counter_file, "w") as f:
        f.write(f"{today}|{new_num}")
    return new_num



def update_stock(articul, platform, quantity=1):
    logger.info(f"🔁 Вычитание со склада: {articul} | Платформа: {platform}")
    platform = platform.lower()
    db_path = "System/marketplace_base.db"
    conn = sqlite3.connect(db_path, timeout=10)
    articul = str(articul).strip()

    df = pd.read_sql_query(
        "SELECT * FROM marketplace WHERE Sklad = ? AND Маркетплейс = ?",
        conn,
        params=(articul, platform)
    )

    if df.empty:
        conn.close()
        return None, None

    def format_price(value):
        try:
            return f"{int(value)} р."
        except (ValueError, TypeError):
            return "—"

    row = df.iloc[0]
    model = row.get("Модель", "Неизвестно")
    stock = int(row.get("Нал", 0))
    # Определяем поставщика по новой логике
    row_dict = row.to_dict()
    chosen_supplier, _, _ = choose_best_supplier_for_row(row_dict, None, use_row_sklad=True)
    supplier = chosen_supplier or "N/A"
    opt_price = format_price(row.get("Опт"))
    artikul_alt = row.get(supplier, "")
    rrc_price = format_price(row.get("Цена", None))

    if supplier.lower() == 'sklad':
        try:
            # --- Работа с Google Sheets ---
            gc = gspread.service_account(filename='System/my-python-397519-3688db4697d6.json')
            sh = gc.open("КАЗНА")
            worksheet = sh.worksheet("СКЛАД")
            data = worksheet.get_all_values()
            sklad = pd.DataFrame(data[1:], columns=data[0])

            sklad['Наличие'] = pd.to_numeric(sklad['Наличие'], errors='coerce').fillna(0).astype(int)
            sklad['Арт мой'] = sklad['Арт мой'].astype(str).str.strip()

            matched_rows = sklad[sklad['Арт мой'] == articul]
            if not matched_rows.empty:
                row_index = matched_rows.index[0]
                prev_q = sklad.at[row_index, 'Наличие']
                sklad.at[row_index, 'Наличие'] = max(0, prev_q - quantity)
                new_q = sklad.at[row_index, 'Наличие']

                updated_data = sklad.iloc[:, :8].replace([float('inf'), float('-inf')], 0).fillna(0).values.tolist()
                worksheet.update('A2:H', updated_data, value_input_option='USER_ENTERED')

                telegram.notify(
                    token=telegram_got_token,
                    chat_id=telegram_chat_id,
                    message=(f"✅ *{supplier}:* {prev_q} → {new_q}\n\n"
                             f"Товар: {model}\n"
                             f"Артикул: {articul}\n"
                             f"Опт: {opt_price} | РРЦ: {rrc_price}"),
                    parse_mode='markdown'
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при работе с Google Sheets: {e}")
            telegram.notify(
                token=telegram_got_token, chat_id=telegram_chat_id,
                message=(f"⚠️ Ошибка доступа к Google Sheets для {model} ({articul})"),
                parse_mode='markdown'
            )

    # --- Общая часть: обновляем marketplace_base.db + !YMWB.db ---
    # --- Вычисляем новый остаток ---
    new_stock = max(0, stock - quantity)

    # --- Правило минимального остатка для внешних поставщиков ---
    if supplier.lower() in ('invask', 'okno', 'united'):
        if stock >= 3 and new_stock < 3:
            logger.info(f"⚙️ Остаток {supplier}: {stock} → {new_stock} (<3) → принудительно 0 | {articul}")
            new_stock = 0

    cur = conn.cursor()
    cur.execute(
        "UPDATE marketplace SET Нал = ?, \"Дата изменения\" = ? WHERE Sklad = ?",
        (new_stock, datetime.now().strftime("%d.%m.%Y %H:%M"), articul)
    )
    conn.commit()
    logger.success(f"✅ Остаток обновлён везде: {articul} | {stock} → {new_stock}")

    try:
        alt_db_path = "System/!YMWB.db"
        alt_conn = sqlite3.connect(alt_db_path, timeout=10)
        alt_cur = alt_conn.cursor()

        alt_df = pd.read_sql_query("SELECT rowid, * FROM prices WHERE Артикул = ?", alt_conn, params=(artikul_alt,))
        if not alt_df.empty:
            for _, alt_row in alt_df.iterrows():
                rowid = alt_row["rowid"]
                current_qty = int(alt_row.get("Наличие", 0))
                updated_qty = max(0, current_qty - quantity)

                # --- Правило минимального остатка для Invask / Okno / United ---
                if supplier.lower() in ('invask', 'okno', 'united'):
                    if current_qty >= 3 and updated_qty < 3:
                        logger.info(
                            f"⚙️ !YMWB: {supplier} {current_qty} → {updated_qty} (<3) → принудительно 0 | {artikul_alt}")
                        updated_qty = 0

                alt_cur.execute("UPDATE prices SET Наличие = ? WHERE rowid = ?", (updated_qty, rowid))
                logger.debug(f"🔧 YMWB: {artikul_alt} | {current_qty} → {updated_qty}")
            alt_conn.commit()
        else:
            logger.warning(f"❗ Артикул {artikul_alt} не найден в !YMWB.db")
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении !YMWB.db: {e}")
    finally:
        alt_conn.close()

    if supplier.lower() != 'sklad':
        telegram.notify(
            token=telegram_got_token, chat_id=telegram_chat_id,
            message=(f"✅ *{supplier}:* {stock} → {new_stock}\n\n"
                     f"Товар: {model}\n"
                     f"Артикул: {articul}\n"
                     f"Опт: {opt_price} | РРЦ: {rrc_price}"),
            parse_mode='markdown'
        )

    conn.close()
    return supplier, rrc_price



def get_orders_yandex_market():
    logger.info("📥 Получаем заказы с Yandex.Market...")
    campaign_id = os.getenv('campaign_id')
    ym_token = os.getenv('ym_token')
    url_ym = f'https://api.partner.market.yandex.ru/campaigns/{campaign_id}/orders'
    headers = {"Authorization": f"Bearer {ym_token}"}
    params = {
        "fake": "False",
        "status": "PROCESSING",
        "substatus": "STARTED"
    }
    response = requests.get(url_ym, headers=headers, params=params, timeout=10)
    if response.status_code == 200:
        orders_data = response.json().get('orders', [])  # Получаем список заказов
        logger.success(f"✅ Заказы от Yandex получены: {len(orders_data)} шт.")
        orders_data = response.json().get('orders', [])  # Получаем список заказов из ключа 'orders'
        return orders_data  # Возвращаем список заказов
    else:
        logger.warning(f"⚠ Ошибка при запросе заказов с Yandex: {response.status_code} — {response.text}")
        return []


def get_orders_wildberries():
    logger.info("📥 Получаем заказы с Wildberries...")
    wb_api_token = os.getenv('wb_token')
    url = 'https://marketplace-api.wildberries.ru/api/v3/orders/new'
    headers = {'Authorization': wb_api_token}
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        orders = response.json().get('orders', [])
        logger.success(f"✅ Заказы от WB получены: {len(orders)} шт.")
        orders = response.json().get('orders', [])
        return orders
    else:
        logger.warning(f"⚠ Ошибка при запросе заказов с Wildberries: {response.status_code} — {response.text}")
        return []

def get_orders_ozon():
    url = "https://api-seller.ozon.ru/v3/posting/fbs/unfulfilled/list"
    headers = {
        'Client-Id': os.getenv('ozon_client_ID'),
        'Api-Key': os.getenv('ozon_API_key'),
        'Content-Type': 'application/json'
    }

    # Устанавливаем диапазон времени за последний год
    cutoff_from = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"  # Год назад
    cutoff_to = (datetime.utcnow() + timedelta(days=20)).isoformat() + "Z"

    payload = {
        "dir": "ASC",  # Сортировка по возрастанию
        "filter": {
            "cutoff_from": cutoff_from,  # Начало диапазона времени
            "cutoff_to": cutoff_to,  # Конец диапазона времени
            "status": "awaiting_packaging",  # Пример статуса (необработанные отправления)
            "delivery_method_id": [],  # Опционально
            "provider_id": [],  # Опционально
            "warehouse_id": []  # Опционально
        },
        "limit": 100,  # Максимальное количество возвращаемых элементов за один запрос
        "offset": 0,  # Начальный индекс
        "with": {
            "analytics_data": True,  # Включить аналитические данные
            "barcodes": True,  # Включить штрихкоды
            "financial_data": True,  # Включить финансовые данные
            "translit": True  # Включить транслитерацию
        }
    }
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    if response.status_code == 200:
        # Фильтруем заказы с нужным статусом 'awaiting_packaging'
        orders = response.json().get("result", {}).get("postings", [])
        filtered_orders = [order for order in orders if order.get("status") == "awaiting_packaging"]
        return filtered_orders
    else:
        logger.warning(f"⚠ Ошибка при получении заказов с Ozon: {response.status_code} — {response.text}")
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
            logger.debug(f"✏️ Записан новый ID заказа: {order_id} → {filename}")
            # Если нет, добавляем его в файл
            with open(filename, 'a') as file:
                file.write(str(order_id) + '\n')
            return True
        else:
            # Если ID уже существует в файле, ничего не делаем и возвращаем False
            return False


# Путь к файлу, куда вы хотите записывать ID заказов
file_path = 'System/order_ids.txt'


# Функция, которая берет название товара из файла, когда есть заказ с WB
def get_product(art_mc):
    """Получает название модели из таблицы marketplace по Sklad (например, артикул Wildberries)."""
    db_path = 'System/marketplace_base.db'
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Модель FROM marketplace 
            WHERE [Sklad] = ? AND lower(Маркетплейс) = 'wildberries'
        """, (str(art_mc),))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"❌ Ошибка при получении модели из базы данных: {e}")
        return None
    finally:
        conn.close()


def notify_about_new_orders(orders, platform, supplier):
    if not orders:
        return

    for order in orders:
        order_id = order.get('posting_number') if supplier == 'Ozon' else order.get('id')
        if not write_order_id_to_file(order_id, file_path):
            continue  # заказ уже обработан

        logger.info(f"📦 Новый заказ: {order_id} ({platform})")

        # Получаем номер заказа на сегодня
        order_number = get_today_order_number()

        # Определяем цветовой маркер по площадке
        platform_marker = "🟣" if platform.lower() == "wildberries" else "🟡" if platform.lower() == "yandex" else "🔵"

        # Заголовок нового сообщения
        message = f"*=== {platform} {platform_marker} Заказ № {order_number} ===*\n\n"
        message += f"ID заказа: {order_id}\n"

        items_to_update = []

        if supplier == 'Yandex':
            shipment_date = next(
                (shipment.get('shipmentDate') for shipment in order.get('delivery', {}).get('shipments', [])),
                'Не указано'
            )
            message += f"Дата отгрузки: {shipment_date}\n"

            for item in order.get('items', []):
                offer_id = item.get('offerId', 'Не указан')
                offer_name = item.get('offerName', 'Не указано')
                subsidy_amount = next(
                    (subsidy.get('amount') for subsidy in item.get('subsidies', []) if
                     subsidy.get('type') == 'SUBSIDY'),
                    0
                )
                price = int(item.get('buyerPrice', 0))
                total_price = int(subsidy_amount + price)
                qty = int(item.get('count', 1))

                qty_marker = " 🆘" if qty > 1 else ""
                message += f"Товар: {offer_name}\n"
                message += f"Артикул: {offer_id}\n"
                message += f"Цена: {total_price} р.\n"
                message += f"Кол-во: {qty} шт.{qty_marker}\n"

                items_to_update.append((offer_id, qty))

        elif supplier == 'Wildberries':
            article = order.get('article')
            model = get_product(article)
            price = str(order.get('convertedPrice'))[:-2]
            qty = int(order.get('quantity', 1))

            qty_marker = " 🆘" if qty > 1 else ""
            message += f"Товар: {model}\n"
            message += f"Артикул: {article}\n"
            message += f"Цена: {price} р.\n"
            message += f"Кол-во: {qty} шт.{qty_marker}\n"

            items_to_update.append((article, qty))

        elif supplier == 'Ozon':
            shipment_date_raw = order.get('shipment_date')
            shipment_date = "Не указана"
            if shipment_date_raw:
                try:
                    shipment_date = datetime.strptime(shipment_date_raw, "%Y-%m-%dT%H:%M:%SZ").strftime("%d.%m.%Y")
                except Exception:
                    pass
            message += f"Дата отгрузки: {shipment_date}\n"

            for product in order.get('products', []):
                offer_id = product.get('offer_id')
                product_name = product.get('name', 'Не указано')
                price = int(float(product.get('price', 0)))
                qty = int(product.get('quantity', 1))

                qty_marker = " 🆘" if qty > 1 else ""
                message += f"Товар: {product_name}\n"
                message += f"Артикул: {offer_id}\n"
                message += f"Цена: {price} р.\n"
                message += f"Кол-во: {qty} шт.{qty_marker}\n"

                items_to_update.append((offer_id, qty))

        # 1. Отправляем сообщение о заказе
        telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message=message, parse_mode='markdown')

        # 2. Вычитаем со склада и сохраняем выбранного поставщика
        final_supplier = None
        rrc_price = None

        for offer_id, qty in items_to_update:
            final_supplier, rrc_price = update_stock(offer_id, platform, qty)

        # 3. Передаём поставщика напрямую в Sheets
        write_order_to_gsheets(platform, order_id, items_to_update, rrc_price, final_supplier)

        # --- Сразу после заказа обновляем маркетплейсы ---
        try:
            from stock import gen_sklad, wb_update, ym_update, oz_update

            wb_data, ym_data, oz_data = gen_sklad()

            if wb_data:
                wb_update(wb_data)
            if ym_data:
                ym_update(ym_data)
            if oz_data:
                oz_update(oz_data)

            logger.success("✅ Остатки сразу отправлены на все маркетплейсы")
        except Exception as e:
            logger.error(f"❌ Ошибка при мгновенном обновлении маркетплейсов: {e}")

        # Уведомление о завершении обработки заказа
        telegram.notify(token=telegram_got_token, chat_id=telegram_chat_id, message="📦")


def check_for_new_orders():
    logger.info("🚦 Проверка новых заказов...")
    orders_yandex_market = get_orders_yandex_market()
    notify_about_new_orders(orders_yandex_market, "Yandex", "Yandex")

    orders_wildberries = get_orders_wildberries()
    notify_about_new_orders(orders_wildberries, "Wildberries", "Wildberries")

    orders_ozon = get_orders_ozon()  # Получаем заказы с Ozon
    notify_about_new_orders(orders_ozon, "Ozon", "Ozon")  # Уведомляем о новых заказах с Ozon


# check_for_new_orders()
