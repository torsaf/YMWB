"""
Модуль `stock.py` предназначен для обновления остатков товаров на маркетплейсах Wildberries, Yandex.Market и Ozon.

Основные задачи модуля:

1. gen_sklad():
    Извлекает актуальные остатки товаров из базы данных SQLite (`marketplace_base.db`) по каждой площадке:
    - Wildberries: по штрихкодам (`WB Barcode`)
    - Yandex.Market: по артикулам (`Sklad`) с временной меткой
    - Ozon: по артикулам с указанием склада

2. wb_update(wb_data):
    Отправляет остатки на Wildberries через API `PUT /api/v3/stocks/{warehouse_id}`.

3. ym_update(ym_data):
    Обновляет остатки на Yandex.Market через API `PUT /campaigns/{campaign_id}/offers/stocks`.

4. oz_update(oz_data):
    Передаёт остатки в Ozon через API `POST /v2/products/stocks`.

Дополнительно:
- Использует библиотеку `loguru` для логирования всех операций.
- В случае ошибок отправляет уведомления в Telegram через `notifiers`.

Модуль может использоваться как часть автоматического конвейера обновления остатков на всех маркетплейсах.
"""


import sqlite3
import pandas as pd
import requests
import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from notifiers import get_notifier
from logger_config import logger


# Загрузка токенов и переменных окружения
load_dotenv(dotenv_path=os.path.join("System", ".env"))

telegram_got_token_error = os.getenv('telegram_got_token_error')
telegram_chat_id_error = os.getenv('telegram_chat_id_error')
telegram = get_notifier('telegram')

# 🔄 Получение остатков из базы
def gen_sklad():
    logger.info("🚀 Генерация остатков из базы данных")
    DB_PATH = "System/marketplace_base.db"
    conn = sqlite3.connect(DB_PATH, timeout=10)
    logger.debug(f"🔗 Подключено к базе данных: {DB_PATH}")

    wb_final, ym_final, oz_final = [], [], []

    try:
        df = pd.read_sql_query("""
            SELECT Маркетплейс, Sklad, `WB Barcode`, Нал
            FROM marketplace
            WHERE Нал IS NOT NULL
        """, conn)

        logger.success(f"📦 Загружено {len(df)} строк из marketplace")

        current_time = datetime.now(timezone.utc).isoformat()
        warehouse_id = 1020002115578000

        for _, row in df.iterrows():
            mp = (row["Маркетплейс"] or "").lower()
            nal = int(row["Нал"]) if row["Нал"] is not None else 0

            if mp == "wildberries" and row.get("WB Barcode"):
                wb_final.append({
                    "sku": str(row["WB Barcode"]).strip(),
                    "amount": nal
                })
            elif mp == "yandex":
                ym_final.append({
                    "sku": str(row["Sklad"]).strip(),
                    "items": [{"count": nal, "updatedAt": current_time}]
                })
            elif mp == "ozon":
                try:
                    product_id = int(row["Sklad"])  # если не int — упадёт
                except:
                    logger.warning(f"⛔ Некорректный product_id для OZON: {row['Sklad']}")
                    continue

                oz_final.append({
                    "offer_id": str(row["Sklad"]).strip(),
                    "product_id": product_id,
                    "stock": nal,
                    "warehouse_id": warehouse_id
                })

        logger.success(f"✅ Wildberries: {len(wb_final)}, Yandex: {len(ym_final)}, Ozon: {len(oz_final)}")

    except Exception as e:
        logger.error(f"❌ Ошибка при чтении остатков: {e}")
    finally:
        conn.close()

    return wb_final, ym_final, oz_final

# 🚚 Wildberries
def wb_update(wb_data):
    try:
        logger.info(f"📤 Отправка {len(wb_data)} остатков в Wildberries")
        token = os.getenv('wb_token')
        warehouse_id = int(os.getenv('warehouseId'))
        url = f'https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}'
        headers = {'Authorization': token, 'stocks': 'application/json'}
        payload = {'warehouseId': warehouse_id, 'stocks': wb_data}
        response = requests.put(url, headers=headers, json=payload, timeout=10)
        if response.status_code != 204:
            raise Exception(f"Статус-код: {response.status_code}, ответ: {response.text}")
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении Wildberries: {e}")
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error,
                        message=f"😨 Ошибка при обновлении WB: {e}")

# 🚚 Yandex Market
def ym_update(ym_data):
    try:
        logger.info(f"📤 Отправка {len(ym_data)} остатков в Yandex Market")
        token = os.getenv('ym_token')
        campaign_id = os.getenv('campaign_id')
        url = f'https://api.partner.market.yandex.ru/campaigns/{campaign_id}/offers/stocks'
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.put(url, headers=headers, json={"skus": ym_data}, timeout=10)
        if response.status_code != 200:
            raise Exception(f"Статус-код: {response.status_code}, ответ: {response.text}")
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении Yandex: {e}")
        telegram.notify(token=telegram_got_token_error, chat_id=telegram_chat_id_error,
                        message=f"😨 Ошибка при обновлении YM: {e}")

# 🚚 Ozon
def oz_update(oz_data):
    try:
        logger.info(f"📤 Отправка {len(oz_data)} остатков в Ozon")
        client_id = os.getenv('ozon_client_ID')
        api_key = os.getenv('ozon_API_key')
        url = 'https://api-seller.ozon.ru/v2/products/stocks'
        headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
            'Content-Type': 'application/json'
        }

        def chunk_list(data, size=100):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(oz_data, 100):
            payload = {"stocks": chunk}
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code != 200:
                raise Exception(f"Статус-код: {response.status_code}, ответ: {response.text}")
            logger.success(f"✅ Отправлено {len(chunk)} товаров в OZON")

    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении Ozon: {e}")
        telegram.notify(
            token=telegram_got_token_error,
            chat_id=telegram_chat_id_error,
            message=f"😨 Ошибка при обновлении OZON: {e}"
        )

# # 🚀 Запуск
# if __name__ == "__main__":
#     wb, ym, oz = gen_sklad()

#     print("WB:", wb[:3])
#     print("YM:", ym[:3])
#     print("OZ:", oz[:3])
#

#
#     wb_update(wb)
#     ym_update(ym)
#     oz_update(oz)

