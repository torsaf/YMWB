"""
Модуль `auto_stock_updater` обновляет остатки товаров на маркетплейсах (ozon, wildberries, yandex)
на основе данных из внешней базы данных (!YMWB.db → таблица prices).

Основные шаги:
- Загружает остатки и ОПТ из таблицы prices.
- Обновляет таблицы маркетплейсов в локальной базе marketplace_base.db.
- Обнуляет остатки у выключенных товаров или тех, что не найдены в источнике.
- Логирует каждый шаг процесса с помощью loguru.
"""

import sqlite3
from logger_config import logger
from datetime import datetime
import json
import os

def update(flags):
    logger.info("🚀 Начато обновление остатков для маркетплейсов")
    source_conn = sqlite3.connect('System/!YMWB.db', timeout=10)
    target_conn = sqlite3.connect('System/marketplace_base.db', timeout=10)
    logger.debug("🔗 Подключение к базам данных установлено")

    source_cursor = source_conn.cursor()
    source_cursor.execute("""
        SELECT "Поставщик", "Артикул", "Наличие", "ОПТ" FROM prices
    """)
    source_data = source_cursor.fetchall()
    logger.debug(f"📥 Загружено {len(source_data)} записей из источника (prices)")

    source_dict = {
        (supplier.strip(), article.strip()): (stock, opt)
        for supplier, article, stock, opt in source_data
    }

    target_tables = ["ozon", "wildberries", "yandex"]

    for table in target_tables:
        if not flags.get(table, True):
            logger.info(f"⏭ Пропускаем таблицу {table} — отключена в флагах")
            continue

        logger.info(f"🔄 Обновление таблицы {table}")
        target_cursor = target_conn.cursor()
        target_cursor.execute(f"""
            SELECT rowid, "Поставщик", "Артикул", "Статус", "Модель" FROM "{table}"
        """)
        rows = target_cursor.fetchall()

        for rowid, supplier, article, status, model in rows:
            supplier = supplier.strip()
            article = article.strip()
            status = status.strip().lower()
            model = model.strip() if model else "—"
            key = (supplier, article)

            if status == "выкл.":
                target_cursor.execute(f"""SELECT "Нал" FROM "{table}" WHERE rowid = ?""", (rowid,))
                current_stock = target_cursor.fetchone()
                if current_stock and int(current_stock[0]) != 0:
                    logger.debug(f"⛔ {table} | {article} ({model}) — статус 'выкл.' → обнуляем stock")
                    target_cursor.execute(f"""
                        UPDATE "{table}" SET "Нал" = ?, "Дата изменения" = ? WHERE rowid = ?
                    """, (0, datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))
                continue

            if key not in source_dict:
                continue

            stock, opt = source_dict[key]

            price_column = {
                'yandex': 'Цена YM',
                'ozon': 'Цена OZ',
                'wildberries': 'Цена WB'
            }.get(table, 'Цена YM')

            target_cursor.execute(f"""
                SELECT "Нал", "Опт", "{price_column}", "Наценка" FROM "{table}" WHERE rowid = ?
            """, (rowid,))
            current = target_cursor.fetchone()
            if not current:
                continue

            current_stock = int(current[0]) if current[0] is not None else 0
            current_opt = int(current[1]) if current[1] is not None else 0
            current_price = int(current[2]) if current[2] is not None else 0
            markup_raw = str(current[3]).replace('%', '').replace(' ', '') if current[3] else '0'

            try:
                markup = float(markup_raw)
            except:
                markup = 0.0

            try:
                price = round((float(opt) + float(opt) * markup / 100) / 100.0) * 100
            except:
                price = opt

            if (current_stock != stock) or (current_opt != opt) or (current_price != price):
                logger.debug(
                    f"✅ {table} | {article} ({model}) → "
                    f"stock: {current_stock} → {stock}, "
                    f"opt: {current_opt} → {opt}, "
                    f"price: {current_price} → {price}"
                )
                target_cursor.execute(f"""
                    UPDATE "{table}"
                    SET "Нал" = ?, "Опт" = ?, "{price_column}" = ?, "Дата изменения" = ?
                    WHERE rowid = ?
                """, (stock, opt, price, datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))

        target_conn.commit()
        logger.info(f"💾 Обновление таблицы {table} сохранено")

    # 🔍 Обнуляем "Нал" у товаров, которых нет в новой базе, кроме Sklad
    for table in target_tables:
        if not flags.get(table, True):
            continue

        logger.info(f"🧹 Проверка отсутствующих товаров для {table}")
        cursor = target_conn.cursor()

        # Все текущие строки в таблице
        cursor.execute(f"""SELECT rowid, "Поставщик", "Артикул", "Нал", "Статус", "Модель" FROM "{table}" """)
        all_rows = cursor.fetchall()

        for rowid, supplier, article, nal, status, model in all_rows:
            key = (supplier.strip(), article.strip())
            if key not in source_dict and supplier.strip() != "Sklad":
                if str(nal).strip() != "0":
                    logger.debug(f"❌ {table} | {article} ({model}) отсутствует в источнике → Нал = 0")
                    cursor.execute(f"""
                        UPDATE "{table}" SET "Нал" = 0, "Дата изменения" = ? WHERE rowid = ?
                    """, (datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))

        target_conn.commit()


    source_conn.close()
    target_conn.close()
    logger.success("✅ Обновление остатков завершено")

