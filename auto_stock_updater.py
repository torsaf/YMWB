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
from loguru import logger
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

        target_cursor = target_conn.cursor()
        target_cursor.execute(f"""
            SELECT rowid, "Поставщик", "Артикул", "Статус" FROM "{table}"
        """)
        rows = target_cursor.fetchall()
        logger.debug(f"📄 Обрабатывается таблица {table}: {len(rows)} строк")

        for rowid, supplier, article, status in rows:
            supplier = supplier.strip()
            article = article.strip()
            status = status.strip().lower()
            key = (supplier, article)

            if status == "выкл.":
                logger.debug(f"🔕 {table} | {supplier} | {article} отключён — stock = 0")
                logger.debug(f"🔕 Отключен товар {supplier} | {article} → stock=0")
                target_cursor.execute(f"""
                    UPDATE "{table}" SET "Нал" = ? WHERE rowid = ?
                """, (0, rowid))
            elif key in source_dict:
                stock, opt = source_dict[key]
                logger.debug(f"✅ Обновление {table} | {supplier} | {article} → stock={stock}, opt={opt}")
                target_cursor.execute(f"""
                    UPDATE "{table}" SET "Нал" = ?, "Опт" = ? WHERE rowid = ?
                """, (stock, opt, rowid))
            else:
                if supplier != "Sklad":
                    logger.debug(f"❓ {table} | {supplier} | {article} не найден в источнике — stock = 0")
                    target_cursor.execute(f"""
                        UPDATE "{table}" SET "Нал" = 0 WHERE rowid = ?
                    """, (rowid,))

        target_conn.commit()
        logger.info(f"💾 Обновление таблицы {table} сохранено")

    source_conn.close()
    target_conn.close()
    logger.success("✅ Обновление остатков завершено")
