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

    # Загружаем остатки и ОПТ из YMWB
    source_cursor = source_conn.cursor()
    source_cursor.execute("""SELECT "Поставщик", "Артикул", "Наличие", "ОПТ" FROM prices""")
    source_data = source_cursor.fetchall()
    logger.debug(f"📥 Загружено {len(source_data)} записей из источника (prices)")

    # Приводим к словарю (поставщик, артикул) → (наличие, опт)
    source_dict = {}
    for supplier, article, stock, opt in source_data:
        try:
            supplier = str(supplier).strip()
            article = str(article).strip()
            stock = int(str(stock).strip())
            opt = int(str(opt).strip())
            if stock < 3:
                stock = 0  # Принудительное обнуление, если меньше 3
            source_dict[(supplier, article)] = (stock, opt)
        except Exception as e:
            logger.warning(f"❌ Ошибка при обработке строки из prices: {e}")

    cursor = target_conn.cursor()
    cursor.execute("""SELECT rowid, Маркетплейс, Поставщик, Артикул, Статус, Модель, Нал, Опт, Наценка FROM marketplace""")
    all_rows = cursor.fetchall()

    updated = 0

    for row in all_rows:
        rowid, mp, supplier, article, status, model, old_stock, old_opt, markup = row
        mp = mp.strip().lower()
        supplier = supplier.strip()
        article = article.strip()
        status = (status or "").strip().lower()
        model = model or "—"

        if not flags.get(mp, True):
            continue

        if not flags.get("suppliers", {}).get(supplier, True):
            continue

        key = (supplier, article)

        # === Обработка выключенных товаров ===
        if status == "выкл.":
            if old_stock != 0:
                logger.debug(f"⛔ {mp} | {article} ({model}) — статус 'выкл.' → обнуляем stock")
                cursor.execute("""
                    UPDATE marketplace SET Нал = 0, "Дата изменения" = ? WHERE rowid = ?
                """, (datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))
            continue

        if key not in source_dict:
            # logger.debug(f"❌ Пропуск {mp} | {supplier} / {article} — отсутствует в source_dict")
            continue

        new_stock, new_opt = source_dict[key]

        try:
            markup = float(str(markup).replace('%', '').replace(' ', '')) if markup else 0.0
        except:
            markup = 0.0

        try:
            new_price = round((new_opt + new_opt * markup / 100) / 100.0) * 100
        except:
            new_price = new_opt

        try:
            old_stock = int(old_stock)
        except:
            old_stock = 0
        try:
            old_opt = int(old_opt)
        except:
            old_opt = 0

        cursor.execute("SELECT Цена FROM marketplace WHERE rowid = ?", (rowid,))
        row_price = cursor.fetchone()
        old_price = int(row_price[0]) if row_price and str(row_price[0]).isdigit() else 0

        if (old_stock != new_stock) or (old_opt != new_opt) or (old_price != new_price):
            logger.debug(
                f"✅ {mp} | {article} ({model}) → "
                f"stock: {old_stock} → {new_stock}, "
                f"opt: {old_opt} → {new_opt}, "
                f"price: {old_price} → {new_price}"
            )
            cursor.execute("""
                UPDATE marketplace
                SET Нал = ?, Опт = ?, Цена = ?, "Дата изменения" = ?
                WHERE rowid = ?
            """, (new_stock, new_opt, new_price, datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))
            updated += 1

    # === Обнуляем "Нал" у отсутствующих в source_dict ===
    logger.info("🧹 Проверка товаров, отсутствующих в source_dict")
    cursor.execute("""SELECT rowid, Поставщик, Артикул, Нал, Статус, Модель FROM marketplace""")
    all_rows = cursor.fetchall()

    for rowid, supplier, article, nal, status, model in all_rows:
        key = (supplier.strip(), article.strip())
        if key not in source_dict and supplier.strip() != "Sklad":
            if str(nal).strip() != "0":
                logger.debug(f"❌ {supplier} / {article} ({model}) нет в прайсе → Нал = 0")
                cursor.execute("""
                    UPDATE marketplace SET Нал = 0, "Дата изменения" = ? WHERE rowid = ?
                """, (datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))

    target_conn.commit()
    source_conn.close()
    target_conn.close()
    logger.success(f"✅ Обновление остатков завершено. Изменено строк: {updated}")
