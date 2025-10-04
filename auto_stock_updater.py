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
            if supplier != "Sklad" and stock < 3:
                stock = 0  # Принудительное обнуление, если меньше 3
            source_dict[(supplier, article)] = (stock, opt)
        except Exception as e:
            logger.warning(f"❌ Ошибка при обработке строки из prices: {e}")

    cursor = target_conn.cursor()
    cursor.execute("""
        SELECT rowid, Маркетплейс,
               Sklad, Invask, Okno, United,
               Статус, Модель, Нал, Опт, "%"
        FROM marketplace
    """)
    all_rows = cursor.fetchall()

    updated = 0

    for row in all_rows:
        (rowid, mp,
         code_sklad, code_invask, code_okno, code_united,
         status, model, old_stock, old_opt, markup) = row

        mp = (mp or "").strip().lower()
        status = (status or "").strip().lower()
        model = model or "—"

        if not flags.get(mp, True):
            continue

        # === Обработка выключенных товаров ===
        if status == "выкл.":
            if str(old_stock).strip() != "0":
                logger.debug(f"⛔ {mp} | {model} — статус 'выкл.' → Нал = 0")
                cursor.execute("""
                    UPDATE marketplace
                       SET Нал = 0, "Дата изменения" = ?
                     WHERE rowid = ?
                """, (datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))
            continue

        # --- 1) Приоритет Sklad ---
        chosen_supplier = ""
        chosen_stock = 0
        chosen_opt = None

        def get_from_source(supplier_name, code_value):
            if not code_value:
                return 0, None
            key = (supplier_name, str(code_value).strip())
            return source_dict.get(key, (0, None))

        # Sklad без порога: если Нал ≥ 1 — берём без сравнений
        if code_sklad:
            stock_sklad, opt_sklad = get_from_source("Sklad", code_sklad)
            if stock_sklad >= 1:
                chosen_supplier = "Sklad"
                chosen_stock = int(stock_sklad)
                chosen_opt = opt_sklad

        # --- 2) Иначе — лучший из Invask/Okno/United по минимальному ОПТ при Нал > 0 ---
        if not chosen_supplier:
            candidates = []
            for sup_name, sup_code in (("Invask", code_invask),
                                       ("Okno", code_okno),
                                       ("United", code_united)):
                stock_val, opt_val = get_from_source(sup_name, sup_code)
                if (stock_val or 0) > 0 and (opt_val is not None):
                    candidates.append((sup_name, int(stock_val), float(opt_val)))

            if candidates:
                # минимальный ОПТ; при равенстве — приоритет Invask > Okno > United
                order = {"Invask": 0, "Okno": 1, "United": 2}
                candidates.sort(key=lambda x: (x[2], order.get(x[0], 9)))
                chosen_supplier, chosen_stock, chosen_opt = candidates[0]

        # --- 3) Если никого не нашли — обнуляем Нал, цену не трогаем ---
        if not chosen_supplier:
            new_stock = 0
            new_opt = old_opt
        else:
            new_stock = chosen_stock
            new_opt = chosen_opt if chosen_opt is not None else old_opt

        # Пересчёт цены (как у тебя): округление до сотен
        try:
            markup = float(str(markup).replace('%', '').replace(' ', '')) if markup else 0.0
        except:
            markup = 0.0
        try:
            base_opt = float(str(new_opt).replace(' ', '').replace('р.', '')) if new_opt is not None else 0.0
        except:
            base_opt = 0.0
        new_price = round((base_opt + base_opt * markup / 100.0) / 100.0) * 100

        # Приведение старых значений к числам
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

        if (old_stock != new_stock) or (old_opt != int(base_opt)) or (old_price != int(new_price)):
            logger.debug(
                f"✅ {mp} | {model} → "
                f"stock: {old_stock} → {new_stock}, "
                f"opt: {old_opt} → {int(base_opt)}, "
                f"price: {old_price} → {int(new_price)}"
            )
            cursor.execute("""
                UPDATE marketplace
                   SET Нал = ?, Опт = ?, Цена = ?, "Дата изменения" = ?
                 WHERE rowid = ?
            """, (new_stock, int(base_opt), int(new_price), datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))
            updated += 1

    # === Обнуляем Нал у тех строк, где ни один из поставщиков не даёт наличия >0 ===
    logger.info("🧹 Проверка товаров без доступного поставщика")
    cursor.execute("""
        SELECT rowid, Статус, Модель, Нал, Sklad, Invask, Okno, United
        FROM marketplace
    """)
    for rowid, status, model, nal, code_sklad, code_invask, code_okno, code_united in cursor.fetchall():
        if (status or "").strip().lower() == "выкл.":
            continue
        # есть ли кто-то с Наличием > 0?
        has_any = False
        if code_sklad:
            st, _ = source_dict.get(("Sklad", str(code_sklad).strip()), (0, None))
            if st >= 1:
                has_any = True
        if not has_any:
            for sup, code in (("Invask", code_invask), ("Okno", code_okno), ("United", code_united)):
                if not code: continue
                st, _ = source_dict.get((sup, str(code).strip()), (0, None))
                if (st or 0) > 0:
                    has_any = True
                    break
        if not has_any and str(nal).strip() != "0":
            logger.debug(f"❌ Обнуляем: {model} — ни у одного поставщика нет наличия")
            cursor.execute("""
                UPDATE marketplace
                   SET Нал = 0, "Дата изменения" = ?
                 WHERE rowid = ?
            """, (datetime.now().strftime("%d.%m.%Y %H:%M"), rowid))

    target_conn.commit()
    source_conn.close()
    target_conn.close()
    logger.success(f"✅ Обновление остатков завершено. Изменено строк: {updated}")
