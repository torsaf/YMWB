"""
Модуль `auto_stock_updater` обновляет остатки товаров на маркетплейсах (ozon, wildberries, yandex)
на основе данных из внешней базы данных (!YMWB.db → таблица prices).

Основные шаги:
- Загружает остатки и ОПТ из таблицы prices.
- Обновляет таблицы маркетплейсов в локальной базе marketplace_base.db.
- Обнуляет остатки у выключенных товаров или тех, что не найдены в источнике.
- Логирует основные этапы (без избыточных сообщений).
"""

import sqlite3
from logger_config import logger
from datetime import datetime

def update(flags):
    logger.info("🚀 Начато обновление остатков для маркетплейсов")

    # --- Подключение к БД ---
    source_conn = sqlite3.connect('System/!YMWB.db', timeout=10)
    target_conn = sqlite3.connect('System/marketplace_base.db', timeout=10)
    source_cursor = source_conn.cursor()
    cursor = target_conn.cursor()

    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    # --- 1. Обнуление остатков <3 у внешних поставщиков ---
    logger.info("⚙️ Проверка внешних поставщиков (Invask, Okno, United): остаток <3 → 0")
    source_cursor.execute("""
        UPDATE prices
           SET "Наличие" = 0
         WHERE UPPER(TRIM("Поставщик")) IN ('INVASK', 'OKNO', 'UNITED')
           AND CAST("Наличие" AS INTEGER) < 3
    """)
    affected = source_conn.total_changes
    source_conn.commit()
    logger.info(f"🔧 Обнулено {affected} записей в !YMWB.db (остаток <3)")

    # --- 2. Загрузка таблицы prices в память ---
    source_cursor.execute("""SELECT "Поставщик", "Артикул", "Наличие", "ОПТ" FROM prices""")
    source_data = source_cursor.fetchall()
    logger.info(f"📥 Загружено {len(source_data)} строк из таблицы prices")

    source_dict = {}
    for supplier, article, stock, opt in source_data:
        try:
            supplier = str(supplier or "").strip()
            article = str(article or "").strip()
            stock = int(stock or 0)
            opt = int(opt or 0)
            if supplier != "Sklad" and stock < 3:
                stock = 0
            source_dict[(supplier, article)] = (stock, opt)
        except Exception as e:
            logger.warning(f"❌ Ошибка при обработке строки из prices: {e}")

    # --- 3. Основной SELECT по маркетплейсам ---
    cursor.execute("""
        SELECT rowid, Маркетплейс, Sklad, Invask, Okno, United,
               Статус, Модель, Нал, Опт, "%", Цена
        FROM marketplace
    """)
    all_rows = cursor.fetchall()
    total_rows = len(all_rows)

    updated_rows = []
    cleared_rows = []
    updated = 0
    cleared = 0

    supplier_flags = flags.get("suppliers", {}) or {}

    def supplier_enabled(supplier_name: str) -> bool:
        return bool(supplier_flags.get(supplier_name, True))

    # --- 4. Основной цикл ---
    for row in all_rows:
        (rowid, mp, code_sklad, code_invask, code_okno, code_united,
         status, model, old_stock, old_opt, markup, old_price) = row

        mp = (mp or "").strip().lower()
        status = (status or "").strip().lower()
        model = model or "—"

        if not flags.get(mp, True):
            if str(old_stock).strip() != "0":
                cleared_rows.append((0, now_str, rowid))
                cleared += 1
            continue

        # --- выключенные товары ---
        if status == "выкл.":
            if str(old_stock).strip() != "0":
                cleared_rows.append((0, now_str, rowid))
                cleared += 1
            continue

        # --- получение данных из источников ---
        def get_from_source(supplier_name, code_value):
            if not code_value:
                return 0, None

            if not supplier_enabled(supplier_name):
                return 0, None

            return source_dict.get((supplier_name, str(code_value).strip()), (0, None))

        chosen_supplier = ""
        chosen_stock = 0
        chosen_opt = None

        # --- приоритет Sklad ---
        if code_sklad:
            st, opt_val = get_from_source("Sklad", code_sklad)
            if st >= 1:
                chosen_supplier = "Sklad"
                chosen_stock, chosen_opt = st, opt_val

        # --- приоритет Invask/Okno/United ---
        if not chosen_supplier:
            candidates = []
            for sup_name, sup_code in (("Invask", code_invask),
                                       ("Okno", code_okno),
                                       ("United", code_united)):
                st, opt_val = get_from_source(sup_name, sup_code)
                if (st or 0) > 0 and opt_val is not None:
                    candidates.append((sup_name, int(st), float(opt_val)))
            if candidates:
                order = {"Invask": 0, "Okno": 1, "United": 2}
                candidates.sort(key=lambda x: (x[2], order.get(x[0], 9)))
                chosen_supplier, chosen_stock, chosen_opt = candidates[0]

        new_stock = chosen_stock if chosen_supplier else 0
        new_opt = chosen_opt if chosen_opt is not None else old_opt

        # --- пересчёт цены ---
        try:
            markup_val = float(str(markup).replace('%', '').replace(' ', '')) if markup else 0.0
            base_opt = float(str(new_opt).replace(' ', '').replace('р.', '')) if new_opt else 0.0
            new_price = round((base_opt + base_opt * markup_val / 100.0) / 100.0) * 100
        except Exception:
            new_price = 0
            base_opt = 0

        try:
            old_stock = int(old_stock or 0)
            old_opt = int(old_opt or 0)
            old_price = int(old_price or 0)
        except Exception:
            old_stock = old_opt = old_price = 0

        if (old_stock != new_stock) or (old_opt != int(base_opt)) or (old_price != int(new_price)):
            updated_rows.append((new_stock, int(base_opt), int(new_price), now_str, rowid))
            updated += 1

    # --- пакетное обновление обновлённых и обнулённых строк ---
    if updated_rows:
        cursor.executemany("""
            UPDATE marketplace
               SET Нал=?, Опт=?, Цена=?, "Дата изменения"=?
             WHERE rowid=?
        """, updated_rows)

    if cleared_rows:
        cursor.executemany("""
            UPDATE marketplace
               SET Нал=?, "Дата изменения"=?
             WHERE rowid=?
        """, cleared_rows)

    # --- фиксация ---
    target_conn.commit()

    # --- завершение ---
    source_conn.close()
    target_conn.close()

    logger.info(f"📊 Обработано {total_rows} строк | Обновлено: {updated} | Обнулено: {cleared}")
    logger.success("✅ Обновление остатков завершено.")

