"""
Модуль `update_sklad` предназначен для синхронизации остатков товаров между Google Sheets и локальной базой данных SQLite.

Основные функции:

- gen_sklad():
    Загружает данные из листа "СКЛАД" Google Sheets, фильтрует по поставщику "SKL" и статусу "На складе",
    очищает и преобразует данные в DataFrame с колонками: "Арт мой", "Модель", "Наличие", "ОПТ".

- update_sklad_db(sklad_df):
    Обновляет таблицы маркетплейсов ("ozon", "wildberries", "yandex") в базе данных SQLite на основе данных из склада.
    Учитывает флаги обновления из файла `System/stock_flags.json`.
    Для товаров с поставщиком "Sklad":
        - Если статус "выкл.": устанавливает остаток 0.
        - Если артикул найден в складе: обновляет остаток и оптовую цену.
        - Если артикул не найден: устанавливает остаток 0.

Логирование:
    Используется библиотека `loguru` для логирования всех этапов работы модуля, включая:
        - Начало и завершение операций.
        - Количество загруженных и обработанных строк.
        - Обновление остатков и оптовых цен.
        - Пропущенные или отключенные таблицы и товары.
        - Ошибки при загрузке флагов или работе с базой данных.
"""


from loguru import logger
import pandas as pd
import gspread
import sqlite3
import json



def gen_sklad():
    logger.info("🚀 Генерация данных со склада (Google Sheets)")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    gc = gspread.service_account(filename='System/my-python-397519-3688db4697d6.json')
    sh = gc.open("КАЗНА")
    worksheet = sh.worksheet("СКЛАД")
    data = worksheet.get('A:W')
    logger.debug(f"📥 Получено {len(data) - 1} строк (без заголовка)")

    # Удаляем пустые строки и пробелы
    data = [row for row in data if any(cell.strip() for cell in row)]
    data = [[cell.strip() for cell in row] for row in data]

    # Оставляем строки, Статус = На складе
    filtered_data = [
        row for row in data[1:]
        if len(row) >= 2 and row[0] == "На складе"
    ]
    logger.debug(f"✅ Отфильтровано строк 'На складе': {len(filtered_data)}")

    # Преобразуем в DataFrame и оставляем нужные столбцы
    columns = data[0]
    sklad = pd.DataFrame(filtered_data, columns=columns)
    sklad = sklad[["Арт мой", "Модель", "Наличие", "ОПТ"]]

    # Очистка и преобразование числовых столбцов
    sklad["Наличие"] = sklad["Наличие"].replace(r"[^\d]", "", regex=True).astype(int)
    sklad["ОПТ"] = sklad["ОПТ"].replace(r"[^\d]", "", regex=True).astype(int)

    logger.success("📦 Складовые данные подготовлены")
    return sklad


def update_sklad_db(sklad_df):
    logger.info("🚀 Начато обновление остатков из склада в базу данных")
    # Загрузим актуальные флаги
    try:
        with open("System/stock_flags.json", "r", encoding="utf-8") as f:
            flags = json.load(f)
            logger.debug(f"⚙️ Загружены флаги обновления: {flags}")
    except:
        flags = {"yandex": True, "ozon": True, "wildberries": True}

    conn = sqlite3.connect("System/marketplace_base.db", timeout=10)
    cursor = conn.cursor()

    # Словарь: Арт мой → (Наличие, ОПТ)
    sklad_dict = {
        str(row["Арт мой"]): (int(row["Наличие"]), int(row["ОПТ"]))
        for _, row in sklad_df.iterrows()
    }
    logger.debug(f"📦 Подготовлено {len(sklad_dict)} записей из склада для обновления")

    target_tables = ["ozon", "wildberries", "yandex"]

    for table in target_tables:
        if not flags.get(table, True):
            logger.info(f"⛔ Таблица {table} отключена — пропущена")
            continue

        logger.info(f"🔄 Обновление таблицы {table}")
        cursor.execute(f"""
            SELECT rowid, "Поставщик", "Арт_MC", "Статус" FROM "{table}"
        """)
        rows = cursor.fetchall()

        for rowid, supplier, art_mc, status in rows:
            supplier = supplier.strip()
            art_mc_str = str(art_mc).strip()
            status = (status or "").strip().lower()

            if supplier == "Sklad":
                logger.debug(f"🔕 {table} | {art_mc_str} отключён — stock = 0")
                if status == "выкл.":
                    cursor.execute(f"""
                        UPDATE "{table}"
                        SET "Нал" = ?
                        WHERE rowid = ?
                    """, (0, rowid))
                elif art_mc_str in sklad_dict:
                    nal, opt = sklad_dict[art_mc_str]
                    logger.debug(f"✅ {table} | {art_mc_str} обновлён → stock = {nal}, opt = {opt}")
                    cursor.execute(f"SELECT Наценка FROM '{table}' WHERE rowid = ?", (rowid,))
                    markup_result = cursor.fetchone()
                    try:
                        markup = float(str(markup_result[0]).replace('%', '').replace(' ', '')) if markup_result and \
                                                                                                   markup_result[
                                                                                                       0] else 0.0
                    except:
                        markup = 0.0

                    # пересчитаем цену
                    try:
                        price = round((opt + opt * markup / 100) / 100.0) * 100
                    except:
                        price = opt

                    # определим колонку для цены
                    price_column = {
                        "yandex": "Цена YM",
                        "ozon": "Цена OZ",
                        "wildberries": "Цена WB"
                    }.get(table, "Цена YM")

                    # обновим также цену
                    cursor.execute(f"""
                        UPDATE "{table}"
                        SET "Нал" = ?, "Опт" = ?, "{price_column}" = ?
                        WHERE rowid = ?
                    """, (nal, opt, price, rowid))
                else:
                    logger.debug(f"❓ {table} | {art_mc_str} не найден в складе — stock = 0")
                    cursor.execute(f"""
                        UPDATE "{table}"
                        SET "Нал" = ?
                        WHERE rowid = ?
                    """, (0, rowid))


        conn.commit()

    conn.close()
    logger.success("✅ Обновление остатков со склада завершено")


gen_sklad()