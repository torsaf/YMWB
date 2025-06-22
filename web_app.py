from flask import Flask, render_template, request, redirect, url_for
from apscheduler.schedulers.background import BackgroundScheduler
from update_sklad import gen_sklad, update_sklad_db
from auto_stock_updater import update
from datetime import datetime
import pandas as pd
from flask import session, Response
from functools import wraps
from dotenv import load_dotenv
import os
import requests
from logger_config import logger, LOG_DIR
from pathlib import Path
from datetime import timedelta
import stock
import json
import sqlite3
import shutil
import glob
from flask import send_file
from copy import deepcopy
from io import BytesIO
from unlisted import generate_unlisted

last_download_time = None
LAST_UPDATE_FILE = "System/last_update.txt"
FLAGS_PATH = "System/stock_flags.json"

# Глобальные флаги доступности (True = показывать остатки, False = всё обнуляется)
global_stock_flags = {
    "yandex": True,
    "ozon": True,
    "wildberries": True
}

def send_telegram_message(message: str):
    stock.telegram.notify(
        token=stock.telegram_got_token_error,
        chat_id=stock.telegram_chat_id_error,
        message=message
    )


def backup_database():
    os.makedirs("System/backups", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    backup_filename = f"System/backups/marketplace_base_{timestamp}.db"

    # Копируем основную базу
    shutil.copy("System/marketplace_base.db", backup_filename)
    logger.info(f"💾 Бэкап базы создан: {backup_filename}")

    # Получаем список всех бэкапов
    backup_files = sorted(
        glob.glob("System/backups/marketplace_base_*.db"),
        key=os.path.getmtime,
        reverse=True
    )

    # Удаляем старые бэкапы, оставляя только последние 14
    for old_file in backup_files[14:]:
        try:
            os.remove(old_file)
            logger.info(f"🗑 Удалён старый бэкап: {old_file}")
        except Exception as e:
            logger.warning(f"❌ Не удалось удалить {old_file}: {e}")

def get_last_download_time():
    if os.path.exists(LAST_UPDATE_FILE):
        with open(LAST_UPDATE_FILE, "r") as f:
            return f.read().strip()
    return None


def update_sklad_task():
    try:
        logger.success("🔁 Обновление склада через update_sklad.py...")
        update(global_stock_flags)
        df = gen_sklad()
        update_sklad_db(df)
        # сохраняем дату
        with open(LAST_UPDATE_FILE, "w") as f:
            f.write(datetime.now().strftime("%d.%m.%Y - %H:%M"))
        logger.success("✅ Склад успешно обновлён.")
        global last_download_time
        last_download_time = datetime.now().strftime("%d.%m.%Y - %H:%M")
    except Exception as e:
        logger.exception("❌ Ошибка при обновлении склада")


def load_stock_flags():
    try:
        with open(FLAGS_PATH, 'r') as f:
            flags = json.load(f)
            if "suppliers" not in flags:
                flags["suppliers"] = {}
            return flags
    except Exception:
        return {
            "yandex": True,
            "ozon": True,
            "wildberries": True,
            "suppliers": {}
        }


global_stock_flags = load_stock_flags()

app = Flask(__name__)
DB_PATH = "System/marketplace_base.db"
load_dotenv(dotenv_path=os.path.join("System", ".env"))
app.secret_key = os.getenv('SECRET_KEY')
USERNAME = "admin"
PASSWORD = os.getenv('PASSWORD')
app.permanent_session_lifetime = timedelta(days=30)


@app.route('/toggle_stock/<market>', methods=['POST'])
def toggle_stock(market):
    if market not in global_stock_flags:
        return '', 400

    global_stock_flags[market] = not global_stock_flags[market]
    with open(FLAGS_PATH, 'w') as f:
        json.dump(global_stock_flags, f)

    logger.info(f"🟡 Переключение {market}: {'ON' if global_stock_flags[market] else 'OFF'}")

    db_path = "System/marketplace_base.db"
    backup_path = "System/temp_stock_backup.db"

    conn = sqlite3.connect(DB_PATH, timeout=10)
    backup_conn = sqlite3.connect(backup_path, timeout=10)
    cur = conn.cursor()
    bcur = backup_conn.cursor()

    # Создаём таблицу в резервной БД, если не существует
    bcur.execute(f"""
        CREATE TABLE IF NOT EXISTS {market}_backup (
            Арт_MC TEXT PRIMARY KEY,
            Нал INTEGER
        )
    """)

    if not global_stock_flags[market]:
        # Сохраняем текущие значения в резервную БД
        cur.execute(f"SELECT Арт_MC, Нал FROM '{market}'")
        data = cur.fetchall()

        bcur.execute(f"DELETE FROM {market}_backup")
        bcur.executemany(f"INSERT INTO {market}_backup (Арт_MC, Нал) VALUES (?, ?)", data)

        # Обнуляем остатки
        cur.execute(f"UPDATE '{market}' SET Нал = 0")

        logger.info(f"📦 Склад {market}: все остатки обнулены и сохранены в резерв.")
    else:
        # Восстанавливаем из резерва
        bcur.execute(f"SELECT Арт_MC, Нал FROM {market}_backup")
        backup_data = bcur.fetchall()

        cur.executemany(f"UPDATE '{market}' SET Нал = ? WHERE Арт_MC = ?", [(n, a) for a, n in backup_data])
        bcur.execute(f"DELETE FROM {market}_backup")

        logger.info(f"🔁 Склад {market}: остатки восстановлены из резерва.")

    conn.commit()
    backup_conn.commit()
    conn.close()
    backup_conn.close()

    return '', 204


@app.route('/run_update')
def run_manual_update():
    try:
        update(global_stock_flags)
        update_sklad_task()
        logger.success("✅ Обновление по кнопке завершено.")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return '', 204
        return redirect(request.referrer or url_for('index'))
    except Exception as e:
        logger.exception("❌ Ошибка при обновлении через кнопку")
        return Response("Ошибка", status=500)


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form['username'] == USERNAME and request.form['password'] == PASSWORD:
            session.permanent = True
            session['logged_in'] = True
            logger.success(f"👤 Успешный вход пользователя {USERNAME}")
            return redirect(url_for('index'))
        else:
            logger.warning("❌ Неверный логин или пароль при попытке входа")
            return "Неверный логин или пароль", 401
    return '''
        <!doctype html>
        <html>
        <head>
            <title>Вход</title>
            <meta charset="utf-8">
            <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
            <style>
                body {
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    background: #f0f2f5;
                    margin: 0;
                    font-family: 'Inter', sans-serif;
                }
                .login-container {
                    background: white;
                    padding: 30px 40px;
                    border-radius: 15px;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                    width: 300px;
                    text-align: center;
                }
                .login-container h2 {
                    margin-bottom: 20px;
                    font-size: 24px;
                }
                .login-container input[type="text"],
                .login-container input[type="password"],
                .login-container input[type="submit"] {
                    width: 100%;
                    padding: 10px;
                    margin-bottom: 15px;
                    border: 1px solid #ccc;
                    border-radius: 8px;
                    font-size: 16px;
                    box-sizing: border-box;
                }
                .login-container input[type="submit"] {
                    background-color: #007bff;
                    color: white;
                    border: none;
                    cursor: pointer;
                    transition: background-color 0.3s;
                }
                .login-container input[type="submit"]:hover {
                    background-color: #0056b3;
                }
            </style>
        </head>
        <body>

        <div class="login-container">
            <h2>Вход</h2>
            <form method="post">
                <input type="text" name="username" placeholder="Логин" required><br>
                <input type="password" name="password" placeholder="Пароль" required><br>
                <input type="submit" value="Войти">
            </form>
        </div>

        </body>
        </html>
        '''

@app.route('/toggle_supplier/<supplier>', methods=['POST'])
def toggle_supplier(supplier):
    global_stock_flags["suppliers"][supplier] = not global_stock_flags["suppliers"].get(supplier, True)

    with open(FLAGS_PATH, 'w') as f:
        json.dump(global_stock_flags, f)

    logger.info(f"🔁 Поставщик {supplier} переключён: {'ON' if global_stock_flags['suppliers'][supplier] else 'OFF'}")

    # Подключение к основным и временным базам
    conn_main = sqlite3.connect(DB_PATH)
    conn_temp = sqlite3.connect("System/temp_stock_backup.db")
    cursor_main = conn_main.cursor()
    cursor_temp = conn_temp.cursor()

    for market in ['yandex', 'ozon', 'wildberries']:
        table_backup = f"backup_supplier_{supplier}_{market}"

        try:
            cursor_main.execute(f"SELECT Арт_MC, Нал FROM '{market}' WHERE Поставщик = ?", (supplier,))
            rows = cursor_main.fetchall()

            if not global_stock_flags["suppliers"][supplier]:
                # Создать временную таблицу, если нет
                cursor_temp.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_backup} (
                        Арт_MC TEXT PRIMARY KEY,
                        Нал INTEGER
                    )
                """)
                # Очистить перед вставкой (перезапись)
                cursor_temp.execute(f"DELETE FROM {table_backup}")

                # Сохранить остатки и обнулить
                for art, nal in rows:
                    cursor_temp.execute(f"""
                        INSERT INTO {table_backup} (Арт_MC, Нал) VALUES (?, ?)
                    """, (art, nal))
                cursor_main.execute(f"""
                    UPDATE '{market}' SET Нал = 0 WHERE Поставщик = ?
                """, (supplier,))
            else:
                for art, _ in rows:
                    cursor_temp.execute(f"SELECT Нал FROM {table_backup} WHERE Арт_MC = ?", (art,))
                    res = cursor_temp.fetchone()
                    if res:
                        nal = res[0]
                        cursor_main.execute(f"""
                            UPDATE '{market}' SET Нал = ? WHERE Арт_MC = ?
                        """, (nal, art))
                        cursor_temp.execute(f"DELETE FROM {table_backup} WHERE Арт_MC = ?", (art,))
        except Exception as e:
            logger.warning(f"❌ Ошибка обработки {supplier} в {market}: {e}")

    conn_main.commit()
    conn_temp.commit()
    conn_main.close()
    conn_temp.close()

    return '', 204

@app.route('/')
@requires_auth
def index():
    return show_table('yandex')


@app.route('/table/<table_name>')
@requires_auth
def show_table(table_name):
    logger.info(f"📊 Открыта таблица: {table_name}")
    sort_column = request.args.get("sort")
    sort_order = request.args.get("order", "asc")  # default: ascending
    last_download_time = get_last_download_time()

    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    if sort_column:
        query = f"SELECT * FROM '{table_name}'"
    else:
        sort_column = "Модель"
        sort_order = "asc"
        query = f"SELECT * FROM '{table_name}'"
    df = pd.read_sql_query(query, conn)
    search_term = request.args.get('search', '').strip().lower()
    if search_term:
        df = df[df.apply(lambda row: any(
            search_term in str(row.get(col, '')).lower()
            for col in ['Арт_MC', 'Поставщик', 'Модель']
        ), axis=1)]
    letter_filter = request.args.get('letter', '').strip().lower()
    if letter_filter:
        if letter_filter == '0-9':
            df = df[df['Модель'].str.match(r'^\d', na=False)]
        elif letter_filter == 'а-я':
            df = df[df['Модель'].str.match(r'^[а-яА-Я]', na=False)]
        else:
            df = df[df['Модель'].str.lower().str.startswith(letter_filter)]
    if '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)
    if all(col in df.columns for col in ['Опт', 'Наценка']):
        def recalc_price(opt, markup):
            try:
                opt = float(str(opt).replace(' ', '').replace('р.', ''))
                markup = float(str(markup).replace('%', '').replace(' ', ''))
                price = int(round((opt + (opt * markup / 100)) / 100.0) * 100)
                return price
            except:
                return opt

        for col in ['Цена YM', 'Цена OZ', 'Цена WB']:
            if col in df.columns:
                df[col] = df.apply(lambda row: recalc_price(row['Опт'], row['Наценка']), axis=1)
    conn.close()

    if sort_column and sort_column in df.columns:
        if sort_column == "Модель":
            df = df.sort_values(by=sort_column, key=lambda x: x.str.lower(), ascending=(sort_order == "asc"))
        else:
            df = df.sort_values(by=sort_column, ascending=(sort_order == "asc"))

    df.insert(0, "№", range(1, len(df) + 1))
    # Если глобальный флаг отключен — принудительно ставим Нал = 0
    if not global_stock_flags.get(table_name, True):
        if 'Нал' in df.columns:
            df['Нал'] = 0
    # 👇 Мини-статистика
    total_rows = len(df)
    in_stock = df[df['Нал'].astype(str).str.replace(r'\D', '', regex=True).astype(float) > 0].shape[0]
    disabled = df[df['Статус'].astype(str).str.lower() == 'выкл.'].shape[0]

    # Выбор нужной колонки с ценой по таблице
    price_col_map = {
        'yandex': 'Цена YM',
        'ozon': 'Цена OZ',
        'wildberries': 'Цена WB'
    }
    price_col = price_col_map.get(table_name.lower(), 'Цена YM')

    def safe_avg(col):
        try:
            return round(
                pd.to_numeric(df[col].astype(str).str.replace(r'\D', '', regex=True), errors='coerce').dropna().mean())
        except:
            return 0

    avg_price = safe_avg(price_col)
    avg_markup = safe_avg('Наценка')

    stats = {
        'Всего товаров': total_rows,
        'В наличии': in_stock,
        'Отключено': disabled,
        f'Средняя цена {price_col.split()[-1]}': f'{avg_price:,} р.'.replace(',', ' '),
        'Средняя наценка': f'{avg_markup} %'
    }

    # 📌 Получаем список всех уникальных поставщиков (фиксированный список, не зависит от df)
    conn_sup = sqlite3.connect(DB_PATH)
    try:
        supplier_df = pd.read_sql_query("SELECT DISTINCT Поставщик FROM yandex", conn_sup)
        suppliers_list = sorted(s for s in supplier_df['Поставщик'].dropna().unique() if s.strip())
    except Exception:
        suppliers_list = []
    conn_sup.close()

    saved_form_data = session.pop('saved_form', {})
    has_errors = has_error_products()

    print("🔥 has_errors =", has_errors)
    logger.debug(f"🔥 has_errors = {has_errors}")
    return render_template(
        "index.html",
        tables=tables,
        table_data=df,
        selected_table=table_name,
        sort_column=sort_column,
        sort_order=sort_order,
        zip=zip,
        stats=stats,
        last_download_time=last_download_time,
        global_stock_flags=global_stock_flags,
        saved_form_data=saved_form_data,
        suppliers_list=suppliers_list,
        has_errors = has_errors
    )


@app.route('/delete/<table>/<item_id>', methods=['POST'])
def delete_row(table, item_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем модель и Арт_MC до удаления
    cursor.execute(f"SELECT Модель, Арт_MC FROM '{table}' WHERE Артикул = ?", (item_id,))
    result = cursor.fetchone()
    model, art_mc = result if result else ("", "")

    cursor.execute(f"DELETE FROM '{table}' WHERE Артикул = ?", (item_id,))
    conn.commit()
    conn.close()

    # Отправляем уведомление
    send_telegram_message(f"🗑 Удалён из {table.upper()}:\n{model} / {art_mc}")
    logger.warning(f"🗑 Удалён товар из {table.upper()}: {model} / {art_mc}")

    return redirect(url_for('show_table', table_name=table, search=''))


@app.route('/update/<table>/<item_id>', methods=['POST'])
def update_row(table, item_id):
    data = request.form.to_dict()
    # Получаем старые данные для сравнения
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM '{table}' WHERE \"Арт_MC\" = ?", (item_id,))
    row = cursor.fetchone()
    column_names = [description[0] for description in cursor.description]
    old_data = dict(zip(column_names, row)) if row else {}

    if not old_data:
        conn.close()
        logger.warning(f"⚠️ Товар с Арт_MC = {item_id} не найден.")
        return '', 400

    if not global_stock_flags.get(table, True):
        logger.info(f"⚙️ Редактирование товара в выключенном маркетплейсе: {table}")
        if 'Нал' in data:
            del data['Нал']

    model = old_data.get("Модель", "—")
    opt_old = int(old_data.get("Опт", 0))
    stock_old = int(old_data.get("Нал", 0))
    price_old = 0

    table_price_column = {
        "yandex": "Цена YM",
        "ozon": "Цена OZ",
        "wildberries": "Цена WB"
    }.get(table, "Цена YM")

    try:
        price_old = int(old_data.get(table_price_column, 0))
    except:
        price_old = 0

    try:
        stock_new = int(data.get("Нал", 0))
        opt_new = int(data.get("Опт", 0))
        markup = float(data.get("Наценка", "0").replace('%', '').replace(' ', ''))
        price_new = round((opt_new + opt_new * markup / 100) / 100.0) * 100
    except Exception as e:
        logger.warning(f"❌ Ошибка при парсинге чисел: {e}")
        stock_new, opt_new, price_new = stock_old, opt_old, price_old

    if (stock_old != stock_new) or (opt_old != opt_new) or (price_old != price_new):
        logger.debug(
            f"✅ {table} | {item_id} ({model}) → "
            f"stock: {stock_old} → {stock_new}, "
            f"opt: {opt_old} → {opt_new}, "
            f"price: {price_old} → {price_new}"
        )
    # Принудительное обнуление "Нал", если статус "выкл."
    if data.get('Статус', '').strip() == 'выкл.':
        data['Нал'] = '0'
    elif not global_stock_flags.get(table, True):
        if 'Нал' in data:
            del data['Нал']

    # Удалить Арт_MC из обновляемых данных
    data.pop("Арт_MC", None)

    try:
        opt = float(data.get('Опт', '0').replace(' ', '').replace('р.', ''))
        markup = float(data.get('Наценка', '0').replace(' ', '').replace('%', ''))
        raw_price = opt + (opt * markup / 100)
        price = int(round(raw_price / 100.0) * 100)
        formatted_price = str(price)

        # Получаем список колонок таблицы
        conn_check = sqlite3.connect(DB_PATH)
        cur_check = conn_check.cursor()
        cur_check.execute(f"PRAGMA table_info('{table}')")
        table_columns = [col[1] for col in cur_check.fetchall()]
        conn_check.close()

        for col in ['Цена YM', 'Цена OZ', 'Цена WB']:
            if col in table_columns:
                data[col] = formatted_price

        if 'Наценка' in data:
            data['Наценка'] = str(int(markup))

    except ValueError:
        logger.warning("❌ Невалидные данные в Опт/Наценка для пересчёта цены.")

    # Ключевые поля, которые влияют на "Дата изменения"
    important_fields = [
        "Поставщик", "Артикул", "Модель", "Статус", "Нал", "Опт", "Наценка", "Комментарий",
        "Цена YM", "Цена OZ", "Цена WB", "WB Артикул", "WB Barcode"
    ]

    # Подготовка к сравнению
    changed = False
    for field in important_fields:
        old_val = str(old_data.get(field, "")).strip()
        new_val = str(data.get(field, "")).strip()
        if field == "Нал" and old_data.get("Статус", "").strip() == "выкл." and old_val == "0" and new_val != old_val:
            # Разрешаем отличия в Нал, если товар выключен и Нал=0 — не считаем изменением
            new_val = "0"
        if old_val != new_val:
            changed = True
            break

    if changed:
        data["Дата изменения"] = datetime.now().strftime("%d.%m.%Y %H:%M")

    columns = list(data.keys())
    values = list(data.values())
    update_clause = ", ".join([f'"{col}" = ?' for col in columns])

    logger.debug(f"🧩 SQL запрос: UPDATE '{table}' SET {update_clause} WHERE \"Арт_MC\" = ?")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE '{table}' SET {update_clause} WHERE \"Арт_MC\" = ?",
            values + [item_id]
        )
        conn.commit()
        logger.debug(f"🧾 Кол-во обновлённых строк: {cursor.rowcount}")
        logger.success("✅ Успешно обновлено!")

        # 🔍 Проверяем смену статуса
        cursor.execute(f"SELECT Статус, Модель FROM '{table}' WHERE Арт_MC = ?", (item_id,))
        row = cursor.fetchone()
        if row:
            new_status, model = row
            old_status = old_data.get("Статус", "").strip().lower()
            new_status = (new_status or "").strip().lower()
            if old_status != new_status:
                action = "🔴 ОТКЛЮЧЕН" if new_status == "выкл." else "🟢 ВКЛЮЧЕН"
                logger.info(f"{action}: {model} ({item_id}) в таблице {table.upper()}")

    except Exception as e:
        logger.exception("❌ Ошибка при обновлении:")
    finally:
        conn.close()

    return '', 204


@app.route('/download_log')
def download_log():
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file_path = LOG_DIR / "app.log"

    if not log_file_path.exists():
        logger.warning("📁 Файл лога не найден для загрузки.")
        return "Файл лога не найден", 404

    return send_file(log_file_path, as_attachment=True)


@app.route('/add/<table_name>', methods=['POST'])
def add_item(table_name):
    from datetime import datetime
    data = request.form.to_dict()

    if 'Комментарий' in data and data['Комментарий'] is None:
        data['Комментарий'] = data.get('Комментарий') or ''

    if not global_stock_flags.get(table_name, True):
        logger.warning(f"⛔ Попытка добавления товара в выключенный маркетплейс: {table_name}")
        return redirect(url_for('show_table', table_name=table_name))

    art_mc = data.get('Арт_MC', '').strip()
    artikul = data.get('Артикул', '').strip()

    if not art_mc or not artikul:
        logger.warning("❌ Не указан Арт_MC или Артикул.")
        return redirect(url_for('show_table', table_name=table_name))

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    model = data.get('Модель', '').strip()
    wb_barcode = data.get('WB Barcode', '').strip()
    wb_artikul = data.get('WB Артикул', '').strip()

    cursor.execute(f"""
        SELECT COUNT(*) FROM '{table_name}'
        WHERE Арт_MC = ?
           OR Артикул = ?
           OR Модель = ?
           OR "WB Barcode" = ?
           OR "WB Артикул" = ?
    """, (art_mc, artikul, model, wb_barcode, wb_artikul))
    existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        conn.close()
        logger.warning("⚠️ Товар с таким Арт_MC, Артикул, Модель, WB Barcode или WB Артикул уже существует.")
        session['saved_form'] = data
        return redirect(url_for('show_table', table_name=table_name, duplicate='1'))

    try:
        opt = float(data.get('Опт', '').replace(' ', '').replace('р.', ''))
        markup = float(data.get('Наценка', '').replace('%', '').replace(' ', ''))
        stock = int(data.get('Нал', '').replace(' ', ''))
        if data.get('Статус', '').strip() == 'выкл.':
            stock = 0

        data['Нал'] = str(stock)
        price_ym = int(round((opt + (opt * markup / 100)) / 100.0) * 100)

        data['Опт'] = str(opt)
        data['Наценка'] = str(int(markup))
        data['Нал'] = str(stock)

        cursor.execute(f"PRAGMA table_info('{table_name}')")
        table_columns = [col[1] for col in cursor.fetchall()]

        if 'Цена YM' in table_columns:
            data['Цена YM'] = str(price_ym)
        if 'Цена OZ' in table_columns:
            data['Цена OZ'] = str(price_ym)
        if 'Цена WB' in table_columns:
            data['Цена WB'] = str(price_ym)

        # Устанавливаем дату изменения только перед вставкой
        data["Дата изменения"] = datetime.now().strftime("%d.%m.%Y %H:%M")

        columns = list(data.keys())
        values = [data[col] for col in columns]
        placeholders = ", ".join(["?"] * len(columns))
        escaped_columns = [f'"{col}"' for col in columns]
        insert_query = f"INSERT INTO '{table_name}' ({', '.join(escaped_columns)}) VALUES ({placeholders})"

        cursor.execute(insert_query, values)
        conn.commit()
        send_telegram_message(f"✅ В {table_name.upper()} добавлен:\n{data.get('Модель', '')} / {art_mc}")
        logger.success(f"✅ Добавлен товар в {table_name.upper()}: {data.get('Модель', '')} / {art_mc}, поставщик: {data.get('Поставщик', '')}")


    except Exception as e:
        logger.exception("❌ Ошибка при добавлении")
    finally:
        conn.close()

    return redirect(url_for('show_table', table_name=table_name, added='1'))



@app.route('/statistic')
@requires_auth
def show_statistic():
    logger.info("📈 Открыта страница статистики")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    tables = ['yandex', 'ozon', 'wildberries']
    data = {}
    supplier_stats = {}

    # === Формирование общей таблицы размещений и статистики по поставщикам ===
    for table in tables:
        df = pd.read_sql_query(f"SELECT Арт_MC, Поставщик, Артикул, Модель, Статус FROM '{table}'", conn)
        for _, row in df.iterrows():
            key = row['Арт_MC']
            supplier = row.get('Поставщик', 'Неизвестно')
            status = (row.get('Статус') or '').strip().lower()

            if key not in data:
                data[key] = {
                    'Арт_MC': key,
                    'Поставщик': supplier,
                    'Артикул': row.get('Артикул', ''),
                    'Модель': row.get('Модель', '')
                }

            mp = table.capitalize()
            data[key][mp] = True
            if status == 'выкл.':
                data[key][f'Статус_{mp}'] = 'выкл.'

            if supplier not in supplier_stats:
                supplier_stats[supplier] = {
                    'Yandex': 0,
                    'Ozon': 0,
                    'Wildberries': 0,
                    'Всего': 0,
                    'Активно': 0,
                    'Неактивно': 0
                }

            supplier_stats[supplier][mp] += 1
            supplier_stats[supplier]['Всего'] += 1
            if status == 'выкл.':
                supplier_stats[supplier]['Неактивно'] += 1
            else:
                supplier_stats[supplier]['Активно'] += 1

    conn.close()

    # === Анализ ошибок между маркетплейсами ===
    errors = detect_errors_across_marketplaces()

    return render_template(
        "statistic.html",
        stats_data=list(data.values()),
        supplier_stats=supplier_stats,
        errors=errors
    )



def has_error_products():
    errors = detect_errors_across_marketplaces()
    return len(errors) > 0

def detect_errors_across_marketplaces():
    conn = sqlite3.connect(DB_PATH)
    dfs = []
    for table in ['yandex', 'ozon', 'wildberries']:
        try:
            df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
            df["Источник"] = table.capitalize()
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Ошибка при чтении таблицы {table}: {e}")
    conn.close()

    if not dfs:
        return []

    all_data = pd.concat(dfs, ignore_index=True)
    all_data = all_data.dropna(subset=["Арт_MC"])
    all_data["Арт_MC"] = all_data["Арт_MC"].astype(str).str.strip()

    duplicates = all_data[all_data.duplicated(subset=["Арт_MC"], keep=False)]
    grouped = duplicates.groupby("Арт_MC")
    logger.debug(f"🔍 Найдено групп с повторяющимся Арт_MC: {len(grouped)}")

    errors = []
    for art_mc, group in grouped:
        nal_values = group["Нал"].astype(str).str.lower().str.strip().tolist()
        status_values = group["Статус"].astype(str).str.lower().str.strip().tolist()
        article_values = group["Артикул"].astype(str).str.lower().str.strip().tolist()
        model_values = group["Модель"].astype(str).str.lower().str.strip().tolist()
        opt_values = group["Опт"].astype(str).str.strip().tolist()
        supplier_values = group["Поставщик"].astype(str).str.lower().str.strip().tolist()

        has_diff = (
            len(set(nal_values)) > 1
            or len(set(status_values)) > 1
            or len(set(article_values)) > 1
            or len(set(model_values)) > 1
            or len(set(opt_values)) > 1
            or len(set(supplier_values)) > 1
        )

        if has_diff:
            diff_flags = {
                "Нал": len(set(nal_values)) > 1,
                "Статус": len(set(status_values)) > 1,
                "Артикул": len(set(article_values)) > 1,
                "Модель": len(set(model_values)) > 1,
                "Опт": len(set(opt_values)) > 1,
                "Поставщик": len(set(supplier_values)) > 1,
            }

            for _, row in group.iterrows():
                errors.append({
                    "Арт_MC": row["Арт_MC"],
                    "Маркетплейс": row["Источник"],
                    "Статус": row.get("Статус", ""),
                    "Нал": row.get("Нал", ""),
                    "Поставщик": row.get("Поставщик", ""),
                    "Артикул": row.get("Артикул", ""),
                    "Модель": row.get("Модель", ""),
                    "Опт": row.get("Опт", ""),
                    "diff": diff_flags
                })
    return errors




@app.errorhandler(Exception)
def handle_error(e):
    logger.exception(f"💥 Ошибка: {str(e)}")
    return "Произошла ошибка на сервере", 500

@app.route('/download_unlisted')
def download_unlisted():
    try:
        df = generate_unlisted()
        if df.empty:
            logger.info("📄 Файл not_listed.xlsx не сформирован: пустой DataFrame.")
            return Response("Нет данных для выгрузки", status=400)

        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Невыложенные товары')
        output.seek(0)

        logger.info("✅ Файл not_listed.xlsx успешно сформирован и отправлен пользователю.")
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='not_listed.xlsx'
        )

    except Exception as e:
        logger.error(f"❌ Ошибка при генерации файла not_listed.xlsx: {e}", exc_info=True)
        return Response("Ошибка сервера при формировании Excel-файла.", status=500)


if __name__ == '__main__':
    if not os.environ.get("WERKZEUG_RUN_MAIN"):  # предотвращает двойной запуск задач
        scheduler = BackgroundScheduler()
        scheduler.add_job(update_sklad_task, 'interval', minutes=5)
        scheduler.add_job(backup_database, 'cron', hour=2)  # каждый день в 2 ночи
        scheduler.start()
        logger.info("📅 Планировщик запущен (обновление склада каждые 5 минут)")
    logger.info("🚀 Приложение запущено")
    app.run(host="127.0.0.1", port=5050, debug=False, use_reloader=False)

