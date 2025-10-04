from flask import Flask, render_template, request, redirect, url_for, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from update_sklad import gen_sklad, update_sklad_db, upsert_ymwb_prices_from_sklad
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
from threading import Lock
from flask import send_file
from copy import deepcopy
from io import BytesIO
from unlisted import generate_unlisted
from ozon_actions import remove_all_products_from_all_actions


last_download_time = None
LAST_UPDATE_FILE = "System/last_update.txt"
FLAGS_PATH = "System/stock_flags.json"

# Глобальные флаги доступности (True = показывать остатки, False = всё обнуляется)
global_stock_flags = {
    "yandex": True,
    "ozon": True,
    "wildberries": True
}
toggle_lock = Lock()

def send_telegram_message(message: str):
    stock.telegram.notify(
        token=stock.telegram_got_token_error,
        chat_id=stock.telegram_chat_id_error,
        message=message
    )


def disable_invask_if_needed():
    try:
        current = global_stock_flags.get("suppliers", {}).get("Invask", True)
        if current:
            logger.info("⏱️ CRON: Пятница — Invask сейчас ON → выключаем (toggle)")
            requests.post("http://127.0.0.1:5050/toggle_supplier/Invask")
        else:
            logger.info("⏱️ CRON: Пятница — Invask уже OFF → ничего не делаем")
    except Exception as e:
        logger.warning(f"❌ CRON: Ошибка при отключении Invask: {e}")


def enable_invask_if_needed():
    try:
        current = global_stock_flags.get("suppliers", {}).get("Invask", True)
        if not current:
            logger.info("⏱️ CRON: Воскресенье — Invask сейчас OFF → включаем (toggle)")
            requests.post("http://127.0.0.1:5050/toggle_supplier/Invask")
        else:
            logger.info("⏱️ CRON: Воскресенье — Invask уже ON → ничего не делаем")
    except Exception as e:
        logger.warning(f"❌ CRON: Ошибка при включении Invask: {e}")

def disable_okno_if_needed():
    try:
        current = global_stock_flags.get("suppliers", {}).get("Okno", True)
        if current:
            logger.info("⏱️ CRON: Пятница — Okno сейчас ON → выключаем (toggle)")
            requests.post("http://127.0.0.1:5050/toggle_supplier/Okno")
        else:
            logger.info("⏱️ CRON: Пятница — Okno уже OFF → ничего не делаем")
    except Exception as e:
        logger.warning(f"❌ CRON: Ошибка при отключении Okno: {e}")

def enable_okno_if_needed():
    try:
        current = global_stock_flags.get("suppliers", {}).get("Okno", True)
        if not current:
            logger.info("⏱️ CRON: Воскресенье — Okno сейчас OFF → включаем (toggle)")
            requests.post("http://127.0.0.1:5050/toggle_supplier/Okno")
        else:
            logger.info("⏱️ CRON: Воскресенье — Okno уже ON → ничего не делаем")
    except Exception as e:
        logger.warning(f"❌ CRON: Ошибка при включении Okno: {e}")


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
        with toggle_lock:
            logger.success("🔁 Обновление склада через update_sklad.py...")

            # 1) Тянем свежие данные склада
            df = gen_sklad()

            # 2) Синхронизируем !YMWB.db/prices (Sklad): UPDATE/INSERT + DELETE отсутствующих
            upsert_ymwb_prices_from_sklad(df)

            # 3) Обновляем marketplace_base.db из склада (Нал/ОПТ/Цена)
            update_sklad_db(df)

            # 4) Полный пересчёт выбора поставщика по всей базе (уже по обновлённому !YMWB.db)
            update(global_stock_flags)

            # 5) Точечный пересчёт для каждой таблицы МП (оставляем, как у тебя)
            for _mp in ('yandex', 'ozon', 'wildberries'):
                try:
                    recompute_marketplace_core(_mp)
                except Exception as e:
                    logger.warning(f"❌ Ошибка пересчёта для {_mp}: {e}")

            # сохраняем дату ...

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


SUPPLIERS = ['Invask', 'Okno', 'United']  # приоритет на равных ценах: Invask > Okno > United (можно поменять)

# Кэшируем авто-обнаруженную таблицу с колонками Поставщик/Артикул/Наличие/ОПТ
_SUP_TBL_CACHE = None
_SUP_DB_PATH = "System/!YMWB.db"

def _detect_suppliers_table(conn) -> str | None:
    global _SUP_TBL_CACHE
    if _SUP_TBL_CACHE:
        return _SUP_TBL_CACHE
    try:
        cur = conn.cursor()
        # 1) Пробуем стандартные имена
        for name in ("prices", "stocks"):
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
            if cur.fetchone():
                # Проверим, что есть нужные колонки
                cur.execute(f'PRAGMA table_info("{name}")')
                cols = {r[1] for r in cur.fetchall()}
                need = {"Поставщик", "Артикул", "Наличие", "ОПТ"}
                if need.issubset(cols):
                    _SUP_TBL_CACHE = name
                    logger.info(f"📦 Используем таблицу остатков: {name}")
                    return name
        # 2) Ищем любую подходящую
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for (tname,) in cur.fetchall():
            cur.execute(f'PRAGMA table_info("{tname}")')
            cols = {r[1] for r in cur.fetchall()}
            if {"Поставщик", "Артикул", "Наличие", "ОПТ"}.issubset(cols):
                _SUP_TBL_CACHE = tname
                logger.info(f"📦 Используем таблицу остатков: {tname}")
                return tname
    except Exception as e:
        logger.warning(f"❌ Не удалось определить таблицу остатков в !YMWB.db: {e}")
    return None

def _fetch_stock_for(conn_unused, supplier: str, code: str):
    if not code or not str(code).strip():
        return 0, None
    try:
        sup_conn = sqlite3.connect("System/!YMWB.db", timeout=5)
        cur = sup_conn.cursor()
        # Без учета регистра по Поставщику + убираем пробелы и ведущие нули в Артикуле
        cur.execute("""
            SELECT COALESCE("Наличие",0), "ОПТ"
              FROM prices
             WHERE UPPER(TRIM("Поставщик")) = UPPER(?)
               AND (
                    REPLACE(REPLACE(REPLACE(CAST("Артикул" AS TEXT), ' ', ''), ' ', ''), CHAR(9), '') 
                        = REPLACE(REPLACE(REPLACE(?, ' ', ''), ' ', ''), CHAR(9), '')
                 OR REPLACE(REPLACE(REPLACE(LTRIM(CAST("Артикул" AS TEXT), '0'), ' ', ''), ' ', ''), CHAR(9), '')
                        = REPLACE(REPLACE(REPLACE(LTRIM(?, '0'), ' ', ''), ' ', ''), CHAR(9), '')
               )
             LIMIT 1
        """, (supplier, str(code).strip(), str(code).strip()))
        row = cur.fetchone()
    except Exception as e:
        logger.warning(f"❌ SUPPLIERS_DB read failed: {e}")
        row = None
    finally:
        try: sup_conn.close()
        except: pass

    if not row:
        return 0, None

    nal, opt = row
    try: nal = int(str(nal).strip() or 0)
    except: nal = 0
    try: opt = float(str(opt).replace(' ', '').replace('р.', '')) if opt is not None else None
    except: opt = None
    return nal, opt

def choose_best_supplier_for_row(row: dict, conn, use_row_sklad: bool = True) -> tuple[str, int, float]:
    """
    Вход: row — dict одной строки marketplace (с полями Sklad, Invask, Okno, United, ...).
    Выход: (chosen_supplier, nal, opt)
      - Если у Sklad nal >= 1 — возвращаем Sklad без сравнений.
      - Иначе ищем среди Invask/Okno/United варианты с nal > 0 и берём минимальный opt.
      - При равных opt — приоритет по порядку SUPPLIERS.
      - Если кандидатов нет — возвращаем ('', 0, None).
    """

    sklad_code = str(row.get('Sklad') or '').strip()
    sklad_enabled = global_stock_flags.get("suppliers", {}).get("Sklad", True)

    if sklad_code and sklad_enabled:
        # Остаток склада берём только из !YMWB.db — чтобы не путать с агрегатным "Нал" строки
        nal_ext, opt_ext = _fetch_stock_for(conn, 'Sklad', sklad_code)
        if nal_ext >= 1:
            return 'Sklad', nal_ext, opt_ext

    # 2) Кандидаты из остальных
    best = ('', 0, None)  # (supplier, nal, opt)
    for sup in SUPPLIERS:
        sup_code = str(row.get(sup) or '').strip()
        if not sup_code:
            continue
        # пропускаем отключённых поставщиков
        if not global_stock_flags.get("suppliers", {}).get(sup, True):
            continue
        nal, opt = _fetch_stock_for(conn, sup, sup_code)
        if nal and nal > 0 and opt is not None:
            if best[0] == '':
                best = (sup, nal, opt)
            else:
                # сравнение по opt; при равенстве — по приоритету SUPPLIERS
                if opt < best[2]:
                    best = (sup, nal, opt)
                elif opt == best[2]:
                    if SUPPLIERS.index(sup) < SUPPLIERS.index(best[0]):
                        best = (sup, nal, opt)

    return best

def _calc_price(opt_value, markup_raw):
    try:
        opt = float(str(opt_value).replace(' ', '').replace('р.', ''))
        markup = float(str(markup_raw).replace('%', '').replace(' ', ''))
        return int(round((opt + opt * markup / 100.0) / 100.0) * 100)
    except:
        return None

global_stock_flags = load_stock_flags()

app = Flask(__name__)
DB_PATH = "System/marketplace_base.db"
load_dotenv(dotenv_path=os.path.join("System", ".env"))
app.secret_key = os.getenv('SECRET_KEY')
USERNAME = "admin"
PASSWORD = os.getenv('PASSWORD')
app.permanent_session_lifetime = timedelta(days=30)

@app.route('/favicon.ico')
def favicon():
    from flask import send_from_directory
    import os
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico',
        mimetype='image/x-icon'
    )

@app.route('/toggle_stock/<market>', methods=['POST', 'GET'])
def toggle_stock(market):
    try:
        with toggle_lock:
            if market not in global_stock_flags:
                return jsonify({"status": "error", "message": "unknown market"}), 400

            # Переключаем состояние (ON/OFF)
            global_stock_flags[market] = not global_stock_flags[market]
            with open(FLAGS_PATH, 'w') as f:
                json.dump(global_stock_flags, f)

            state = "ON" if global_stock_flags[market] else "OFF"
            logger.info(f"🟡 Переключение {market}: {state}")

            conn_main = sqlite3.connect(DB_PATH, timeout=10)
            cur = conn_main.cursor()
            conn_backup = sqlite3.connect("System/temp_stock_backup.db", timeout=10)
            bcur = conn_backup.cursor()

            if not global_stock_flags[market]:
                bcur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {market}_backup (
                        Маркетплейс TEXT,
                        Sklad TEXT,
                        Нал INTEGER,
                        PRIMARY KEY (Маркетплейс, Sklad)
                    )
                """)
                bcur.execute(f"DELETE FROM {market}_backup")

                cur.execute("""
                    SELECT Sklad, Нал
                      FROM marketplace
                     WHERE LOWER(TRIM("Маркетплейс")) = LOWER(TRIM(?))
                """, (market.lower(),))
                data = cur.fetchall()

                bcur.executemany(
                    f"INSERT INTO {market}_backup (Маркетплейс, Sklad, Нал) VALUES (?, ?, ?)",
                    [(market, art, nal) for art, nal in data]
                )

                cur.execute("""
                    UPDATE marketplace
                       SET Нал = 0
                     WHERE LOWER(TRIM("Маркетплейс")) = LOWER(TRIM(?))
                """, (market.lower(),))
                logger.info(f"📦 {market}: сохранено {len(data)} строк, остатки обнулены")
            else:
                bcur.execute(f"""
                    SELECT Sklad, Нал
                      FROM {market}_backup
                     WHERE LOWER(TRIM("Маркетплейс")) = LOWER(TRIM(?))
                """, (market.lower(),))
                backup_data = bcur.fetchall()

                for art, nal in backup_data:
                    cur.execute("""
                        UPDATE marketplace
                           SET Нал = ?
                         WHERE LOWER(TRIM("Маркетплейс")) = LOWER(TRIM(?))
                           AND TRIM(COALESCE(Sklad,'')) = TRIM(?)
                    """, (nal, market.lower(), art))

                bcur.execute(f"DELETE FROM {market}_backup")
                logger.info(f"🔁 {market}: восстановлено {len(backup_data)} строк")

            conn_main.commit()
            conn_backup.commit()
            conn_main.close()
            conn_backup.close()

            try:
                recompute_marketplace_core(market)
                logger.info(f"🔄 Пересчёт выполнен для {market}")
            except Exception as e:
                logger.error(f"❌ Ошибка пересчёта для {market}: {e}")

            # ✅ ВСЕГДА JSON
            return jsonify({
                "status": "ok",
                "market": market,
                "enabled": global_stock_flags[market]
            }), 200
    except Exception as e:
        logger.exception("❌ toggle_stock failed")
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route('/run_update')
def run_manual_update():
    try:
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

@app.route('/toggle_supplier/<supplier>', methods=['POST', 'GET'])
def toggle_supplier(supplier):
    try:
        with toggle_lock:  # последовательное выполнение
            # Переключаем флаг в JSON
            global_stock_flags["suppliers"][supplier] = not global_stock_flags["suppliers"].get(supplier, True)
            with open(FLAGS_PATH, 'w') as f:
                json.dump(global_stock_flags, f)
            logger.info(f"🔁 Поставщик {supplier} переключён: {'ON' if global_stock_flags['suppliers'][supplier] else 'OFF'}")

            # Маппим имя поставщика -> имя колонки с его кодом
            col_by_supplier = {"Invask": "Invask", "Okno": "Okno", "United": "United", "Sklad": None}
            col = col_by_supplier.get(supplier)
            if supplier not in col_by_supplier:
                return jsonify({"status": "error", "message": "unknown supplier"}), 400

            # Условие отбора строк
            if supplier == "Sklad":
                where_clause = """
                    COALESCE(Invask,'')='' AND COALESCE(Okno,'')='' AND COALESCE(United,'')='' 
                    AND TRIM(COALESCE(Sklad,''))<>'' AND Маркетплейс = ?
                """
            else:
                where_clause = f"{col} IS NOT NULL AND TRIM({col}) <> '' AND Маркетплейс = ?"

            conn_main = sqlite3.connect(DB_PATH, timeout=10)
            conn_temp = sqlite3.connect("System/temp_stock_backup.db", timeout=10)
            cursor_main = conn_main.cursor()
            cursor_temp = conn_temp.cursor()

            for market in ['yandex', 'ozon', 'wildberries']:
                table_backup = f"backup_supplier_{supplier}_{market}"
                try:
                    cursor_main.execute(f"SELECT Sklad, Нал FROM marketplace WHERE {where_clause}", (market,))
                    rows = cursor_main.fetchall()

                    if not global_stock_flags["suppliers"][supplier]:
                        cursor_temp.execute(f"""
                            CREATE TABLE IF NOT EXISTS {table_backup} (
                                Sklad TEXT PRIMARY KEY,
                                Нал INTEGER
                            )
                        """)
                        cursor_temp.execute(f"DELETE FROM {table_backup}")
                        for art, nal in rows:
                            cursor_temp.execute(
                                f"INSERT INTO {table_backup} (Sklad, Нал) VALUES (?, ?)",
                                (art, nal)
                            )
                        cursor_main.execute(f"UPDATE marketplace SET Нал = 0 WHERE {where_clause}", (market,))
                    else:
                        for art, _ in rows:
                            cursor_temp.execute(
                                f"SELECT Нал FROM {table_backup} WHERE Sklad = ?",
                                (art,)
                            )
                            res = cursor_temp.fetchone()
                            if res:
                                nal = res[0]
                                cursor_main.execute("""
                                    UPDATE marketplace
                                       SET Нал = ?
                                     WHERE Sklad = ? AND Маркетплейс = ?
                                """, (nal, art, market))
                                cursor_temp.execute(
                                    f"DELETE FROM {table_backup} WHERE Sklad = ?",
                                    (art,)
                                )
                except Exception as e:
                    logger.warning(f"❌ Ошибка обработки {supplier} в {market}: {e}")

            conn_main.commit()
            conn_temp.commit()
            conn_main.close()
            conn_temp.close()

            # Пересчёт по всем МП
            for market in ['yandex', 'ozon', 'wildberries']:
                try:
                    recompute_marketplace_core(market)
                except Exception as e:
                    logger.error(f"❌ Ошибка пересчёта для {market}: {e}")

            # ✅ ВСЕГДА JSON
            return jsonify({
                "status": "ok",
                "supplier": supplier,
                "enabled": global_stock_flags["suppliers"][supplier]
            }), 200
    except Exception as e:
        logger.exception("❌ toggle_supplier failed")
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route('/')
@requires_auth
def index():
    return show_table('yandex')


@app.route('/table/<table_name>')
@requires_auth
def show_table(table_name):
    logger.info(f"📊 Открыта таблица: {table_name}")
    sort_column = request.args.get("sort")
    sort_order = request.args.get("order")  # None, если параметра нет

    if not sort_column:
        # дефолтная сортировка по Модели
        sort_column = "Модель"
        sort_order = "asc"
    elif sort_column == "Нал" and sort_order is None:
        # 👇 для "Нал" первый клик = desc
        sort_order = "desc"
    elif sort_order is None:
        sort_order = "asc"
    last_download_time = get_last_download_time()

    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]


    query = "SELECT * FROM marketplace WHERE Маркетплейс = ?"
    df = pd.read_sql_query(query, conn, params=(table_name,))
    if "Маркетплейс" in df.columns:
        df.drop(columns=["Маркетплейс"], inplace=True)
    # Желаемый порядок колонок (WB — отдельный порядок)
    if table_name == "wildberries":
        preferred = [
            'Sklad', 'Invask', 'Okno', 'United',
            'WB Barcode', 'WB Артикул',
            'Модель',
            'Статус', 'Нал', 'Опт', '%', 'Цена',
            'Комментарий', 'Дата изменения'
        ]
    else:
        preferred = [
            'Sklad', 'Invask', 'Okno', 'United',
            'Модель',
            'Статус', 'Нал', 'Опт', '%', 'Цена',
            'Комментарий', 'Дата изменения'
        ]

    preferred = [c for c in preferred if c in df.columns]
    others = [c for c in df.columns if c not in preferred]
    df = df[preferred + others]
    search_term = request.args.get('search', '').strip().lower()
    if search_term:
        df = df[df.apply(lambda row: any(
            search_term in str(row.get(col, '')).lower()
            for col in ['Sklad', 'Invask', 'Okno', 'United', 'Модель']
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
    # Преобразуем "Дата изменения" в datetime для правильной сортировки
    if "Дата изменения" in df.columns:
        try:
            df["Дата изменения"] = pd.to_datetime(df["Дата изменения"], format="%d.%m.%Y %H:%M", errors="coerce")
        except Exception as e:
            logger.warning(f"❌ Ошибка преобразования дат: {e}")
    if all(col in df.columns for col in ['Опт', '%', 'Цена', 'Нал']):
        def recalc_price(opt, markup):
            try:
                opt = float(str(opt).replace(' ', '').replace('р.', ''))
                markup = float(str(markup).replace('%', '').replace(' ', ''))
                return int(round((opt + (opt * markup / 100)) / 100.0) * 100)
            except:
                return None  # не трогаем, если не смогли посчитать

        mask = pd.to_numeric(df['Нал'], errors='coerce').fillna(0) > 0
        df.loc[mask, 'Цена'] = df.loc[mask].apply(
            lambda row: recalc_price(row['Опт'], row['%']), axis=1
        ).fillna(df.loc[mask, 'Цена'])
    conn.close()

    if sort_column and sort_column in df.columns:
        # 👇 Добавляем признак "выключен"
        df['_disabled_flag'] = df['Статус'].astype(str).str.lower().eq('выкл.').astype(int)

        # 👇 Особая логика для колонок поставщиков
        if sort_column in ["Sklad", "Invask", "Okno", "United"]:
            def highlight_sort(row):
                active_supplier = choose_best_supplier_for_row(row.to_dict(), None, use_row_sklad=True)[0]
                return 1 if active_supplier == sort_column else 0

            df['_highlight_sort'] = df.apply(highlight_sort, axis=1)
            # 🔑 по умолчанию (asc) цветные сверху
            df = df.sort_values(
                by=['_disabled_flag', '_highlight_sort'],
                ascending=[True, False if sort_order == "asc" else True]
            )
            df.drop(columns=['_highlight_sort'], inplace=True)

        # 👇 Обычная сортировка для остальных колонок
        elif sort_column == "Модель":
            df = df.sort_values(
                by=['_disabled_flag', sort_column],
                key=lambda x: x.str.lower() if x.name == sort_column else x,
                ascending=[True, sort_order == "asc"]
            )
        else:
            df = df.sort_values(
                by=['_disabled_flag', sort_column],
                ascending=[True, sort_order == "asc"]
            )

        # убираем временный флаг
        df.drop(columns=['_disabled_flag'], inplace=True)

    df.insert(0, "№", range(1, len(df) + 1))
    # Удаляем лишние столбцы для Yandex и Ozon
    if table_name != "wildberries":
        for col in ["WB Barcode", "WB Артикул"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
    # Если глобальный флаг отключен — принудительно ставим Нал = 0
    if not global_stock_flags.get(table_name, True):
        if 'Нал' in df.columns:
            df['Нал'] = 0
    # 👇 Мини-статистика
    total_rows = len(df)
    in_stock = df[df['Нал'].astype(str).str.replace(r'\D', '', regex=True).astype(float) > 0].shape[0]
    disabled = df[df['Статус'].astype(str).str.lower() == 'выкл.'].shape[0]

    price_col = 'Цена'

    def safe_avg(col):
        try:
            return round(
                pd.to_numeric(df[col].astype(str).str.replace(r'\D', '', regex=True), errors='coerce').dropna().mean())
        except:
            return 0

    avg_price = safe_avg(price_col)
    avg_markup = safe_avg('%')

    stats = {
        'Всего товаров': total_rows,
        'В наличии': in_stock,
        'Отключено': disabled,
        f'Средняя цена {price_col.split()[-1]}': f'{avg_price:,} р.'.replace(',', ' '),
        'Средняя наценка': f'{avg_markup} %'
    }

    # 📌 Получаем список всех уникальных поставщиков (фиксированный список, не зависит от df)
    # 📌 Получаем список всех уникальных поставщиков из общей таблицы
    conn_sup = sqlite3.connect(DB_PATH)
    try:
        # Фиксированный список поставщиков
        suppliers_list = ["Invask", "Okno", "United", "Sklad"]

        # Подсчёты: "total" — строк с непустым кодом этого поставщика;
        # "active" — такие строки, у которых Нал > 0.
        conn_cnt = sqlite3.connect(DB_PATH)
        cnt_df = pd.read_sql_query("""
            SELECT LOWER(Маркетплейс) AS mp,
                   SUM(CASE WHEN TRIM(COALESCE(Invask,''))<>'' THEN 1 ELSE 0 END) AS invask_total,
                   SUM(CASE WHEN TRIM(COALESCE(Invask,''))<>'' AND Нал>0 THEN 1 ELSE 0 END) AS invask_active,

                   SUM(CASE WHEN TRIM(COALESCE(Okno,''))<>'' THEN 1 ELSE 0 END) AS okno_total,
                   SUM(CASE WHEN TRIM(COALESCE(Okno,''))<>'' AND Нал>0 THEN 1 ELSE 0 END) AS okno_active,

                   SUM(CASE WHEN TRIM(COALESCE(United,''))<>'' THEN 1 ELSE 0 END) AS united_total,
                   SUM(CASE WHEN TRIM(COALESCE(United,''))<>'' AND Нал>0 THEN 1 ELSE 0 END) AS united_active,

                   SUM(CASE WHEN TRIM(COALESCE(Sklad,''))<>'' THEN 1 ELSE 0 END) AS sklad_total,
                   SUM(CASE WHEN TRIM(COALESCE(Sklad,''))<>'' AND Нал>0 THEN 1 ELSE 0 END) AS sklad_active
              FROM marketplace
             GROUP BY LOWER(Маркетплейс)
        """, conn_cnt)
        conn_cnt.close()

        supplier_counts = {}
        for _, r in cnt_df.iterrows():
            mp = r['mp']
            supplier_counts.setdefault('Invask', {})[mp] = {'total': int(r['invask_total'] or 0),
                                                            'active': int(r['invask_active'] or 0)}
            supplier_counts.setdefault('Okno', {})[mp] = {'total': int(r['okno_total'] or 0),
                                                          'active': int(r['okno_active'] or 0)}
            supplier_counts.setdefault('United', {})[mp] = {'total': int(r['united_total'] or 0),
                                                            'active': int(r['united_active'] or 0)}
            supplier_counts.setdefault('Sklad', {})[mp] = {
                'total': int(r['sklad_total'] or 0),
                'active': int(r['sklad_active'] or 0)
            }

    except Exception:
        suppliers_list = []
    conn_sup.close()

    saved_form_data = session.pop('saved_form', {})
    # Форматируем дату обратно в нужный вид (дд.мм.гггг чч:мм)
    if "Дата изменения" in df.columns:
        df["Дата изменения"] = df["Дата изменения"].dt.strftime("%d.%m.%Y %H:%M")
    has_errors = has_error_products()

    print("🔥 has_errors =", has_errors)
    logger.debug(f"🔥 has_errors = {has_errors}")
    # === ВСТАВИТЬ ПЕРЕД return render_template(...) ===
    # ВСТАВИТЬ ПЕРЕД return render_template(...)
    active_suppliers = []

    # Берём только нужные поля; отсутствующие — заполняем пустыми
    need_cols = ['Sklad', 'Invask', 'Okno', 'United', '%', 'Цена', 'Опт', 'Нал', 'Статус', 'Модель']
    df_for_pick = df.copy()
    for c in need_cols:
        if c not in df_for_pick.columns:
            df_for_pick[c] = ''

    for _, r in df_for_pick[need_cols].fillna('').iterrows():
        row_dict = dict(r)

        # выключенные товары не подсвечиваем
        if str(row_dict.get('Статус', '')).strip().lower() == 'выкл.':
            active_suppliers.append('')
            continue

        chosen_sup, _, _ = choose_best_supplier_for_row(row_dict, None, use_row_sklad=True)

        active_suppliers.append(chosen_sup or '')
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
        supplier_counts=supplier_counts,
        active_suppliers=active_suppliers,
        has_errors=has_errors

    )


@app.route('/delete/<table>/<item_id>', methods=['POST'])
def delete_row(table, item_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем модель и Sklad до удаления
    cursor.execute("SELECT Модель, Sklad FROM marketplace WHERE Sklad = ? AND Маркетплейс = ?", (item_id, table))
    result = cursor.fetchone()
    model, art_mc = result if result else ("", "")

    cursor.execute("DELETE FROM marketplace WHERE Sklad = ? AND Маркетплейс = ?", (item_id, table))
    conn.commit()
    conn.close()

    # Отправляем уведомление
    send_telegram_message(f"🗑 Удалён из {table.upper()}:\n{model} / {art_mc}")
    logger.warning(f"🗑 Удалён товар из {table.upper()}: {model} / {art_mc}")

    return redirect(url_for('show_table', table_name=table, search=''))


@app.route('/update/<table>/<item_id>', methods=['POST'])
def update_row(table, item_id):
    data = request.form.to_dict()
    if "Sklad" in data:
        del data["Sklad"]

    # Получаем старые данные для сравнения
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM marketplace WHERE Sklad = ? AND Маркетплейс = ?", (item_id, table))
    row = cursor.fetchone()
    column_names = [description[0] for description in cursor.description]
    old_data = dict(zip(column_names, row)) if row else {}

    if not old_data:
        conn.close()
        logger.warning(f"⚠️ Товар с Sklad = {item_id} не найден.")
        return '', 400

    if not global_stock_flags.get(table, True):
        logger.info(f"⚙️ Редактирование товара в выключенном маркетплейсе: {table}")
        if 'Нал' in data:
            del data['Нал']

    model = old_data.get("Модель", "—")
    opt_old = int(old_data.get("Опт", 0))
    stock_old = int(old_data.get("Нал", 0))
    price_old = 0

    price_old = int(old_data.get("Цена", 0) or 0)

    try:
        stock_new = int(data.get("Нал", 0))
        opt_new = int(data.get("Опт", 0))
        markup = float(data.get("%", "0").replace('%', '').replace(' ', ''))
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

    # Удалить Sklad из обновляемых данных

    try:
        opt = float(data.get('Опт', '0').replace(' ', '').replace('р.', ''))
        markup = float(data.get('%', '0').replace(' ', '').replace('%', ''))
        raw_price = opt + (opt * markup / 100)
        price = int(round(raw_price / 100.0) * 100)
        formatted_price = str(price)

        # Получаем список колонок таблицы
        conn_check = sqlite3.connect(DB_PATH)
        cur_check = conn_check.cursor()
        cur_check.execute("PRAGMA table_info(marketplace)")
        table_columns = [col[1] for col in cur_check.fetchall()]
        conn_check.close()

        data['Цена'] = formatted_price  # В новой структуре колонка 'Цена' всегда есть

        if '%' in data:
            data['%'] = str(int(markup))

    except ValueError:
        logger.warning("❌ Невалидные данные в Опт/Наценка для пересчёта цены.")

    # Ключевые поля, которые влияют на "Дата изменения"
    important_fields = [
        "Invask", "Okno", "United", "Модель", "Статус", "Нал", "Опт", "%", "Комментарий",
        "Цена", "WB Артикул", "WB Barcode"
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

    logger.debug(f"🧩 SQL запрос: UPDATE '{table}' SET {update_clause} WHERE \"Sklad\" = ?")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE marketplace SET {update_clause} WHERE Sklad = ? AND Маркетплейс = ?",
            values + [item_id, table]
        )
        conn.commit()
        logger.debug(f"🧾 Кол-во обновлённых строк: {cursor.rowcount}")
        logger.success("✅ Успешно обновлено!")

        # 🔍 Проверяем смену статуса
        cursor.execute("""
            SELECT Статус, Модель
              FROM marketplace
             WHERE Sklad = ? AND LOWER(Маркетплейс) = LOWER(?)
        """, (item_id, table))
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


@app.route('/bulk_markup/<market>', methods=['POST'])
def bulk_markup(market):
    """
    Массовая корректировка наценки для выбранного маркетплейса.
    delta = +1 или -1 (в теле запроса x-www-form-urlencoded: delta=1|-1)
    Пересчитывает 'Цена' по формуле округления к сотне.
    """
    try:
        delta = int(request.form.get('delta', '0'))
    except Exception:
        return Response("Bad delta", status=400)

    if market not in ('yandex', 'ozon', 'wildberries'):
        return Response("Bad market", status=400)

    # Если маркетплейс глобально выключен — меняем только наценку и цену, остатки не трогаем
    from datetime import datetime
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Наценка хранится без знака %, Опт и Цена — числа/строки-числа.
    # Обновляем наценку и сразу цену с округлением к сотне.
    try:
        cur.execute("""
            UPDATE marketplace
               SET "%" = COALESCE(CAST("%" AS INTEGER), 0) + ?,
                   Цена = CAST(
                              ROUND(
                                  (CAST(Опт AS FLOAT) + CAST(Опт AS FLOAT) * (COALESCE(CAST("%" AS INTEGER),0) + ?)/100.0)
                                  / 100.0, 0
                              ) * 100 AS INTEGER
                          ),
                   "Дата изменения" = ?
             WHERE Маркетплейс = ?
        """, (delta, delta, now_str, market))
        conn.commit()
        updated = cur.rowcount
    except Exception as e:
        conn.rollback()
        logger.exception("❌ Ошибка массового изменения наценки")
        return Response("Server error", status=500)
    finally:
        conn.close()

    logger.success(f"📈 Массовое изменение наценки {market}: delta={delta}, затронуто строк: {updated}")
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
        # Если маркетплейс OFF — принудительно Нал = 0
        data['Нал'] = '0'
        logger.info(f"➖ Добавление товара в выключенный маркетплейс {table_name}: остаток принудительно 0")

    art_mc = data.get('Sklad', '').strip()
    invask = (data.get('Invask', '') or '').strip()
    okno = (data.get('Okno', '') or '').strip()
    united = (data.get('United', '') or '').strip()

    # Проверяем обязательные поля
    if not art_mc or not data.get('Модель') or not data.get('Нал') or not data.get('Опт') or not data.get('%'):
        logger.warning("❌ Не заполнены обязательные поля (Sklad, Модель, Нал, Опт, %).")
        return redirect(url_for('show_table', table_name=table_name))

    # Дополнительно проверка для Wildberries
    if table_name == "wildberries":
        if not data.get('WB Barcode') or not data.get('WB Артикул'):
            logger.warning("❌ Для Wildberries обязательны WB Barcode и WB Артикул.")
            return redirect(url_for('show_table', table_name=table_name))

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    model = data.get('Модель', '').strip()
    wb_barcode = data.get('WB Barcode', '').strip()
    wb_artikul = data.get('WB Артикул', '').strip()

    # Проверка дубликатов: учитываем только непустые поля
    conditions = []
    params = [table_name]

    if art_mc:
        conditions.append("Sklad = ?")
        params.append(art_mc)
    if model:
        conditions.append("Модель = ?")
        params.append(model)
    if wb_barcode:
        conditions.append("\"WB Barcode\" = ?")
        params.append(wb_barcode)
    if wb_artikul:
        conditions.append("\"WB Артикул\" = ?")
        params.append(wb_artikul)

    existing_count = 0
    if conditions:
        query = f"""
            SELECT COUNT(*) FROM marketplace
            WHERE Маркетплейс = ?
              AND ({' OR '.join(conditions)})
        """
        cursor.execute(query, params)
        existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        conn.close()
        logger.warning("⚠️ Товар с таким Sklad, Модель, WB Barcode или WB Артикул уже существует.")
        session['saved_form'] = data
        return redirect(url_for('show_table', table_name=table_name, duplicate='1'))

    try:
        opt = float(data.get('Опт', '').replace(' ', '').replace('р.', ''))
        markup = float(data.get('%', '').replace('%', '').replace(' ', ''))
        stock = int(data.get('Нал', '').replace(' ', ''))
        if data.get('Статус', '').strip() == 'выкл.':
            stock = 0

        data['Нал'] = str(stock)
        price_ym = int(round((opt + (opt * markup / 100)) / 100.0) * 100)

        data['Опт'] = str(opt)
        data['%'] = str(int(markup))
        data['Нал'] = str(stock)

        data['Цена'] = str(price_ym)

        # Устанавливаем дату изменения только перед вставкой
        data["Дата изменения"] = datetime.now().strftime("%d.%m.%Y %H:%M")

        data["Маркетплейс"] = table_name  # добавляем до формирования списков

        columns = list(data.keys())
        values = [data[col] for col in columns]
        placeholders = ", ".join(["?"] * len(columns))
        escaped_columns = [f'"{col}"' for col in columns]
        insert_query = f"INSERT INTO marketplace ({', '.join(escaped_columns)}) VALUES ({placeholders})"

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
    df = pd.read_sql_query(
        "SELECT Sklad, Invask, Okno, United, Модель, Статус, Маркетплейс, Опт, Нал FROM marketplace",
        conn
    )
    conn.close()

    data = {}
    supplier_stats = {
        "Sklad": {"Yandex": 0, "Ozon": 0, "Wildberries": 0, "Всего": 0, "Активно": 0, "Неактивно": 0},
        "Invask": {"Yandex": 0, "Ozon": 0, "Wildberries": 0, "Всего": 0, "Активно": 0, "Неактивно": 0},
        "Okno": {"Yandex": 0, "Ozon": 0, "Wildberries": 0, "Всего": 0, "Активно": 0, "Неактивно": 0},
        "United": {"Yandex": 0, "Ozon": 0, "Wildberries": 0, "Всего": 0, "Активно": 0, "Неактивно": 0},
    }

    for _, row in df.iterrows():
        art_mc = row['Sklad']
        status = (row.get('Статус') or '').strip().lower()
        mp = row.get('Маркетплейс', '').capitalize()

        if art_mc not in data:
            data[art_mc] = {
                'Sklad': art_mc,
                'Модель': row.get('Модель', '')
            }

        # отмечаем наличие товара на маркетплейсе
        data[art_mc][mp] = True
        if status == 'выкл.':
            data[art_mc][f'Статус_{mp}'] = 'выкл.'

        # обновляем статистику по каждому поставщику
        for sup in ["Sklad", "Invask", "Okno", "United"]:
            if sup == "Sklad":
                cond = (not row['Invask'] and not row['Okno'] and not row['United'] and row['Sklad'])
            else:
                cond = bool(row[sup])

            if cond:
                supplier_stats[sup][mp] += 1
                supplier_stats[sup]['Всего'] += 1
                if status == 'выкл.':
                    supplier_stats[sup]['Неактивно'] += 1
                else:
                    supplier_stats[sup]['Активно'] += 1

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
    import sqlite3
    import pandas as pd

    db_path = "System/marketplace_base.db"
    conn = sqlite3.connect(db_path, timeout=10)
    df = pd.read_sql_query("SELECT * FROM marketplace", conn)
    conn.close()

    df["Маркетплейс"] = df["Маркетплейс"].str.capitalize()

    errors = []
    # 👇 теперь проверяем и Sklad как поле
    fields_to_check = ["Sklad", "Invask", "Okno", "United", "Модель", "Статус"]

    for art_mc, group in df.groupby("Sklad"):
        if len(group) <= 1:
            continue

        values_by_field = {field: set(group[field].astype(str).fillna("")) for field in fields_to_check}

        has_diff = any(len(values) > 1 for values in values_by_field.values())
        if not has_diff:
            continue

        for _, row in group.iterrows():
            row_dict = row.to_dict()
            diff = {field: len(values_by_field[field]) > 1 for field in fields_to_check}
            diff["Нал"] = False
            diff["Опт"] = False

            errors.append({
                "Маркетплейс": row_dict.get("Маркетплейс", ""),
                "Sklad": row_dict.get("Sklad", ""),
                "Invask": row_dict.get("Invask", ""),
                "Okno": row_dict.get("Okno", ""),
                "United": row_dict.get("United", ""),
                "Модель": row_dict.get("Модель", ""),
                "Статус": row_dict.get("Статус", ""),
                "Нал": row_dict.get("Нал", ""),
                "Опт": row_dict.get("Опт", ""),
                "diff": diff
            })

    return errors


@app.errorhandler(Exception)
def handle_error(e):
    logger.exception(f"💥 Ошибка: {str(e)}")
    return "Произошла ошибка на сервере", 500

@app.route('/download_unlisted')
@requires_auth
def download_unlisted():
    try:
        df = generate_unlisted()
        if df.empty:
            return Response("Нет новых товаров", status=404)

        # Сохраняем в Excel в памяти
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name="new_products.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        logger.exception("❌ Ошибка при формировании списка новых товаров")
        return Response("Ошибка при формировании файла", status=500)

def recompute_marketplace_core(market: str) -> int:
    # если маркетплейс выключен - не трогаем остатки
    if not global_stock_flags.get(market, True):
        logger.info(f"⏭ {market.upper()} выключен → пересчёт пропущен, нули сохраняем")
        return
    """Чистый пересчёт без Flask-контекста. Возвращает кол-во обновлённых строк."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT rowid, Sklad, Invask, Okno, United,
               "%", Цена, Опт, Нал, Статус, Модель
          FROM marketplace
         WHERE Маркетплейс = ?
    """, (market,)).fetchall()

    updated = 0
    for r in rows:
        row = dict(r)
        chosen_sup, nal, opt = choose_best_supplier_for_row(row, conn, use_row_sklad=True)
        logger.debug(
            f"🔎 {market.upper()} | {row.get('Модель', '—')} | Sklad={row.get('Sklad', '')}, "
            f"Invask={row.get('Invask', '')}, Okno={row.get('Okno', '')}, United={row.get('United', '')} "
            f"→ chosen={chosen_sup} nal={nal} opt={opt}"
        )

        if chosen_sup == '':
            new_nal = 0
            new_opt = row.get('Опт')
        else:
            new_nal = int(nal or 0)
            new_opt = opt if opt is not None else row.get('Опт')

        try:
            markup = float(str(row.get('%', '0')).replace('%', '').replace(' ', ''))
        except:
            markup = 0.0

            # Выключенный товар всегда с Нал = 0
        if str(row.get('Статус', '')).strip().lower() == 'выкл.':
            new_nal = 0

            # 🚫 Заморозка: при Нал=0 НЕ трогаем Опт/Цена
        freeze_price = int(new_nal or 0) == 0

        new_price = None
        if not freeze_price and (new_opt is not None):
            try:
                base_opt = float(str(new_opt).replace(' ', '').replace('р.', ''))
                new_price = int(round((base_opt + (base_opt * markup / 100.0)) / 100.0) * 100)
            except Exception:
                new_price = None

        changed = False
        sets, vals = [], []

        if int(row.get('Нал') or 0) != int(new_nal):
            sets.append('Нал = ?'); vals.append(int(new_nal)); changed = True

        try:
            cur_opt = float(str(row.get('Опт') or '0').replace(' ', '').replace('р.', ''))
        except:
            cur_opt = None
        if (not freeze_price) and (new_opt is not None):
            try:
                new_opt_f = float(new_opt)
            except:
                new_opt_f = cur_opt
            if cur_opt is None or (new_opt_f is not None and new_opt_f != cur_opt):
                sets.append('Опт = ?');
                vals.append(new_opt_f);
                changed = True

        try:
            cur_price = int(str(row.get('Цена') or '0').replace(' ', '').replace('р.', ''))
        except:
            cur_price = 0
        if new_price is not None and new_price != cur_price:
            sets.append('Цена = ?')
            vals.append(int(new_price))
            changed = True

        if changed:
            sets.append('"Дата изменения" = ?'); vals.append(datetime.now().strftime("%d.%m.%Y %H:%M"))
            vals.append(r['rowid'])
            cur.execute(f"UPDATE marketplace SET {', '.join(sets)} WHERE rowid = ?", vals)
            updated += 1

    conn.commit()
    conn.close()

    logger.success(f"🔄 Пересчитано строк в {market.upper()}: {updated}")
    return updated

@app.route('/recompute/<market>', methods=['POST', 'GET'])
@requires_auth
def recompute_marketplace(market):
    updated = recompute_marketplace_core(market)
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return '', 204
    return redirect(url_for('show_table', table_name=market))


if __name__ == '__main__':
    if not os.environ.get("WERKZEUG_RUN_MAIN"):  # предотвращает двойной запуск задач
        scheduler = BackgroundScheduler()
        scheduler.add_job(update_sklad_task, 'interval', minutes=5)
        scheduler.add_job(remove_all_products_from_all_actions, 'interval', minutes=1)  # Проверка Акций Озон
        scheduler.add_job(backup_database, 'cron', hour=2)  # каждый день в 2 ночи
        scheduler.add_job(disable_invask_if_needed, 'cron', day_of_week='fri', hour=1, minute=0)
        scheduler.add_job(enable_invask_if_needed, 'cron', day_of_week='sun', hour=15, minute=0)
        scheduler.add_job(disable_okno_if_needed, 'cron', day_of_week='fri', hour=1, minute=0)
        scheduler.add_job(enable_okno_if_needed, 'cron', day_of_week='sun', hour=15, minute=0)
        scheduler.start()
        logger.info("📅 Планировщик запущен (обновление склада каждые 5 минут)")
    logger.info("🚀 Приложение запущено")
    app.run(host="127.0.0.1", port=5050, debug=False, use_reloader=False)

