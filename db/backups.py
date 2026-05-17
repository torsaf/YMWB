from flask import Blueprint, render_template, request, redirect, url_for, session
import sqlite3
import pandas as pd

from logger_config import logger

tables_bp = Blueprint('tables', __name__)


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

