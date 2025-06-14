import sqlite3
import pandas as pd
from logger_config import logger


def generate_unlisted():
    """Формирует DataFrame с товарами, которых нет на маркетплейсах и не от 'Sklad'."""
    try:
        # Подключение к базе маркетплейсов
        with sqlite3.connect('System/marketplace_base.db') as mp_conn:
            ozon_df = pd.read_sql_query("SELECT Артикул FROM ozon", mp_conn)
            wb_df = pd.read_sql_query("SELECT Артикул FROM wildberries", mp_conn)
            ym_df = pd.read_sql_query("SELECT Артикул FROM yandex", mp_conn)

        all_listed_articles = pd.concat([ozon_df, wb_df, ym_df])['Артикул'].dropna().unique()

        # Подключение к базе всех товаров
        with sqlite3.connect('System/!YMWB.db') as all_conn:
            all_df = pd.read_sql_query("SELECT * FROM prices", all_conn)

        # Отбор: не выложенные и не от 'Sklad'
        not_listed_df = all_df[
            (~all_df['Артикул'].isin(all_listed_articles)) &
            (all_df['Поставщик'] != 'Sklad')
        ]

        return not_listed_df

    except Exception as e:
        logger.error(f"Ошибка в generate_unlisted(): {e}", exc_info=True)
        return pd.DataFrame()
