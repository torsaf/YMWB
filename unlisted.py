import sqlite3
import pandas as pd
from logger_config import logger


def generate_unlisted():
    """Формирует DataFrame с товарами, которых нет на маркетплейсах и не от 'Sklad'."""
    try:
        # Подключение к базе marketplace
        with sqlite3.connect('System/marketplace_base.db') as mp_conn:
            mp_df = pd.read_sql_query("SELECT Sklad, Invask, Okno, United FROM marketplace", mp_conn)

        # Собираем все артикулы в один список
        listed_articles = pd.concat([
            mp_df['Sklad'],
            mp_df['Invask'],
            mp_df['Okno'],
            mp_df['United']
        ], ignore_index=True).dropna().unique()

        # Подключение к базе всех товаров
        with sqlite3.connect('System/!YMWB.db') as all_conn:
            all_df = pd.read_sql_query("SELECT * FROM prices", all_conn)

        # Отбор: не выложенные и не от 'Sklad'
        not_listed_df = all_df[
            (~all_df['Артикул'].isin(listed_articles)) &
            (all_df['Поставщик'] != 'Sklad')
        ]

        return not_listed_df

    except Exception as e:
        logger.error(f"Ошибка в generate_unlisted(): {e}", exc_info=True)
        return pd.DataFrame()

