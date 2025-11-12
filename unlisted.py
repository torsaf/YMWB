import sqlite3
import pandas as pd
from logger_config import logger


def generate_unlisted():
    """Формирует DataFrame с товарами, которых нет на маркетплейсах и не от 'Sklad'."""
    try:
        # Подключение к базе marketplace
        with sqlite3.connect('System/marketplace_base.db') as mp_conn:
            mp_df = pd.read_sql_query("SELECT Sklad, Invask, Okno, United FROM marketplace", mp_conn)

        # Объединяем и нормализуем артикулы
        listed_articles = pd.concat([
            mp_df['Sklad'],
            mp_df['Invask'],
            mp_df['Okno'],
            mp_df['United']
        ], ignore_index=True).fillna('').astype(str)

        # Очистка от пробелов, табов и ведущих нулей
        listed_articles = listed_articles.str.replace(r'\s+', '', regex=True).str.lstrip('0')
        listed_articles = listed_articles[listed_articles != ''].unique()

        # Подключение к базе всех товаров
        with sqlite3.connect('System/!YMWB.db') as all_conn:
            all_df = pd.read_sql_query("SELECT * FROM prices", all_conn)

        # Нормализуем артикулы в базе всех товаров
        all_df['Артикул'] = all_df['Артикул'].astype(str).fillna('').str.replace(r'\s+', '', regex=True).str.lstrip('0')

        # Фильтрация: исключаем уже имеющиеся и склад
        not_listed_df = all_df[
            (~all_df['Артикул'].isin(listed_articles)) &
            (all_df['Поставщик'].str.lower() != 'sklad')
        ]

        return not_listed_df.reset_index(drop=True)

    except Exception as e:
        logger.error(f"Ошибка в generate_unlisted(): {e}", exc_info=True)
        return pd.DataFrame()


