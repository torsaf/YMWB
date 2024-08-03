"""
В модуле берутся данные товаров из гугл таблиц и сохраняются в файл sklad_prices.csv
"""

import pandas as pd
import gspread
import numpy as np


def gen_sklad():
    # pd.set_option('display.max_columns', None)  # Показывать все столбцы
    # pd.set_option('display.expand_frame_repr', False)  # Отключить перенос строк
    gc = gspread.service_account(filename='my-python-397519-3688db4697d6.json')
    sh = gc.open("КАЗНА")
    worksheet_name = "СКЛАД"
    worksheet = sh.worksheet(worksheet_name)
    data = worksheet.get('A1:T')
    data = [row for row in data if any(cell.strip() for cell in row)]
    data = [[cell.strip() for cell in row] for row in data]
    filtered_data = [row for row in data[1:] if 'SKL' in row]
    columns = data[0]
    data = filtered_data
    sklad = pd.DataFrame(data, columns=columns)
    desired_columns = ['YM', 'MM', 'WB Barcode', 'WB Артикул', 'Статус', 'Модель', 'Наличие', 'Цены YM, WB, MM']
    sklad = sklad.loc[:, desired_columns]
    sklad_filtered = sklad[sklad['Статус'] == 'На складе']
    # Удаление столбца 'Статус'
    sklad_filtered = sklad_filtered.drop(columns=['Статус'])

    # Удаление строк с пустыми значениями в 'Цены YM, WB, MM'
    sklad_filtered = sklad_filtered[sklad_filtered['Цены YM, WB, MM'].str.strip().astype(bool)]
    # # Удалить пробелы и символы валюты, преобразовать к int
    sklad_filtered['Цены YM, WB, MM'] = sklad_filtered['Цены YM, WB, MM'].replace({'\s+|₽': ''}, regex=True).astype(int)
    sklad_filtered = sklad_filtered.rename(columns={'Цены YM, WB, MM': 'Цена'})
    # Создание нового столбца 'Цена до скидки'
    sklad_filtered['Цена до скидки'] = (np.ceil(sklad_filtered['Цена'] * 1.20 / 100) * 100).astype(int)
    # Удаление строк, если оба поля "YM" и "WB" пустые, или если "Наличие" == 0
    sklad_filtered = sklad_filtered[~((sklad_filtered['YM'].str.strip() == '') & (sklad_filtered['WB Barcode'].str.strip() == ''))]
    sklad_filtered.to_csv('sklad_prices.csv', index=False)


