import pandas as pd
import gspread
import numpy as np


def gen_sklad():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    gc = gspread.service_account(filename='my-python-397519-3688db4697d6.json')
    sh = gc.open("КАЗНА")
    worksheet_name = "СКЛАД"
    worksheet = sh.worksheet(worksheet_name)
    data = worksheet.get('A1:AG')
    data = [row for row in data if any(cell.strip() for cell in row)]
    data = [[cell.strip() for cell in row] for row in data]
    filtered_data = [row for row in data[1:] if 'SKL' in row or 'UNT' in row]
    columns = data[0]
    data = filtered_data
    sklad = pd.DataFrame(data, columns=columns)

    marketplaces = {
        'ozon': ['OZ', 'Статус', 'Модель', 'Наличие', 'OZ-наличие', 'Цены OZ'],
        'yandex': ['YM', 'Статус', 'Модель', 'Наличие', 'YM-наличие', 'Цены YM'],
        'megamarket': ['MM', 'Статус', 'Модель', 'Наличие', 'MM-наличие', 'Цены MM'],
        'wildberries': ['WB Barcode', 'WB Артикул', 'Статус', 'Модель', 'Наличие', 'WB-наличие', 'Цены WB']
    }

    for marketplace, columns in marketplaces.items():
        sklad_filtered = sklad.loc[:, columns]
        sklad_filtered = sklad_filtered[sklad_filtered['Статус'].isin(['На складе', 'Товар в UM'])]
        sklad_filtered = sklad_filtered.drop(columns=['Статус'])

        price_column = columns[-1]

        # Определяем название столбца наличия для каждого маркетплейса
        availability_column = columns[-2] if marketplace != 'wildberries' else 'WB-наличие'
        id_column = columns[0]

        sklad_filtered = sklad_filtered[sklad_filtered[price_column].str.strip().astype(bool)]
        sklad_filtered[price_column] = sklad_filtered[price_column].replace({'\s+|₽': ''}, regex=True).astype(float)

        sklad_filtered = sklad_filtered.rename(columns={price_column: 'Цена'})
        sklad_filtered['Цена'] = sklad_filtered['Цена'].astype(int)  # Преобразование к int после удаления дробной части

        sklad_filtered.loc[sklad_filtered[availability_column] == '-', 'Наличие'] = 0
        sklad_filtered['Наличие'] = sklad_filtered['Наличие'].astype(int)

        sklad_filtered['Цена до скидки'] = (sklad_filtered['Цена'] * 1.20).round(-2).astype(
            int)  # Вычисление до скидки и округление

        # Удаление столбца "OZ-наличие", "YM-наличие", "MM-наличие", или "WB-наличие"
        sklad_filtered = sklad_filtered.drop(columns=[availability_column])

        sklad_filtered = sklad_filtered[~(sklad_filtered[id_column].str.strip() == '')]

        filename = f'sklad_prices_{marketplace}.csv'
        sklad_filtered.to_csv(filename, index=False)
        # print(f"Файл для {marketplace} сохранен: {filename}")


