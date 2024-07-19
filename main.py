import remainders
import gen_file_sklad
from order_notifications import check_for_new_orders
import price_updater_WB as updater
import price_updater_YM as ym_updater
import time

if __name__ == "__main__":
    while True:

        # Берем данные из Google таблиц и сохраняем в CSV
        try:
            gen_file_sklad.gen_sklad()
        except Exception as e:
            message = f"😨 Ошибка при получении данных из Google Таблицы: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id,
                                       message=message)
        time.sleep(5)

        # Берем данные из CSV и преобразовываем в нужный вид
        try:
            wb, ym = remainders.gen_sklad()
        except Exception as e:
            message = f"😨 Ошибка при получении данных из файла: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id,
                                       message=message)

        # Отправляем остатки в WB
        try:
            remainders.wb_update(wb)
        except Exception as e:
            message = f"😨 Ошибка при обновлении данных в Wildberries: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id,
                                       message=message)
        # Отправляем остатки в YM
        try:
            remainders.ym_update(ym)
        except Exception as e:
            message = f"😨 Ошибка при обновлении данных в Yandex.Market: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id,
                                       message=message)

        # Проверяем наличие новых заказов
        try:
            check_for_new_orders()
        except Exception as e:
            message = f"😨 Ошибка при проверке новых заказов: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id,
                                       message=message)

        # Обновляем цены на WB
        try:
            # Загрузка данных из CSV файла
            df = updater.load_data('sklad_prices.csv')
            # Создание данных для WB API
            wb_data = updater.create_wb_data(df)
            # Обновление цен на Wildberries
            updater.wb_price_update(wb_data)
        except Exception as e:
            message = f"😨 Ошибка при обновлении цен в Wildberries: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id,
                                       message=message)

        # Обновляем цены на YM
        try:
            # Путь к файлу sklad_prices.csv
            file_path = 'sklad_prices.csv'
            # Чтение данных из CSV файла
            ym_data = ym_updater.read_sklad_csv(file_path)
            # Обновление цен на Yandex Market
            ym_updater.ym_price_update(ym_data)
        except Exception as e:
            message = f"😨 Ошибка при обновлении цен в Yandex.Market: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id,
                                       message=message)


        time.sleep(60)
