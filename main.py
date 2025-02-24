import remainders
import gen_file_sklad
from order_notifications import check_for_new_orders
import price_updater_WB as wb_updater
import price_updater_YM as ym_updater
import price_updater_MM as mm_updater
import price_updater_OZ as oz_updater
import signal
import time
import sys


def send_telegram_message(message):
    remainders.telegram.notify(token=remainders.telegram_got_token_error, chat_id=remainders.telegram_chat_id_error,
                               message=message)


# Обработчик сигнала для корректного завершения программы
def handle_exit_signal(signum, frame):
    send_telegram_message("Программа завершена❗️")
    sys.exit(0)


# Установка обработчиков сигнала для корректного завершения программы
signal.signal(signal.SIGTERM, handle_exit_signal)
signal.signal(signal.SIGINT, handle_exit_signal)

# Отправка сообщения о запуске программы
send_telegram_message("Программа запущена⭐️")


def notify_error(action, error):
    message = f"😨 Ошибка при {action}: {error}"
    send_telegram_message(message)


def update_prices(updater, file_path, update_action):
    try:
        data = updater.read_sklad_csv(file_path)
        update_action(data)
    except AttributeError as e:
        notify_error(f"обновлении цен в {updater.__name__}", f"Отсутствует функция read_sklad_csv: {e}")
    except Exception as e:
        notify_error(f"обновлении цен в {updater.__name__}", e)


def update_stock(update_func, stock_data):
    try:
        update_func(stock_data)
    except Exception as e:
        notify_error(f"обновлении данных в {update_func.__name__}", e)


if __name__ == "__main__":
    while True:
        # Берем данные из Google таблиц и сохраняем в CSV
        try:
            gen_file_sklad.gen_sklad()
        except Exception as e:
            notify_error("получении данных из Google Таблицы", e)
        time.sleep(5)

        # Берем данные из CSV и преобразовываем в нужный вид
        try:
            wb, ym, mm, oz = remainders.gen_sklad()
        except Exception as e:
            notify_error("получении данных из файла", e)
            continue  # Пропускаем оставшуюся часть итерации, если данные не получены

        # Обновляем остатки в WB, YM и MM
        update_stock(remainders.wb_update, wb)
        update_stock(remainders.ym_update, ym)
        update_stock(remainders.mm_update, mm)
        update_stock(remainders.oz_update, oz)

        # Проверяем наличие новых заказов
        try:
            check_for_new_orders()
        except Exception as e:
            notify_error("проверке новых заказов", e)

        # Обновляем цены на WB
        try:
            df = wb_updater.load_data('sklad_prices_wildberries.csv')
            wb_data = wb_updater.create_wb_data(df)
            wb_updater.wb_price_update(wb_data)
        except Exception as e:
            notify_error("обновлении цен в Wildberries", e)

        # Обновляем цены на YM
        update_prices(ym_updater, 'sklad_prices_yandex.csv', ym_updater.ym_price_update)

        # Обновляем цены на MM
        update_prices(mm_updater, 'sklad_prices_megamarket.csv', mm_updater.mm_price_update)

        # Обновляем цены на OZ
        update_prices(oz_updater, 'sklad_prices_ozon.csv', oz_updater.oz_price_update)

        time.sleep(400)
