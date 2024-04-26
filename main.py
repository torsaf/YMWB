import remainders
from order_notifications import check_for_new_orders
import time

if __name__ == "__main__":
    while True:
        try:
            wb, ym = remainders.gen_sklad()
        except Exception as e:
            print(f"😨 Ошибка при получении данных из Google Таблицы: {e}")

        try:
            remainders.wb_update(wb)
        except Exception as e:
            message = f"😨 Ошибка при обновлении данных в Wildberries: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id, message=message)

        try:
            remainders.ym_update(ym)
        except Exception as e:
            message = f"😨 Ошибка при обновлении данных в Yandex.Market: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id, message=message)

        # Проверяем наличие новых заказов
        try:
            check_for_new_orders()
        except Exception as e:
            message = f"😨 Ошибка при проверке новых заказов: {e}"
            remainders.telegram.notify(token=remainders.telegram_got_token, chat_id=remainders.telegram_chat_id, message=message)

        time.sleep(30)

