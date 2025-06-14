import os
import sys
import signal
from pathlib import Path
from stock import gen_sklad, wb_update, ym_update, oz_update
from order_notifications import check_for_new_orders
from price_updater_master import update_all_prices
import stock
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent / "System" / ".env")
from logger_config import logger


# 📦 Переход в директорию проекта
os.chdir(os.path.dirname(os.path.abspath(__file__)))


# 📬 Телеграм-уведомление
def send_telegram_message(message: str):
    stock.telegram.notify(
        token=stock.telegram_got_token_error,
        chat_id=stock.telegram_chat_id_error,
        message=message
    )


# ❌ Обработчик завершения по сигналу
def handle_exit_signal(signum, frame):
    logger.info("📴 Получен сигнал завершения")
    send_telegram_message("Программа завершена❗️")
    sys.exit(0)


# Установка сигналов завершения
signal.signal(signal.SIGTERM, handle_exit_signal)
signal.signal(signal.SIGINT, handle_exit_signal)


# 🛠 Универсальная обёртка для try/except с логом и телеграмом
def run_safe(action_description: str, func):
    try:
        logger.info(f"▶ Начало: {action_description}")
        func()
        logger.success(f"✅ Завершено: {action_description}")
    except Exception as e:
        logger.exception(f"❌ Ошибка при {action_description}")
        send_telegram_message(f"😨 Ошибка при {action_description}: {e}")
        sys.exit(1)


# 🔁 Обновление остатков
def run_price_updates():
    try:
        logger.info("📦 Получаем складские остатки...")
        wb_data, ym_data, oz_data = gen_sklad()
        logger.success("✅ Остатки успешно получены")
    except Exception as e:
        logger.exception("❌ Ошибка при получении данных из БД")
        send_telegram_message(f"❌ Ошибка при получении данных из БД: {e}")
        sys.exit(1)

    tasks = [
        ("обновлении WB", lambda: wb_update(wb_data)),
        ("обновлении YM", lambda: ym_update(ym_data)),
        ("обновлении OZ", lambda: oz_update(oz_data)),
    ]

    for description, task in tasks:
        run_safe(description, task)


# 🚀 Основной запуск
def main():
    logger.info("🚀 Старт фонового процесса")
    # send_telegram_message("Программа запущена⭐️")

    run_price_updates()  # Обновление остатков складов
    run_safe("проверке новых заказов", check_for_new_orders)
    run_safe("обновлении всех цен", update_all_prices)

    logger.success("🏁 Все задачи завершены успешно")


if __name__ == "__main__":
    main()
