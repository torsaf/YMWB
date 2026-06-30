import os
import sys
import signal
import json
from pathlib import Path
from stock import gen_sklad, wb_update, ym_update, oz_update
from order_notifications import check_for_new_orders
from price_updater_master import update_all_prices
import stock
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / "System" / ".env")
from logger_config import logger


# 📦 Переход в директорию проекта
os.chdir(os.path.dirname(os.path.abspath(__file__)))

FLAGS_PATH = Path("System/stock_flags.json")

STOCK_FIELD_NAMES = {
    "нал",
    "наличие",
    "остаток",
    "остатки",
    "quantity",
    "quantities",
    "stock",
    "available",
    "available_stock",
    "amount",
    "count",
}


def load_stock_flags():
    try:
        with FLAGS_PATH.open("r", encoding="utf-8") as f:
            flags = json.load(f)
    except Exception as e:
        logger.warning(f"⚠️ Не удалось прочитать {FLAGS_PATH}: {e}. Используем все маркетплейсы ON.")
        return {
            "wildberries": True,
            "yandex": True,
            "ozon": True,
            "suppliers": {},
        }

    flags.setdefault("wildberries", True)
    flags.setdefault("yandex", True)
    flags.setdefault("ozon", True)
    flags.setdefault("suppliers", {})
    return flags


def is_market_enabled(flags: dict, market: str) -> bool:
    return bool(flags.get(market, True))


def is_stock_field(field_name) -> bool:
    normalized = str(field_name).strip().lower().replace(" ", "_").replace("-", "_")
    return normalized in STOCK_FIELD_NAMES


def _zero_stock_payload(payload):
    if hasattr(payload, "copy") and hasattr(payload, "columns"):
        result = payload.copy()
        stock_columns = [col for col in result.columns if is_stock_field(col)]

        for col in stock_columns:
            result[col] = 0

        return result, len(stock_columns)

    if isinstance(payload, dict):
        result = {}
        changed = 0

        for key, value in payload.items():
            if isinstance(value, (dict, list, tuple)):
                result[key], nested_changed = _zero_stock_payload(value)
                changed += nested_changed
            elif is_stock_field(key):
                result[key] = 0
                changed += 1
            else:
                result[key] = value

        return result, changed

    if isinstance(payload, list):
        result = []
        changed = 0

        for item in payload:
            new_item, item_changed = _zero_stock_payload(item)
            result.append(new_item)
            changed += item_changed

        return result, changed

    if isinstance(payload, tuple):
        result = []
        changed = 0

        for item in payload:
            new_item, item_changed = _zero_stock_payload(item)
            result.append(new_item)
            changed += item_changed

        return tuple(result), changed

    return payload, 0


def zero_stock_payload(payload, market: str):
    zeroed_payload, changed = _zero_stock_payload(payload)

    if changed == 0:
        raise RuntimeError(
            f"{market}: не найдено ни одного поля остатка для обнуления. "
            f"Нужен файл stock.py, чтобы точно указать структуру данных для API."
        )

    logger.info(f"🧯 {market.upper()} выключен → перед отправкой обнулены поля остатков: {changed}")
    return zeroed_payload


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

    flags = load_stock_flags()

    market_tasks = [
        ("wildberries", "обновлении WB", wb_update, wb_data),
        ("yandex", "обновлении YM", ym_update, ym_data),
        ("ozon", "обновлении OZ", oz_update, oz_data),
    ]

    for market, description, update_func, payload in market_tasks:
        sync_payload = payload

        if not is_market_enabled(flags, market):
            sync_payload = zero_stock_payload(payload, market)

        run_safe(
            description,
            lambda update_func=update_func, sync_payload=sync_payload: update_func(sync_payload)
        )

# 🚀 Основной запуск
def main():
    logger.info("🚀 Старт фонового процесса")
    # send_telegram_message("Программа запущена⭐️")

    run_price_updates()  # Обновление остатков складов
    run_safe("проверке новых заказов маркетплейсов", check_for_new_orders)
    run_safe("обновлении всех цен", update_all_prices)

    logger.success("🏁 Все задачи завершены успешно")


if __name__ == "__main__":
    main()
