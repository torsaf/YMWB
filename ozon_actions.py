import requests
import os
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime, timezone, timedelta
from notifiers import get_notifier
from pathlib import Path

# Загрузка переменных окружения
load_dotenv(dotenv_path=Path(__file__).resolve().parent / "System" / ".env")

# Telegram уведомления
telegram = get_notifier('telegram')
telegram_got_token = os.getenv('telegram_got_token')
telegram_chat_id = os.getenv('telegram_chat_id')

# Ozon API
OZON_API_KEY = os.getenv("ozon_API_key")
OZON_CLIENT_ID = os.getenv("ozon_client_ID")

HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json"
}

URL_LIST_ACTIONS = "https://api-seller.ozon.ru/v1/actions"
URL_GET_PRODUCTS = "https://api-seller.ozon.ru/v1/actions/products"
URL_REMOVE_PRODUCTS = "https://api-seller.ozon.ru/v1/actions/products/deactivate"

def format_iso_date(date_str, hide_year=False):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        dt = dt.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=3)))  # МСК
        if hide_year:
            return dt.strftime("%d.%m %H:%M")
        return dt.strftime("%d.%m.%Y в %H:%M")
    except Exception:
        return date_str

def remove_all_products_from_all_actions(limit_per_page=100):
    logger.info("\n🚨 НАЧАЛО: отключение всех товаров из всех активных акций на Ozon\n")

    response = requests.get(URL_LIST_ACTIONS, headers=HEADERS)
    if response.status_code != 200:
        logger.error(f"❌ Ошибка при получении списка акций: {response.status_code} — {response.text}")
        return

    actions = response.json().get("result", [])
    logger.info(f"📋 Найдено акций: {len(actions)}\n")

    total_removed = 0
    telegram_report = [f"📋 Акций на Ozon: {len(actions)}\n"]

    for action in actions:
        action_id = action.get("id")
        action_title = action.get("title", "Без названия")
        date_start = format_iso_date(action.get("date_start", "—"), hide_year=True)
        date_end = format_iso_date(action.get("date_end", "—"), hide_year=True)
        is_participating = action.get("is_participating", False)
        product_count = action.get("participating_products_count", 0)

        if product_count == 0 and not is_participating:
            continue

        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"🔸 Акция: {action_title}")
        logger.info(f"🆔 ID акции: {action_id}")
        logger.info(f"📅 Период действия: с {date_start} до {date_end}")
        logger.info(f"📦 Товаров в акции: {product_count}")
        logger.info(f"📍 Участие продавца: {'ДА' if is_participating else 'НЕТ'}")

        report_block = [
            f"✔️ {action_title}",
            f"ID: {action_id}",
            f"с {date_start} до {date_end}",
            f"Товаров: {product_count} | Участие: {'ДА' if is_participating else 'НЕТ'}"
        ]

        all_product_ids = []
        last_id = ""

        while True:
            payload = {
                "action_id": action_id,
                "limit": limit_per_page,
                "last_id": last_id
            }
            r = requests.post(URL_GET_PRODUCTS, json=payload, headers=HEADERS)
            if r.status_code != 200:
                logger.error(f"❌ Ошибка при получении товаров акции {action_id}: {r.status_code} — {r.text}")
                break

            result = r.json().get("result", {})
            products = result.get("products", [])
            all_product_ids.extend([p["id"] for p in products])
            last_id = result.get("last_id", "")

            if not products or not last_id:
                break

        if not all_product_ids:
            logger.info("⏭ Комментарий: Нет товаров для удаления")
            report_block.append("Комментарий: Нет товаров для удаления")
            telegram_report.append("\n" + "\n".join(report_block))
            continue

        logger.info(f"🗑 Удаляем {len(all_product_ids)} товаров из акции: {action_title}")

        delete_payload = {
            "action_id": action_id,
            "product_ids": all_product_ids
        }

        del_response = requests.post(URL_REMOVE_PRODUCTS, json=delete_payload, headers=HEADERS)
        if del_response.status_code != 200:
            logger.error(f"❌ Ошибка при удалении товаров из акции {action_id}: {del_response.status_code} — {del_response.text}")
            continue

        del_result = del_response.json().get("result", {})
        removed = del_result.get("product_ids", [])
        rejected = del_result.get("rejected", [])

        logger.success(f"✅ Удалено из акции: {len(removed)} товаров")
        report_block.append(f"Удалено: {len(removed)} товаров")
        if rejected:
            logger.warning(f"⚠ Не удалось удалить: {len(rejected)} товаров — {rejected}")
            report_block.append(f"Не удалено: {len(rejected)} товаров")

        telegram_report.append("\n" + "\n".join(report_block))
        total_removed += len(removed)

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if total_removed:
        logger.info(f"🏁 Завершено. Всего удалено товаров из акций: {total_removed}")
        telegram_report.append(f"\n✅ Удалено всего товаров из акций: {total_removed} шт.")
        try:
            telegram.notify(
                token=telegram_got_token,
                chat_id=telegram_chat_id,
                message="\n".join(telegram_report),
                parse_mode='markdown'
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке Telegram-уведомления: {e}")
    else:
        logger.info("🏁 Завершено. Товаров в акциях не было — удаление не потребовалось.")

if __name__ == "__main__":
    remove_all_products_from_all_actions()
