import html
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from logger_config import logger


PROJECT_DIR = Path(__file__).resolve().parent
SYSTEM_DIR = PROJECT_DIR / "System"
SYSTEM_DIR.mkdir(exist_ok=True)

load_dotenv(dotenv_path=SYSTEM_DIR / ".env")

os.chdir(PROJECT_DIR)


BASE_URL = os.getenv("ADVANTSHOP_BASE_URL", "https://mussic.ru").rstrip("/")
API_KEY = os.getenv("ADVANTSHOP_API_KEY")

TELEGRAM_TOKEN = os.getenv("telegram_got_token") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("telegram_chat_id") or os.getenv("TELEGRAM_CHAT_ID")

SHOP_TZ = ZoneInfo(os.getenv("SHOP_TIMEZONE", "Europe/Moscow"))

REQUEST_TIMEOUT_SECONDS = 30

# В документации AdvantShop: при LoadItems=true максимум 50
ITEMS_PER_PAGE = 50
LOAD_ITEMS = True

# Ищем все заказы в этом статусе, включая старые
WATCH_STATUS_NAME = "Новый"

# Сюда пишем только ID уже обработанных заказов
ORDERS_FILE = SYSTEM_DIR / "advantshop_order_ids.txt"

# Сюда пишем только ID уже обработанных лидов
LEADS_FILE = SYSTEM_DIR / "advantshop_lead_ids.txt"

# В документации для лидов itemsPerPage по умолчанию 100
LEADS_ITEMS_PER_PAGE = 100
LOAD_LEAD_ITEMS = True
LOAD_LEAD_CUSTOMER_FIELDS = True


class AdvantshopApiError(Exception):
    pass


class TelegramApiError(Exception):
    pass


def normalize_api_errors(errors) -> str:
    if isinstance(errors, str):
        return errors

    if isinstance(errors, list):
        return "; ".join(map(str, errors))

    return "Неизвестная ошибка API"


def post_advantshop_api(path: str, payload: dict | None = None) -> dict:
    if not API_KEY:
        raise AdvantshopApiError("Не задан API_KEY.")

    url = f"{BASE_URL}{path}"

    response = requests.post(
        url,
        params={"apikey": API_KEY},
        json=payload or {},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    try:
        data = response.json()
    except ValueError as exc:
        raise AdvantshopApiError(
            f"API вернул не JSON. HTTP {response.status_code}: {response.text[:500]}"
        ) from exc

    if data.get("result") is not True:
        raise AdvantshopApiError(normalize_api_errors(data.get("errors")))

    return data

def post_advantshop_raw_api(path: str, payload: dict | None = None) -> dict:
    if not API_KEY:
        raise AdvantshopApiError("Не задан API_KEY.")

    url = f"{BASE_URL}{path}"

    response = requests.post(
        url,
        params={"apikey": API_KEY},
        json=payload or {},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    try:
        data = response.json()
    except ValueError as exc:
        raise AdvantshopApiError(
            f"API вернул не JSON. HTTP {response.status_code}: {response.text[:500]}"
        ) from exc

    if data.get("result") is False:
        raise AdvantshopApiError(normalize_api_errors(data.get("errors")))

    if data.get("status") == "error":
        raise AdvantshopApiError(normalize_api_errors(data.get("errors")))

    return data

def extract_data_items(obj) -> list[dict]:
    if isinstance(obj, list):
        return obj

    if isinstance(obj, dict):
        data_items = obj.get("DataItems")
        if isinstance(data_items, list):
            return data_items

    return []


def parse_advantshop_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=SHOP_TZ)

    return dt.astimezone(SHOP_TZ)


def format_order_date(value: str | None) -> str:
    dt = parse_advantshop_datetime(value)

    if dt is None:
        return value or "-"

    return dt.strftime("%d.%m.%Y %H:%M")


def to_decimal(value) -> Decimal | None:
    if value is None or value == "":
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def format_money(value) -> str:
    amount = to_decimal(value)

    if amount is None:
        return "-"

    if amount == amount.to_integral_value():
        text = f"{int(amount):,}".replace(",", " ")
    else:
        rounded = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        text = f"{rounded:,.2f}".replace(",", " ").replace(".", ",")

    return f"{text} р."


def format_quantity(value) -> str:
    amount = to_decimal(value)

    if amount is None:
        return "-"

    if amount == amount.to_integral_value():
        text = str(int(amount))
    else:
        text = format(amount.normalize(), "f").replace(".", ",")

    return f"{text} шт."


def get_order_id(order: dict) -> str:
    return str(order.get("Id") or "").strip()


def load_saved_order_ids() -> set[str]:
    if not ORDERS_FILE.exists():
        return set()

    saved_ids = set()

    with ORDERS_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            order_id = line.strip()

            if order_id:
                saved_ids.add(order_id)

    return saved_ids


def save_order_id(order: dict) -> None:
    order_id = get_order_id(order)

    if not order_id:
        return

    with ORDERS_FILE.open("a", encoding="utf-8") as file:
        file.write(f"{order_id}\n")

def get_lead_id(lead: dict) -> str:
    return str(lead.get("id") or "").strip()


def load_saved_lead_ids() -> set[str]:
    if not LEADS_FILE.exists():
        return set()

    saved_ids = set()

    with LEADS_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            lead_id = line.strip()

            if lead_id:
                saved_ids.add(lead_id)

    return saved_ids


def save_lead_id(lead: dict) -> None:
    lead_id = get_lead_id(lead)

    if not lead_id:
        return

    with LEADS_FILE.open("a", encoding="utf-8") as file:
        file.write(f"{lead_id}\n")


def fetch_order_statuses() -> list[dict]:
    data = post_advantshop_api("/api/orderstatus/getlist")
    return extract_data_items(data.get("obj"))


def get_status_id_by_name(status_name: str) -> int:
    statuses = fetch_order_statuses()

    for status in statuses:
        current_name = str(status.get("Name", "")).strip().lower()

        if current_name == status_name.strip().lower():
            return int(status["Id"])

    available_statuses = ", ".join(
        f'{status.get("Name")}={status.get("Id")}'
        for status in statuses
    )

    raise AdvantshopApiError(
        f'Статус "{status_name}" не найден. Доступные статусы: {available_statuses}'
    )


def fetch_orders_by_status(status_id: int) -> list[dict]:
    page = 1
    orders: list[dict] = []

    while True:
        payload = {
            "Page": page,
            "ItemsPerPage": ITEMS_PER_PAGE,
            "LoadItems": LOAD_ITEMS,
            "StatusId": status_id,
        }

        data = post_advantshop_api("/api/order/getlist", payload)

        obj = data.get("obj") or {}
        data_items = extract_data_items(obj)

        orders.extend(data_items)

        total_pages = int(obj.get("TotalPageCount") or 1)

        if page >= total_pages:
            break

        page += 1

    return orders

def fetch_leads() -> list[dict]:
    page = 1
    leads: list[dict] = []

    while True:
        payload = {
            "page": page,
            "itemsPerPage": LEADS_ITEMS_PER_PAGE,
            "loadItems": LOAD_LEAD_ITEMS,
            "loadCustomerFields": LOAD_LEAD_CUSTOMER_FIELDS,
        }

        data = post_advantshop_raw_api("/api/leads/getlist", payload)

        data_items = data.get("dataItems") or []

        if not isinstance(data_items, list):
            data_items = []

        leads.extend(data_items)

        total_pages = int(data.get("totalPageCount") or 1)

        if page >= total_pages:
            break

        page += 1

    return leads

def format_customer(order: dict) -> str:
    customer = order.get("Customer") or {}

    full_name = " ".join(
        part
        for part in [
            customer.get("LastName"),
            customer.get("FirstName"),
            customer.get("Patronymic"),
        ]
        if part
    )

    phone = customer.get("Phone") or "-"
    email = customer.get("Email") or "-"

    if not full_name:
        full_name = customer.get("Organization") or "-"

    return f"{full_name}, телефон: {phone}, email: {email}"

def clean_text(value) -> str:
    text = str(value or "").strip()

    if not text or text in {"-", "—", "None", "null"}:
        return ""

    return text


def build_delivery_address_parts(customer: dict) -> list[str]:
    country = clean_text(customer.get("Country"))
    region = clean_text(customer.get("Region"))
    district = clean_text(customer.get("District"))
    city = clean_text(customer.get("City"))
    zip_code = clean_text(customer.get("Zip"))

    street = clean_text(customer.get("Street"))
    house = clean_text(customer.get("House"))
    structure = clean_text(customer.get("Structure"))
    apartment = clean_text(customer.get("Apartment"))
    entrance = clean_text(customer.get("Entrance"))
    floor = clean_text(customer.get("Floor"))

    parts = []

    if city:
        parts.append(f"Город: {city}")

    if region and region.lower() != city.lower():
        parts.append(f"Регион: {region}")

    if district:
        parts.append(f"Район: {district}")

    if country and country.lower() != "россия":
        parts.append(f"Страна: {country}")

    address_parts = []

    if street:
        address_parts.append(street)

    if house:
        address_parts.append(f"д. {house}")

    if structure:
        address_parts.append(f"стр./корп. {structure}")

    if apartment:
        address_parts.append(f"кв. {apartment}")

    if entrance:
        address_parts.append(f"подъезд {entrance}")

    if floor:
        address_parts.append(f"этаж {floor}")

    if address_parts:
        parts.append(f"Адрес: {', '.join(address_parts)}")

    if zip_code:
        parts.append(f"Индекс: {zip_code}")

    return parts


def format_delivery_for_telegram(order: dict) -> str:
    customer = order.get("Customer") or {}
    shipping_name = clean_text(order.get("ShippingName")) or "-"

    lines = [
        "<b>🚚 Доставка:</b>",
        html.escape(shipping_name),
    ]

    for address_part in build_delivery_address_parts(customer):
        lines.append(html.escape(address_part))

    return "\n".join(lines)


def get_lead_value(lead: dict, *keys, default="-"):
    for key in keys:
        value = lead.get(key)

        if value not in (None, ""):
            return value

    return default


def format_lead_full_name(lead: dict) -> str:
    full_name = " ".join(
        part
        for part in [
            lead.get("lastName"),
            lead.get("firstName"),
            lead.get("patronymic"),
        ]
        if part
    )

    return full_name or "-"


def format_lead_items_for_telegram(lead: dict) -> str:
    items = lead.get("leadItems") or []
    lead_sum = format_money(lead.get("sum"))

    if not items:
        title = clean_text(lead.get("title"))
        description = clean_text(lead.get("description"))

        lines = ["<b>🎹 Лид:</b>"]

        if title:
            lines.append(html.escape(title))

        if description and description != title:
            lines.append(f"Описание: {html.escape(description)}")

        lines.append(f"Сумма: {html.escape(lead_sum)}")

        return "\n".join(lines)

    if len(items) == 1:
        item = items[0]

        art_no = get_lead_value(item, "ArtNo", "artNo", "artno", default="-")
        name = get_lead_value(item, "Name", "name", default="-")
        amount = format_quantity(get_lead_value(item, "Amount", "amount", default=1))
        price = format_money(get_lead_value(item, "Price", "price", default=None))

        qty_marker = " 🆘" if (to_decimal(get_lead_value(item, "Amount", "amount", default=1)) or Decimal("0")) > 1 else ""

        return "\n".join(
            [
                "<b>🎹 Товар / интерес:</b>",
                f"{html.escape(str(name))} — {html.escape(amount)}{qty_marker}",
                f"Артикул: {html.escape(str(art_no))}",
                f"Цена: {html.escape(price)}",
                f"Сумма лида: {html.escape(lead_sum)}",
            ]
        )

    lines = ["<b>🎹 Товары / интерес:</b>"]

    for index, item in enumerate(items, start=1):
        art_no = get_lead_value(item, "ArtNo", "artNo", "artno", default="-")
        name = get_lead_value(item, "Name", "name", default="-")
        amount = format_quantity(get_lead_value(item, "Amount", "amount", default=1))
        price = format_money(get_lead_value(item, "Price", "price", default=None))

        qty_marker = " 🆘" if (to_decimal(get_lead_value(item, "Amount", "amount", default=1)) or Decimal("0")) > 1 else ""

        lines.extend(
            [
                f"{index}) {html.escape(str(name))} — {html.escape(amount)}{qty_marker}",
                f"Артикул: {html.escape(str(art_no))}",
                f"Цена: {html.escape(price)}",
                "",
            ]
        )

    lines.append(f"Сумма лида: {html.escape(lead_sum)}")

    return "\n".join(lines).strip()


def format_lead_location_for_telegram(lead: dict) -> str:
    city = clean_text(lead.get("city"))
    region = clean_text(lead.get("region"))
    district = clean_text(lead.get("district"))
    country = clean_text(lead.get("country"))
    zip_code = clean_text(lead.get("zip"))

    lines = []

    if city:
        lines.append(f"Город: {html.escape(city)}")

    if region and region.lower() != city.lower():
        lines.append(f"Регион: {html.escape(region)}")

    if district:
        lines.append(f"Район: {html.escape(district)}")

    if country and country.lower() != "россия":
        lines.append(f"Страна: {html.escape(country)}")

    if zip_code:
        lines.append(f"Индекс: {html.escape(zip_code)}")

    if not lines:
        return ""

    return "\n".join(["<b>📍 География:</b>", *lines])


def build_lead_text_for_telegram(lead: dict) -> str:
    lead_id = get_lead_id(lead)

    full_name = format_lead_full_name(lead)
    phone = clean_text(lead.get("phone")) or "-"
    email = clean_text(lead.get("email")) or "-"

    customer_comment = clean_text(lead.get("customerComment"))
    description = clean_text(lead.get("description"))
    admin_comment = clean_text(lead.get("adminComment"))

    message_lines = [
        f"<b>=== Лид с сайта 🧲 #{html.escape(str(lead_id))} ===</b>",
        "",
        format_lead_items_for_telegram(lead),
    ]

    location_block = format_lead_location_for_telegram(lead)

    if location_block:
        message_lines.extend(["", location_block])

    message_lines.extend(
        [
            "",
            "<b>👤 Покупатель:</b>",
            html.escape(str(full_name)),
            html.escape(str(phone)),
            html.escape(str(email)),
        ]
    )

    if customer_comment:
        message_lines.extend(
            [
                "",
                "<b>💬 Комментарий покупателя:</b>",
                html.escape(customer_comment),
            ]
        )

    if description and description != customer_comment:
        message_lines.extend(
            [
                "",
                "<b>📝 Описание:</b>",
                html.escape(description),
            ]
        )

    if admin_comment:
        message_lines.extend(
            [
                "",
                "<b>🛠 Комментарий администратора:</b>",
                html.escape(admin_comment),
            ]
        )

    return "\n".join(message_lines)


def build_lead_text_for_console(lead: dict) -> str:
    lead_id = get_lead_id(lead)
    title = clean_text(lead.get("title")) or f"Лид #{lead_id}"
    created_date = lead.get("createdDateFormatted") or lead.get("createdDate") or "-"
    lead_sum = format_money(lead.get("sum"))
    deal_status_name = clean_text(lead.get("dealStatusName")) or "-"
    full_name = format_lead_full_name(lead)
    phone = clean_text(lead.get("phone")) or "-"
    email = clean_text(lead.get("email")) or "-"
    description = clean_text(lead.get("description")) or "-"

    separator = "=" * 100

    return "\n".join(
        [
            separator,
            f"Лид #{lead_id}: {title}",
            f"Дата: {created_date}",
            f"Сумма: {lead_sum}",
            f"Статус: {deal_status_name}",
            f"Покупатель: {full_name}, телефон: {phone}, email: {email}",
            f"Описание: {description}",
            separator,
        ]
    )


def process_new_lead(lead: dict) -> None:
    telegram_text = build_lead_text_for_telegram(lead)

    send_telegram_message(telegram_text)
    save_lead_id(lead)

    lead_id = get_lead_id(lead)
    lead_sum = format_money(lead.get("sum"))
    items_count = len(lead.get("leadItems") or [])

    logger.success(
        f"🌐 AdvantShop: отправлено уведомление по лиду "
        f"ID={lead_id} | сумма={lead_sum} | позиций={items_count}"
    )

def format_items_for_console(order: dict) -> str:
    items = order.get("Items") or []

    if not items:
        return "  Позиции: -"

    lines = ["  Позиции:"]

    for item in items:
        art_no = item.get("ArtNo") or "-"
        name = item.get("Name") or "-"
        amount = format_quantity(item.get("Amount"))
        price = format_money(item.get("Price"))

        lines.append(
            f"    - {art_no} | {name} | кол-во: {amount} | цена: {price}"
        )

    return "\n".join(lines)


def format_items_for_telegram(order: dict) -> str:
    items = order.get("Items") or []
    order_sum = format_money(order.get("Sum"))

    if not items:
        return "\n".join(
            [
                "<b>🎹 Товар:</b>",
                "товар не указан",
                f"К оплате: {html.escape(order_sum)}",
            ]
        )

    if len(items) == 1:
        item = items[0]

        art_no = item.get("ArtNo") or "-"
        name = item.get("Name") or "-"
        amount = format_quantity(item.get("Amount"))
        price = format_money(item.get("Price"))

        qty_marker = " 🆘" if (to_decimal(item.get("Amount")) or Decimal("0")) > 1 else ""

        return "\n".join(
            [
                "<b>🎹 Товар:</b>",
                f"{html.escape(str(name))} — {html.escape(amount)}{qty_marker}",
                f"Артикул: {html.escape(str(art_no))}",
                f"Цена: {html.escape(price)} (к оплате: {html.escape(order_sum)})",
            ]
        )

    lines = ["<b>🎹 Товары:</b>"]

    for index, item in enumerate(items, start=1):
        art_no = item.get("ArtNo") or "-"
        name = item.get("Name") or "-"
        amount = format_quantity(item.get("Amount"))
        price = format_money(item.get("Price"))

        qty_marker = " 🆘" if (to_decimal(item.get("Amount")) or Decimal("0")) > 1 else ""

        lines.extend(
            [
                f"{index}) {html.escape(str(name))} — {html.escape(amount)}{qty_marker}",
                f"Артикул: {html.escape(str(art_no))}",
                f"Цена: {html.escape(price)}",
                "",
            ]
        )

    lines.append(f"К оплате: {html.escape(order_sum)}")

    return "\n".join(lines).strip()


def build_order_text_for_console(order: dict) -> str:
    status = order.get("Status") or {}

    order_id = order.get("Id")
    number = order.get("Number") or "-"
    order_date = format_order_date(order.get("Date"))
    order_sum = format_money(order.get("Sum"))
    status_name = status.get("Name") or "-"
    is_paid = order.get("IsPaid")
    payment_name = order.get("PaymentName") or "-"
    shipping_name = order.get("ShippingName") or "-"
    customer_comment = order.get("CustomerComment") or "-"

    if is_paid is True:
        paid_text = "да"
    elif is_paid is False:
        paid_text = "нет"
    else:
        paid_text = "-"

    separator = "=" * 100

    return "\n".join(
        [
            separator,
            f"Заказ #{number} / ID: {order_id}",
            f"Дата: {order_date}",
            f"Сумма: {order_sum}",
            f"Статус: {status_name}",
            f"Оплачен: {paid_text}",
            f"Метод оплаты: {payment_name}",
            f"Метод доставки: {shipping_name}",
            f"Покупатель: {format_customer(order)}",
            f"Комментарий покупателя: {customer_comment}",
            format_items_for_console(order),
            separator,
        ]
    )


def build_order_text_for_telegram(order: dict) -> str:
    customer = order.get("Customer") or {}

    number = order.get("Number") or "-"
    payment_name = order.get("PaymentName") or "-"
    customer_comment = str(order.get("CustomerComment") or "").strip()

    full_name = " ".join(
        part
        for part in [
            customer.get("LastName"),
            customer.get("FirstName"),
            customer.get("Patronymic"),
        ]
        if part
    )

    if not full_name:
        full_name = customer.get("Organization") or "-"

    phone = clean_text(customer.get("Phone"))
    email = clean_text(customer.get("Email"))

    customer_lines = [
        "",
        "<b>👤 Покупатель:</b>",
        html.escape(str(full_name)),
    ]

    if phone:
        customer_lines.append(html.escape(phone))

    if email:
        customer_lines.append(html.escape(email))

    message_lines = [
        f"<b>=== С сайта 🛒 Заказ #{html.escape(str(number))} ===</b>",
        "",
        format_items_for_telegram(order),
        "",
        format_delivery_for_telegram(order),
        "",
        "<b>💳 Оплата:</b>",
        html.escape(str(payment_name)),
        *customer_lines,
    ]

    if customer_comment and customer_comment not in {"-", "—"}:
        message_lines.extend(
            [
                "",
                "<b>💬 Комментарий:</b>",
                html.escape(customer_comment),
            ]
        )

    return "\n".join(message_lines)


def split_telegram_message(message: str, limit: int = 3900) -> list[str]:
    if len(message) <= limit:
        return [message]

    parts = []
    current = ""

    for line in message.splitlines(keepends=True):
        if len(current) + len(line) > limit:
            parts.append(current)
            current = line
        else:
            current += line

    if current:
        parts.append(current)

    return parts


def send_telegram_message(message: str) -> None:
    if not TELEGRAM_TOKEN:
        raise TelegramApiError(
            "Не задан telegram_got_token в System/.env."
        )

    if not TELEGRAM_CHAT_ID:
        raise TelegramApiError(
            "Не задан telegram_chat_id в System/.env."
        )

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    for part in split_telegram_message(message):
        response = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": part,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        response.raise_for_status()

        data = response.json()

        if data.get("ok") is not True:
            description = data.get("description") or "Неизвестная ошибка Telegram API"
            raise TelegramApiError(description)


def process_new_order(order: dict) -> None:
    telegram_text = build_order_text_for_telegram(order)

    send_telegram_message(telegram_text)
    save_order_id(order)

    order_id = get_order_id(order)
    order_number = order.get("Number") or "-"
    order_sum = format_money(order.get("Sum"))
    items_count = len(order.get("Items") or [])

    logger.success(
        f"🌐 AdvantShop: отправлено уведомление по заказу "
        f"#{order_number} / ID={order_id} | сумма={order_sum} | позиций={items_count}"
    )


def check_advantshop_notifications() -> None:
    logger.info("🌐 AdvantShop: старт проверки заказов и лидов")
    logger.debug(f"🌐 AdvantShop: магазин={BASE_URL}")
    logger.debug(f"🌐 AdvantShop: файл заказов={ORDERS_FILE.resolve()}")
    logger.debug(f"🌐 AdvantShop: файл лидов={LEADS_FILE.resolve()}")

    # ===== ЗАКАЗЫ =====
    status_id = get_status_id_by_name(WATCH_STATUS_NAME)

    saved_order_ids = load_saved_order_ids()
    orders = fetch_orders_by_status(status_id)

    orders.sort(
        key=lambda order: parse_advantshop_datetime(order.get("Date"))
        or datetime.min.replace(tzinfo=SHOP_TZ),
        reverse=True,
    )

    new_orders = [
        order
        for order in orders
        if get_order_id(order) and get_order_id(order) not in saved_order_ids
    ]

    logger.info(
        f'🌐 AdvantShop: заказы в статусе "{WATCH_STATUS_NAME}" | '
        f"всего={len(orders)} | новых={len(new_orders)}"
    )

    for order in new_orders:
        process_new_order(order)
        saved_order_ids.add(get_order_id(order))

    if new_orders:
        send_telegram_message("📦")

    # ===== ЛИДЫ =====
    saved_lead_ids = load_saved_lead_ids()
    leads = fetch_leads()

    leads.sort(
        key=lambda lead: parse_advantshop_datetime(lead.get("createdDate"))
        or datetime.min.replace(tzinfo=SHOP_TZ),
        reverse=True,
    )

    new_leads = [
        lead
        for lead in leads
        if get_lead_id(lead) and get_lead_id(lead) not in saved_lead_ids
    ]

    logger.info(
        f"🌐 AdvantShop: лиды | всего={len(leads)} | новых={len(new_leads)}"
    )

    for lead in new_leads:
        process_new_lead(lead)
        saved_lead_ids.add(get_lead_id(lead))

    if new_leads:
        send_telegram_message("🧲")

    logger.success(
        f"🌐 AdvantShop: проверка завершена | "
        f"новых заказов={len(new_orders)} | новых лидов={len(new_leads)}"
    )