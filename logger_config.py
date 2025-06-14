from pathlib import Path
from loguru import logger
import sys

# Папка для логов
LOG_DIR = Path("System/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Удаляем все предыдущие обработчики
logger.remove()

# Формат лога
log_format = "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function}:{line} - {message}"

# Поток в консоль
logger.add(sys.stdout, format=log_format, level="DEBUG", colorize=False)

# Запись в файл
logger.add(
    LOG_DIR / "app.log",
    format=log_format,
    level="DEBUG",
    rotation="1 day",
    retention="7 days",
    encoding="utf-8"
)

# Экспорт
__all__ = ["logger", "LOG_DIR"]
