# YM-WB-MM
Управление ценами и остатками для Yandex.Market, Wildberries и Megamarket с использованием данных из Google Таблиц и уведомлениями о новых заказах в Telegram.

# Система управления запасами
Этот проект представляет собой систему управления ценами и остатками, которая автоматически синхронизирует данные о товарах из Google Таблиц с платформами Wildberries, Yandex.Market и Megamarket. Также предусмотрены уведомления в Telegram о новых заказах и возникающих ошибках.

## Особенности

- **Обновление запасов:** Система обновляет информацию о товарах на Wildberries, Yandex.Market и Megamarket каждые 30 секунд, используя данные из Google Таблиц.
- **Уведомления о новых заказах:** Пользователи получают уведомления о новых заказах на всех платформах.
- **Настраиваемость:** Конфигурация системы осуществляется через переменные среды, что обеспечивает гибкость настройки для различных сред.
- **Обработка ошибок:** В случае возникновения проблем, пользователи получают уведомления в Telegram с описанием ошибки.
