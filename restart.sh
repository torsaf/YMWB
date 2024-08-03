#!/bin/bash

# Бесконечный цикл для перезапуска программы каждые 30 секунд
while true; do
    # Логирование времени начала выполнения скрипта
    echo "Запуск скрипта в $(date)" >> /home/f0429526/domains/f0429526.xsph.ru/ymwb/runner.log
    echo "Запуск скрипта в $(date)"

    # Активация виртуальной среды
    echo "Активация виртуальной среды..."
    source ~/python/bin/activate

    # Проверка успешности активации виртуальной среды
    if [ $? -ne 0 ]; then
        echo "Ошибка активации виртуальной среды!" >> /home/f0429526/domains/f0429526.xsph.ru/ymwb/runner.log
        sleep 5
        continue
    fi

    # Переход в директорию с вашим скриптом
    echo "Переход в директорию с вашим скриптом..."
    cd /home/f0429526/domains/f0429526.xsph.ru/ymwb/

    # Запуск Python-скрипта и ограничение времени выполнения до 30 секунд
    echo "Запуск Python скрипта..."
    timeout 86400 /home/f0429526/python/bin/python main.py

    # Проверка успешности выполнения Python-скрипта
    if [ $? -ne 0 ]; then
        echo "Python скрипт завершился с ошибкой!" >> /home/f0429526/domains/f0429526.xsph.ru/ymwb/runner.log
    fi

    echo "Python скрипт завершен."

    # Логирование времени завершения выполнения скрипта
    echo "Скрипт завершен в $(date)" >> /home/f0429526/domains/f0429526.xsph.ru/ymwb/runner.log
    echo "Программа завершена. Перезапуск через 5 секунд."

    # Ждем 5 секунд перед следующим запуском
    sleep 5
done
