#!/usr/bin/env bash
# Деплой parking-bot на VPS (Ubuntu 24.04).
# Запускать на сервере: sudo bash setup-vps.sh
set -euo pipefail

APP_DIR=/opt/parking-bot
SERVICE=parking-bot

if [ "$EUID" -ne 0 ]; then echo "Запусти через sudo"; exit 1; fi

echo "==> Установка Python и git"
apt-get update -qq
apt-get install -y python3 python3-venv python3-pip git

echo "==> Копирование проекта в $APP_DIR"
mkdir -p "$APP_DIR"
# Предполагается, что скрипт запущен из корня репозитория
cp -r bot.py requirements.txt .env "$APP_DIR"/

if [ ! -f "$APP_DIR/.env" ]; then
    echo "ОШИБКА: положи .env рядом с setup-vps.sh"; exit 1
fi
chmod 600 "$APP_DIR/.env"

echo "==> Виртуальное окружение"
python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/pip" install --upgrade pip -q
"$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt" -q

echo "==> Ограничение размера журнала (защита диска от переполнения логами)"
mkdir -p /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/size-limit.conf <<'JCONF'
[Journal]
SystemMaxUse=200M
MaxRetentionSec=14day
JCONF

echo "==> Systemd сервис"
cp deploy/parking-bot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now "$SERVICE"

echo "==> Готово. Статус:"
systemctl --no-pager --lines=20 status "$SERVICE"
