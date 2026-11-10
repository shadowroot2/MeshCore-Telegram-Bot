#!/bin/sh
# Контейнер стартует от root только затем, чтобы /app/data была доступна
# на запись боту: bind-монтирование с NAS приезжает с чужим владельцем.
set -e
mkdir -p /app/data
chown -R bot:bot /app/data
export TOPICS_FILE="${TOPICS_FILE:-/app/data/topics.json}"
exec su-exec bot /app/meshcore-telegram-bot
