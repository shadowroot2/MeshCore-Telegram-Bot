#!/bin/sh
# Контейнер стартует от root, чтобы создать папку данных и отдать её боту:
# bind-монтирование с NAS приезжает с чужим владельцем, а папки данных
# может не существовать вовсе (daemon Synology не создаёт bind-источники).
set -e

if [ -z "$TOPICS_FILE" ]; then
	if [ -d /app/project ]; then
		TOPICS_FILE=/app/project/data/topics.json
	else
		TOPICS_FILE=/app/data/topics.json
	fi
fi
export TOPICS_FILE

DATA_DIR=$(dirname "$TOPICS_FILE")
mkdir -p "$DATA_DIR"
chown bot:bot "$DATA_DIR" 2>/dev/null || echo "entrypoint: chown $DATA_DIR failed, continue" >&2
chmod u+rwx "$DATA_DIR" 2>/dev/null || true

exec su-exec bot /app/meshcore-telegram-bot
