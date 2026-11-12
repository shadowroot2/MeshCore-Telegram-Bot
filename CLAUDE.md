# MeshCore Telegram Bot

Двусторонний мост mesh-канала (ZephCore companion, TCP :5000) и группы Telegram.

## Структура

- `cmd/meshcore-telegram-bot/` — точка входа (сборка и запуск компонентов)
- `internal/config/` — конфиг: файл `.env` + переменные окружения
- `internal/telegram/` — клиент Telegram Bot API
- `internal/companion/` — companion-протокол радио (TCP, бинарные фреймы)
- `internal/bridge/` — связка сторон: вкладки (topics), доставка, форматирование, счётчик репиттеров
- `docker/` — Dockerfile и docker-compose

## Команды

- `make test` — `go test -race ./...`
- `make build` — бинарник в `bin/`
- `make docker-up` / `make docker-down` — контейнер через compose
- Перед сдачей: `make fmt && make vet && make test` — всё должно быть зелёным.

## Правила

- Конфиг — `.env` (в git не коммитить), шаблон `.env.example`.
- Секреты (BOT_TOKEN и т.п.) не коммитить и не зашивать в образ.
- Поведение, которое видит пользователь, фиксируется тестами.
- Сырой текст команд `$`/`$$` никогда не уходит в LoRa: `$` выполняет сам бот,
  `$$` шлётся только протокольно (TXT_TYPE_CLI_COMMAND) и только после
  подтверждённого допуска — иначе целевой узел показал бы команду владельцу.
- `internal/companion`: сокет радио читает только `pump`, она же маршрутизирует
  ack'и запросчикам (`request`/`requestFrames` работают лишь при живой помпе).
  `recv` из других мест не звать, помпу не блокировать долгой обработкой —
  иначе живые запросы ловят `ack timeout`.
