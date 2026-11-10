# Сборка в отдельном контейнере, запуск — минимальный Alpine.
# Multi-arch (linux/amd64, linux/arm64): Go-сборка идёт под архитектуру хоста
# через кросс-компиляцию (TARGETARCH), поэтому QEMU-эмуляция не нужна.
# Контекст сборки — корень репозитория (см. docker/docker-compose.yml).
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETARCH
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" \
	-o /out/meshcore-telegram-bot ./cmd/meshcore-telegram-bot

FROM alpine:3.20

# CA-сертификаты: Telegram API по HTTPS; su-exec — сброс привилегий после chown
RUN apk add --no-cache ca-certificates su-exec \
	&& adduser -D -u 10001 bot

WORKDIR /app
COPY --from=build /out/meshcore-telegram-bot .
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# entrypoint стартует от root, делает chown /app/data и переходит на bot
ENTRYPOINT ["/entrypoint.sh"]
