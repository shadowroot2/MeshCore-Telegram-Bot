# Сборка в отдельном контейнере, запуск — минимальный Alpine.
# Контекст сборки — корень репозитория (см. docker/docker-compose.yml).
FROM golang:1.27-alpine AS build
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
	-o /out/meshcore-telegram-bot ./cmd/meshcore-telegram-bot

FROM alpine:3.20

# CA-сертификаты: Telegram API по HTTPS
RUN apk add --no-cache ca-certificates \
	&& adduser -D -u 10001 bot

WORKDIR /app
COPY --from=build /out/meshcore-telegram-bot .

USER bot
ENTRYPOINT ["/app/meshcore-telegram-bot"]
