BINARY = bin/meshcore-telegram-bot

.PHONY: build test vet fmt run docker-build docker-up docker-down clean

build:
	go build -o $(BINARY) ./cmd/meshcore-telegram-bot

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

run: build
	./$(BINARY)

docker-build:
	docker build -f docker/Dockerfile -t meshcore-telegram-bot .

# --project-directory . : compose ожидает .env рядом с собой, а в репозитории
# он лежит в корне (как на Synology — рядом с docker-compose.yml).
docker-up:
	docker compose --project-directory . -f docker/docker-compose.yml up -d

docker-down:
	docker compose --project-directory . -f docker/docker-compose.yml down

clean:
	rm -rf bin
