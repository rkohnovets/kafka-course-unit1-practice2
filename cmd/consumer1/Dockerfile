# Этап сборки
FROM golang:1.25 AS builder

# Установка librdkafka-dev из пакетного менеджера
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Сборка (CGO_ENABLED=1 по умолчанию в Debian-образах)
RUN go build -o consumer ./cmd/consumer1

# Финальный образ
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /root
COPY --from=builder /app/consumer .
CMD ["./consumer"]