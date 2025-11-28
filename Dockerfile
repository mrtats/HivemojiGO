FROM golang:1.21 AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o hivemoji ./cmd/server

FROM alpine:3.18

RUN adduser -S -D appuser
WORKDIR /home/appuser

COPY --from=builder /app/hivemoji .
COPY --from=builder /app/web ./web

EXPOSE 8080
USER appuser

ENV SERVER_ADDR=":8080"

ENTRYPOINT ["./hivemoji"]
