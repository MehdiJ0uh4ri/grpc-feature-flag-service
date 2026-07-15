.PHONY: proto build run-server run-client test lint up down

proto:
	buf generate

build:
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client

run-server:
	go run ./cmd/server

run-client:
	go run ./cmd/client

test:
	go test ./...

lint:
	buf lint

up:
	docker compose up --build

down:
	docker compose down -v
