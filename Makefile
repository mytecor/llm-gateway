.PHONY: all build test test-race lint run clean check-config

APP_NAME := llm-gateway
CMD_DIR := ./cmd/llm-gateway
BIN_DIR := bin
BINARY := $(BIN_DIR)/$(APP_NAME)
CONFIG ?= configs/config.example.json

all: build

build:
	go build -v -o $(BINARY) $(CMD_DIR)

run:
	go run $(CMD_DIR) --config $(CONFIG) check

check-config:
	go run $(CMD_DIR) --config $(CONFIG) check

test:
	go test -v ./...

test-race:
	go test -race -v ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint is not installed, running go vet instead"; \
		go vet ./...; \
	fi

clean:
	rm -rf $(BIN_DIR)
