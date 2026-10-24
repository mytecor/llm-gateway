.PHONY: all build build-drivers build-http build-acp build-web test test-race lint run clean check-config test-web

APP_NAME := llm-gateway
CMD_DIR := ./cmd/llm-gateway
BIN_DIR := bin
BINARY := $(BIN_DIR)/$(APP_NAME)
HTTP_DRIVER := $(BIN_DIR)/http-driver
ACP_DRIVER := $(BIN_DIR)/acp-driver
CONFIG ?= configs/config.example.json

all: build

build: build-drivers
	go build -v -o $(BINARY) $(CMD_DIR)

build-drivers: build-http build-acp

build-http:
	cd drivers/http && go build -v -o ../../$(HTTP_DRIVER) .

build-acp:
	cd drivers/acp && go build -v -o ../../$(ACP_DRIVER) .

run:
	go run $(CMD_DIR) --config $(CONFIG) check

check-config:
	go run $(CMD_DIR) --config $(CONFIG) check

test: build-drivers
	go test -v ./...
	cd drivers/http && go test -v ./...
	cd drivers/acp && go test -v ./...

test-race: build-drivers
	go test -race -v ./...
	cd drivers/http && go test -race -v ./...
	cd drivers/acp && go test -race -v ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./... && \
		(cd drivers/http && golangci-lint run ./...) && \
		(cd drivers/acp && golangci-lint run ./...); \
	else \
		echo "golangci-lint is not installed, running go vet instead"; \
		go vet ./... && \
		(cd drivers/http && go vet ./...) && \
		(cd drivers/acp && go vet ./...); \
	fi

clean:
	rm -rf $(BIN_DIR)

# The Rust parser must be checked out at ../parser.
build-web:
	cargo build --release --locked --manifest-path drivers/web/Cargo.toml

test-web:
	cargo test --locked --manifest-path drivers/web/Cargo.toml
	cargo test --locked --manifest-path drivers/web/Cargo.toml --test browser -- --ignored --nocapture
