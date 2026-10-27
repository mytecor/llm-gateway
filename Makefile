.PHONY: all build build-core build-all build-drivers build-http build-acp build-web test test-unit test-core test-http test-acp test-web-unit test-race lint run clean check-config test-web

APP_NAME := llm-gateway
CMD_DIR := ./cmd/llm-gateway
BIN_DIR := bin
BINARY := $(BIN_DIR)/$(APP_NAME)
HTTP_DRIVER := $(BIN_DIR)/http-driver
ACP_DRIVER := $(BIN_DIR)/acp-driver
WEB_DRIVER := $(BIN_DIR)/web-driver
CONFIG ?= configs/config.example.json
DRIVERS ?= http acp web

all: build

# The default build is enough for the example HTTP configuration.
build: build-core build-http

# The gateway core needs only Go; each driver is independently selectable.
build-core: | $(BIN_DIR)
	go build -v -o $(BINARY) $(CMD_DIR)

build-all: build-core build-drivers

build-drivers: $(addprefix build-,$(DRIVERS))

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

build-http: | $(BIN_DIR)
	cd drivers/http && go build -v -o ../../$(HTTP_DRIVER) .

build-acp: | $(BIN_DIR)
	cd drivers/acp && go build -v -o ../../$(ACP_DRIVER) .

build-web: | $(BIN_DIR)
	cargo build --release --locked --manifest-path drivers/web/Cargo.toml
	cp drivers/web/target/release/web-driver $(WEB_DRIVER)

run: build
	$(BINARY) --config $(CONFIG) serve

check-config: build
	$(BINARY) --config $(CONFIG) check

# These unit packages never discover or start a bundled driver.
test-unit:
	go test ./driverproto ./internal/model ./internal/policy ./internal/session ./internal/telemetry

# The core integration suite validates all bundled driver contracts.
test-core: build-drivers
	go test -v ./...

test-http:
	cd drivers/http && go test -v ./...

test-acp:
	cd drivers/acp && go test -v ./...

test-web-unit:
	cargo test --locked --manifest-path drivers/web/Cargo.toml

test: test-core test-http test-acp test-web-unit

test-race: build-drivers
	go test -race -v ./...
	cd drivers/http && go test -race -v ./...
	cd drivers/acp && go test -race -v ./...
	cargo test --locked --manifest-path drivers/web/Cargo.toml

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

# Explicit opt-in browser integration tests; ordinary test runs stay offline.
test-web: test-web-unit
	cargo test --locked --manifest-path drivers/web/Cargo.toml --test browser -- --ignored --nocapture
