GO        ?= go
BIN       := $(CURDIR)/bin
PROTO_DIR := api/proto
GEN_DIR   := gen

PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

COMPOSE := docker compose -f deploy/docker-compose.yml

DEV_DATABASE_URL ?= postgres://controlplane:controlplane@localhost:5432/controlplane?sslmode=disable
DEV_REDIS_URL    ?= redis://localhost:6379/0

.PHONY: all tools proto build test test-race test-postgres vet fmt run run-memory compose-up compose-down docker clean

all: vet test build

tools: $(BIN)/protoc-gen-go $(BIN)/protoc-gen-go-grpc

$(BIN)/protoc-gen-go:
	GOBIN=$(BIN) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(BIN)/protoc-gen-go-grpc:
	GOBIN=$(BIN) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

proto: tools
	protoc -I $(PROTO_DIR) \
		--plugin=protoc-gen-go=$(BIN)/protoc-gen-go \
		--plugin=protoc-gen-go-grpc=$(BIN)/protoc-gen-go-grpc \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(shell find $(PROTO_DIR) -name '*.proto')

build:
	$(GO) build -o $(BIN)/controlplane ./cmd/controlplane
	$(GO) build -o $(BIN)/cpctl ./cmd/cpctl

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

# Runs the PostgreSQL conformance suite against DEV_DATABASE_URL (make compose-up first).
test-postgres:
	CONTROLPLANE_TEST_DATABASE_URL="$(DEV_DATABASE_URL)" $(GO) test -count=1 ./internal/store/postgres/

vet:
	$(GO) vet ./...

fmt:
	gofmt -s -w $(shell find . -name '*.go' -not -path './gen/*')

run: build
	$(BIN)/controlplane --database-url "$(DEV_DATABASE_URL)" --redis-url "$(DEV_REDIS_URL)" --log-format text

run-memory: build
	$(BIN)/controlplane --store memory --log-format text

compose-up:
	$(COMPOSE) up -d --wait

compose-down:
	$(COMPOSE) down -v

docker:
	docker build -t controlplane:dev .

clean:
	rm -rf $(BIN)
