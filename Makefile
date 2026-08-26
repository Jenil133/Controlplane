GO        ?= go
BIN       := $(CURDIR)/bin
PROTO_DIR := api/proto
GEN_DIR   := gen

PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

COMPOSE   := docker compose -f deploy/docker-compose.yml
KUSTOMIZE ?= kubectl kustomize
K8S_OUT   := dist/k8s

DEV_DATABASE_URL ?= postgres://controlplane:controlplane@localhost:5432/controlplane?sslmode=disable
DEV_REDIS_URL    ?= redis://localhost:6379/0

# make loadtest: the server(s) to load. Extra cpload flags go in
# LOADTEST_FLAGS, e.g. LOADTEST_FLAGS="--watchers 2000 --json"; with auth on,
# pass --token there or set CPLOAD_TOKEN.
LOADTEST_ADDRS ?= localhost:9090
LOADTEST_FLAGS ?=
# make example: extra flags for the checkout demo service.
EXAMPLE_FLAGS ?=
# make token: the name and role of the new API token.
TOKEN_NAME ?= dev
TOKEN_ROLE ?= admin

.PHONY: all tools proto build test test-race test-postgres vet fmt run run-memory compose-up compose-down docker loadtest example k8s-render token clean

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
	$(GO) build -o $(BIN)/cpload ./cmd/cpload

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

# Watchers plus a stream of writes against a running server (make run or
# run-memory); reports propagation latency and fails above the SLO.
loadtest:
	$(GO) run ./cmd/cpload --addrs $(LOADTEST_ADDRS) $(LOADTEST_FLAGS)

# The checkout demo service against a running server; its package
# documentation lists the cpctl commands that seed it.
example:
	$(GO) run ./examples/checkout $(EXAMPLE_FLAGS)

# Renders both kustomize overlays into $(K8S_OUT)/, to inspect, diff or
# apply with kubectl apply -f. A failed render leaves no file behind.
k8s-render:
	@mkdir -p $(K8S_OUT)
	@for overlay in dev prod; do \
		$(KUSTOMIZE) deploy/k8s/overlays/$$overlay > $(K8S_OUT)/$$overlay.yaml || { rm -f $(K8S_OUT)/$$overlay.yaml; exit 1; }; \
		echo "rendered $(K8S_OUT)/$$overlay.yaml"; \
	done

# Prints a new API token once, with the tokens-file entry that holds its hash.
token:
	$(GO) run ./cmd/cpctl token generate --name $(TOKEN_NAME) --role $(TOKEN_ROLE)

clean:
	rm -rf $(BIN) $(K8S_OUT)
