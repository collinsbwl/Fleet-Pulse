.DEFAULT_GOAL := help

GO_PKGS := ./...
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help build test fmt check up down topics psql

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all Go packages
	go build $(GO_PKGS)

test: ## Run unit tests
	go test $(GO_PKGS)

fmt: ## Format Go code
	gofmt -w .

check: ## Vet, test and check formatting (must pass before every commit)
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	go vet $(GO_PKGS)
	go test $(GO_PKGS)

up: ## Start the stack and create topics
	$(COMPOSE) up -d --wait
	$(COMPOSE) run --rm redpanda-init

down: ## Stop the stack and delete its data volumes
	$(COMPOSE) down -v --remove-orphans

topics: ## Create or show topics (idempotent)
	$(COMPOSE) run --rm redpanda-init

psql: ## Open a psql shell on the fleet database
	$(COMPOSE) exec postgres psql -U fleet -d fleet
