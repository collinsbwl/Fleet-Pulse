.DEFAULT_GOAL := help

GO_PKGS := ./...

.PHONY: help build test fmt check

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
