SHELL := /bin/sh
.DEFAULT_GOAL := help

.PHONY: help bootstrap-check generate generate-check format format-check lint test test-unit build check ci compose-config compose-up compose-down clean

help: ## Show available commands.
	@awk 'BEGIN {FS = ":.*## "; printf "Epoch development commands:\n\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bootstrap-check: ## Print and validate the required local toolchain.
	@command -v go >/dev/null || { echo "missing Go 1.26.5" >&2; exit 1; }
	@go version
	@go version | grep -q 'go1\.26\.5' || { echo "expected Go 1.26.5" >&2; exit 1; }
	@command -v rustc >/dev/null || { echo "missing Rust 1.97.1" >&2; exit 1; }
	@rustc --version
	@rustc --version | grep -q '^rustc 1\.97\.1 ' || { echo "expected Rust 1.97.1" >&2; exit 1; }
	@cargo --version
	@rustfmt --version
	@cargo clippy --version
	@protoc --version
	@protoc --version | grep -q '35\.1$$' || { echo "expected protoc 35.1" >&2; exit 1; }
	@buf --version
	@buf --version | grep -q '^1\.72\.0$$' || { echo "expected Buf 1.72.0" >&2; exit 1; }
	@node --version
	@node -e 'if (Number(process.versions.node.split(".")[0]) !== 24) { console.error("expected Node.js 24 LTS; see docs/DEVELOPMENT.md"); process.exit(1) }'
	@pnpm --version
	@docker --version
	@docker compose version

generate: ## Generate language bindings from Protobuf contracts.
	@if find spec/proto -type f -name '*.proto' -print -quit 2>/dev/null | grep -q .; then buf generate; else echo "no Protobuf contracts found; skipping generation"; fi

generate-check: ## Fail when generated bindings are stale (requires Git).
	@$(MAKE) generate
	@if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then git diff --exit-code -- sdk/go/gen; else echo "not a Git worktree; generation diff check skipped"; fi

format: ## Format Rust, Go, and JavaScript/TypeScript sources.
	@if [ -f Cargo.toml ]; then cargo fmt --all; fi
	@files="$$(find control operator sdk/go -type f -name '*.go' 2>/dev/null)"; if [ -n "$$files" ]; then gofmt -w $$files; fi
	@pnpm run format

format-check: ## Check formatting without changing files.
	@if [ -f Cargo.toml ]; then cargo fmt --all --check; fi
	@files="$$(find control operator sdk/go -type f -name '*.go' 2>/dev/null)"; if [ -n "$$files" ]; then unformatted="$$(gofmt -l $$files)"; test -z "$$unformatted" || { printf '%s\n' "$$unformatted"; exit 1; }; fi
	@pnpm run format:check

lint: ## Run static checks for every language and contract.
	@if [ -f Cargo.toml ]; then cargo clippy --workspace --all-targets --all-features -- -D warnings; fi
	@if find control operator sdk/go -type f -name '*.go' -print -quit 2>/dev/null | grep -q .; then go vet ./...; fi
	@pnpm run lint
	@pnpm run typecheck
	@if find spec/proto -type f -name '*.proto' -print -quit 2>/dev/null | grep -q .; then buf lint; else echo "no Protobuf contracts found; skipping Buf lint"; fi

test: test-unit ## Run the default local test suite.

test-unit: ## Run unit tests for Rust, Go, and workspace packages.
	@if [ -f Cargo.toml ]; then cargo test --workspace --all-targets; fi
	@if find control operator sdk/go -type f -name '*.go' -print -quit 2>/dev/null | grep -q .; then go test -race ./...; fi
	@pnpm run test

build: ## Build all available workspace components.
	@if [ -f Cargo.toml ]; then cargo build --workspace --all-targets; fi
	@if find control operator sdk/go -type f -name '*.go' -print -quit 2>/dev/null | grep -q .; then go build ./...; fi
	@pnpm run build

check: generate-check format-check lint test-unit ## Run the local pre-commit gate.

ci: bootstrap-check check build compose-config ## Run the deterministic CI gate available locally.

compose-config: ## Validate the development Compose model.
	docker compose -f deploy/compose/docker-compose.yml config --quiet

compose-up: ## Build and start the standalone development node.
	docker compose -f deploy/compose/docker-compose.yml up --build --detach

compose-down: ## Stop the standalone development node without deleting its volume.
	docker compose -f deploy/compose/docker-compose.yml down

clean: ## Remove language build output (runtime data is retained).
	@if [ -f Cargo.toml ]; then cargo clean; fi
	@pnpm -r --if-present clean
