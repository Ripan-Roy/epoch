SHELL := /bin/sh
.DEFAULT_GOAL := help

# Prefer the repository's pinned keg-only Node LTS on Homebrew systems without
# changing the user's globally linked Node version. On other systems the
# nonexistent prefix is harmless and the normal PATH remains effective.
NODE_LTS := $(if $(wildcard /opt/homebrew/opt/node@24/bin/node),/opt/homebrew/opt/node@24/bin/node,node)
PNPM_ENV := PATH="/opt/homebrew/opt/node@24/bin:$$PATH"
JAVA_MVN := ./sdk/java/mvnw --file sdk/java/pom.xml --batch-mode --no-transfer-progress

.PHONY: help bootstrap-check generate generate-check format format-check lint audit test test-unit test-integration build check ci compose-config compose-up compose-down clean

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
	@python3 --version
	@python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else "expected Python 3.11 or newer")'
	@command -v java >/dev/null || { echo "missing Java 25 or newer" >&2; exit 1; }
	@java -version
	@javac -version
	@java -version 2>&1 | awk -F'[ ."]' 'NR == 1 {major = ($$2 == "1" ? $$3 : $$2); exit !(major >= 25)}' || { echo "expected Java 25 or newer" >&2; exit 1; }
	@$(JAVA_MVN) --version | grep -q '^Apache Maven 3\.9\.16 ' || { echo "expected Maven wrapper 3.9.16" >&2; exit 1; }
	@ruff --version
	@ruff --version | grep -q '^ruff 0\.15\.19$$' || { echo "expected Ruff 0.15.19" >&2; exit 1; }
	@actionlint --version
	@actionlint --version | grep -q '^1\.7\.12$$' || { echo "expected actionlint 1.7.12" >&2; exit 1; }
	@shellcheck --version | grep -q '^version: 0\.11\.0$$' || { echo "expected ShellCheck 0.11.0" >&2; exit 1; }
	@protoc --version
	@protoc --version | grep -q '35\.1$$' || { echo "expected protoc 35.1" >&2; exit 1; }
	@buf --version
	@buf --version | grep -q '^1\.72\.0$$' || { echo "expected Buf 1.72.0" >&2; exit 1; }
	@$(NODE_LTS) --version
	@$(NODE_LTS) -e 'if (Number(process.versions.node.split(".")[0]) !== 24) { console.error("expected Node.js 24 LTS; see docs/DEVELOPMENT.md"); process.exit(1) }'
	@$(PNPM_ENV) pnpm --version
	@docker --version
	@docker compose version

generate: ## Generate language bindings from Protobuf contracts.
	@if find spec/proto -type f -name '*.proto' -print -quit 2>/dev/null | grep -q .; then buf generate; else echo "no Protobuf contracts found; skipping generation"; fi

generate-check: ## Fail when generated bindings are stale.
	@epoch_generate_snapshot="$$(mktemp -d "$${TMPDIR:-/tmp}/epoch-generate.XXXXXX")"; \
	trap 'rm -rf -- "$$epoch_generate_snapshot"' EXIT INT TERM; \
	if [ -d sdk/go/gen ]; then cp -R sdk/go/gen "$$epoch_generate_snapshot/generated"; else mkdir "$$epoch_generate_snapshot/generated"; fi; \
	$(MAKE) generate; \
	diff -ru "$$epoch_generate_snapshot/generated" sdk/go/gen

format: ## Format Rust, Go, Java, Python, and JavaScript/TypeScript sources.
	@if [ -f Cargo.toml ]; then cargo fmt --all; fi
	@files="$$(find control operator sdk/go -type f -name '*.go' 2>/dev/null)"; if [ -n "$$files" ]; then gofmt -w $$files; fi
	@if [ -d sdk/python ]; then ruff format sdk/python; fi
	@if [ -f sdk/java/pom.xml ]; then $(JAVA_MVN) spotless:apply; fi
	@$(PNPM_ENV) pnpm run format

format-check: ## Check formatting without changing files.
	@if [ -f Cargo.toml ]; then cargo fmt --all --check; fi
	@files="$$(find control operator sdk/go -type f -name '*.go' 2>/dev/null)"; if [ -n "$$files" ]; then unformatted="$$(gofmt -l $$files)"; test -z "$$unformatted" || { printf '%s\n' "$$unformatted"; exit 1; }; fi
	@if [ -d sdk/python ]; then ruff format --check sdk/python; fi
	@if [ -f sdk/java/pom.xml ]; then $(JAVA_MVN) spotless:check; fi
	@$(PNPM_ENV) pnpm run format:check

lint: ## Run static checks for every language and contract.
	@if [ -f Cargo.toml ]; then cargo clippy --locked --workspace --all-targets --all-features -- -D warnings; fi
	@if [ -f Cargo.toml ]; then RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps; fi
	@if find control operator sdk/go -type f -name '*.go' -print -quit 2>/dev/null | grep -q .; then go vet ./...; fi
	@if [ -d sdk/python ]; then ruff check sdk/python; fi
	@if [ -f sdk/java/pom.xml ]; then $(JAVA_MVN) -DskipTests verify; fi
	@if [ -d .github/workflows ]; then actionlint; fi
	@if [ -d tests/integration ]; then shellcheck scripts/*.sh tests/integration/*.sh; fi
	@$(PNPM_ENV) pnpm run lint
	@$(PNPM_ENV) pnpm run typecheck
	@if find spec/proto -type f -name '*.proto' -print -quit 2>/dev/null | grep -q .; then buf lint; else echo "no Protobuf contracts found; skipping Buf lint"; fi

audit: ## Reject Rust dependency advisories except the documented Raft exception.
	@cargo audit --version 2>/dev/null | grep -Eq '^cargo-audit(-audit)? 0\.22\.2$$' || { echo "missing cargo-audit 0.22.2; see docs/DEVELOPMENT.md" >&2; exit 1; }
	# The only temporary exception is Raft's unmaintained fxhash dependency;
	# ADR-0003 stays Proposed until the dependency decision is accepted.
	cargo audit --deny warnings --ignore RUSTSEC-2025-0057

test: test-unit ## Run the default local test suite.

test-unit: ## Run unit tests for Rust, Go, Java, Python, and workspace packages.
	@if [ -f Cargo.toml ]; then cargo test --locked --workspace --all-targets --all-features; fi
	@if find control operator sdk/go -type f -name '*.go' -print -quit 2>/dev/null | grep -q .; then go test -race ./...; fi
	@if [ -d sdk/python ]; then PYTHONPATH=sdk/python/src python3 -m unittest discover -s sdk/python/tests -v; fi
	@if [ -f sdk/java/pom.xml ]; then $(JAVA_MVN) test; fi
	@$(PNPM_ENV) pnpm run test

test-integration: ## Exercise real processes through the CLI and Go/Java/Python SDKs.
	@bash tests/integration/smoke.sh
	@bash tests/integration/docs-quickstarts.sh

build: ## Build all available workspace components.
	@if [ -f Cargo.toml ]; then cargo build --locked --workspace --all-targets --all-features; fi
	@if find control operator sdk/go -type f -name '*.go' -print -quit 2>/dev/null | grep -q .; then go build ./...; fi
	@if [ -d sdk/python ]; then python3 -m compileall -q sdk/python/src sdk/python/tests; fi
	@if [ -f sdk/java/pom.xml ]; then $(JAVA_MVN) -DskipTests package; fi
	@$(PNPM_ENV) pnpm run build

check: generate-check format-check lint test-unit audit ## Run the local pre-commit gate.

ci: bootstrap-check check build test-integration compose-config ## Run the deterministic CI gate available locally.

compose-config: ## Validate the development Compose model.
	docker compose -f deploy/compose/docker-compose.yml config --quiet

compose-up: ## Build and start the standalone development node.
	docker compose -f deploy/compose/docker-compose.yml up --build --detach

compose-down: ## Stop the standalone development node without deleting its volume.
	docker compose -f deploy/compose/docker-compose.yml down

clean: ## Remove language build output (runtime data is retained).
	@if [ -f Cargo.toml ]; then cargo clean; fi
	@if [ -f sdk/java/pom.xml ]; then $(JAVA_MVN) clean; fi
	@$(PNPM_ENV) pnpm -r --if-present clean
