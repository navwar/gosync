# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

.PHONY: help
help:  ## Print the help documentation
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

#
# Go building, formatting, testing, and installing
#

fmt:  ## Format Go source code
	go fmt $$(go list ./... )

.PHONY: imports
imports: bin/goimports ## Update imports in Go source code
	bin/goimports -w \
	-local github.com/deptofdefense/gosync,github.com/deptofdefense \
	$$(find . -iname '*.go')

vet: ## Vet Go source code
	go vet github.com/deptofdefense/gosync/pkg/... # vet packages
	go vet github.com/deptofdefense/gosync/cmd/... # vet commands

tidy: ## Tidy Go source code
	go mod tidy

.PHONY: test_go
test_go: bin/errcheck bin/misspell bin/staticcheck bin/shadow ## Run Go tests
	bash scripts/test.sh

.PHONY: test_cli
test_cli: bin/gosync ## Run CLI tests
	bash scripts/test-cli.sh

install:  ## Install the CLI on current platform
	go install github.com/deptofdefense/gosync/cmd/gosync

#
# Command line Programs
#

bin/errcheck:
	go build -o bin/errcheck github.com/kisielk/errcheck

bin/goimports:
	go build -o bin/goimports golang.org/x/tools/cmd/goimports

bin/gox:
	go build -o bin/gox github.com/mitchellh/gox

bin/misspell:
	go build -o bin/misspell github.com/client9/misspell/cmd/misspell

bin/staticcheck:
	go build -o bin/staticcheck honnef.co/go/tools/cmd/staticcheck

bin/shadow:
	go build -o bin/shadow golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow

bin/gosync: ## Build gosync CLI for Darwin / amd64
	go build -o bin/gosync github.com/deptofdefense/gosync/cmd/gosync

bin/gosync_linux_amd64: bin/gox ## Build gosync CLI for Darwin / amd64
	scripts/build-release linux amd64

#
# Build Targets
#

.PHONY: build
build: bin/gosync

.PHONY: build_release
build_release: bin/gox
	scripts/build-release

.PHONY: rebuild
rebuild:
	rm -f bin/gosync
	make bin/gosync

#
# Local
#

sync_example: bin/gosync  ## Sync using local binary
	bin/gosync sync \
	temp/src \
	temp/dst

#
# Docker
#

docker_build:
	docker build -f Dockerfile --tag gosync:latest .

docker_help: ## Run the help command using docker server image
	docker run -it --rm gosync:latest help

docker_sync_example: ## Sync using docker server image
	docker run -it --rm -p 8080:8080 -v $(PWD):/gosync gosync:latest sync \
	/gosync/temp/src \
	/gosync/temp/dst

docker_version:  ## Run the version command using docker server image
	docker run -it --rm gosync:latest version

## Clean

.PHONY: clean
clean:  ## Clean artifacts
	rm -fr bin
