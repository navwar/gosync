# ==================================================================================
#
# Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
# Released as open source under the MIT License.  See LICENSE file.
#
# ==================================================================================

.PHONY: help
help:  ## Print the help documentation
	@grep -E '^[/a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

#
# Go building, formatting, testing, and installing
#

.PHONY: check_headers
check_headers:  ## Check headers are correct
	scripts/check-headers headers/go.txt cmd pkg

fmt:  ## Format Go source code
	go fmt $$(go list ./... )

.PHONY: imports
imports: bin/goimports ## Update imports in Go source code
	bin/goimports -w \
	-local github.com/navwar/gosync,github.com/navwar \
	$$(find . -iname '*.go')

.PHONY: vet
vet: ## Vet Go source code
	go vet github.com/navwar/gosync/pkg/... # vet packages
	go vet github.com/navwar/gosync/cmd/... # vet commands

.PHONY: tidy
tidy: ## Tidy Go source code
	go mod tidy

.PHONY: test_go
test_go: bin/errcheck bin/misspell bin/staticcheck bin/shadow ## Run Go tests
	bash scripts/test.sh

.PHONY: test_cli
test_cli: bin/gosync ## Run CLI tests
	bash scripts/test-cli.sh

.PHONY: install
install:  ## Install the CLI on current platform
	go install github.com/navwar/gosync/cmd/gosync

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

bin/shadow:  ## Build shadow tool
	go build -o bin/shadow golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow

bin/gosync: ## Build gosync program for local operating system and architecture
	go build -o bin/gosync github.com/navwar/gosync/cmd/gosync

bin/gosync_linux_amd64: bin/gox ## Build gosync program for Linux on amd64
	scripts/build-release linux amd64

bin/minio_linux_amd64:
	curl -sSL https://dl.min.io/server/minio/release/linux-amd64/minio -o bin/minio_linux_amd64
	chmod +x bin/minio_linux_amd64

#
# Build Targets
#

.PHONY: build
build: bin/gosync  ## Build program for development

.PHONY: build_release
build_release: bin/gox  ## Build program for release
	scripts/build-release

.PHONY: rebuild
rebuild:  ## Rebuild binary
	rm -f bin/gosync
	make bin/gosync

#
# Local
#

.PHONY: sync_testdata
sync_testdata: bin/gosync  ## Sync using local binary
	# delete temp directory
	rm -fr temp
	# sync files
	bin/gosync sync \
	--parents \
	testdata \
	temp
	# sync again
	bin/gosync sync \
	--parents \
	testdata \
	temp

#
# Docker
#

.PHONY: docker_build
docker_build: ## Build the docker image
	docker build -f Dockerfile --tag gosync:latest .

.PHONY: docker_help
docker_help: ## Run the help command using docker image
	docker run -it --rm gosync:latest help

.PHONY: docker_sync_testdata
docker_sync_testdata: ## Sync using docker image
	# delete temp directory
	rm -fr temp
	# sync files
	docker run -it --rm -v $(PWD):/gosync gosync:latest sync \
	--parents \
	/gosync/testdata \
	/gosync/temp
	# sync again
	docker run -it --rm -v $(PWD):/gosync gosync:latest sync \
	--parents \
	/gosync/testdata \
	/gosync/temp

.PHONY: docker_version
docker_version:  ## Run the version command using docker image
	docker run -it --rm gosync:latest version

.PHONY: minio_serve
minio_serve:  ## Run a local minio server
	mkdir -p temp/minio
	minio server temp/minio/data

.PHONY: minio_serve_detached
minio_serve_detached:  ## Run a local minio server as detached process
	mkdir -p temp/minio
	scripts/minio-start temp/minio/data temp/minio/pid temp/minio/log

.PHONY: minio_serve_detached
minio_stop_detached:  ## Stop the local minio server running as as detached process
	scripts/pid-stop temp/minio/pid

.PHONY: minio_sync_testdata
minio_sync_testdata:  ## Sync the testdata directory to a local minio server and back to temp/testdata
	@scripts/minio-sync http://localhost:9000 testdata s3://testdata
	@scripts/minio-sync http://localhost:9000 s3://testdata s3://testdata2
	@scripts/minio-sync http://localhost:9000 s3://testdata2 s3://testdata
	@scripts/minio-sync http://localhost:9000 s3://testdata temp/testdata

## Clean

.PHONY: clean
clean:  ## Clean artifacts
	rm -fr bin
