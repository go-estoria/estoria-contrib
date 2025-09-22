.PHONY: itest itestv atest atestv

ITEST_TIMEOUT ?= 120s
ATEST_TIMEOUT ?= 60s

# Run integration tests
itest: _require_docker
	go test -timeout $(ITEST_TIMEOUT) -failfast -run Integration ./...

# Run integration tests in verbose mode
itestv: _require_docker
	go clean -testcache
	go test -timeout $(ITEST_TIMEOUT) -failfast -run Integration -v ./...

# Run acceptance tests
atest: _require_docker
	go test -timeout $(ITEST_TIMEOUT) -failfast -run AcceptanceTest ./...

# Run acceptance tests in verbose mode
atestv: _require_docker
	go clean -testcache
	go test -timeout $(ITEST_TIMEOUT) -failfast -run AcceptanceTest -v ./...

_require_docker:
	@which docker > /dev/null || (echo "Docker is required to run this target" && exit 1)
