.PHONY: itest itestv

ITEST_TIMEOUT ?= 120s

# Run integration tests
itest:
	go test -race -timeout $(ITEST_TIMEOUT) -failfast .

# Run integration tests in verbose mode
itestv:
	go clean -testcache
	go test -race -timeout $(ITEST_TIMEOUT) -failfast -v .
