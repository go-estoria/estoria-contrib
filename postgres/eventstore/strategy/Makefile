.PHONY: itest itestv

ITEST_TIMEOUT ?= 30s

# Run integration tests
itest:
	go test -timeout $(ITEST_TIMEOUT) -failfast -run Integration .

# Run integration tests in verbose mode
itestv:
	go clean -testcache
	go test -timeout $(ITEST_TIMEOUT) -failfast -run Integration -v .
