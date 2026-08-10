.PHONY: itest itestv atest atestv

# Each test function may wait up to three minutes for a KurrentDB container to elect a
# leader (see testutil_test.go), so per-run budgets must sit well above that.
ITEST_TIMEOUT ?= 20m
ATEST_TIMEOUT ?= 20m

# Run integration tests
itest:
	go test -timeout $(ITEST_TIMEOUT) -failfast -run Integration .

# Run integration tests in verbose mode
itestv:
	go clean -testcache
	go test -timeout $(ITEST_TIMEOUT) -failfast -run Integration -v .

# Run acceptance tests
atest:
	go test -timeout $(ATEST_TIMEOUT) -failfast -run AcceptanceTest .

# Run acceptance tests in verbose mode
atestv:
	go clean -testcache
	go test -timeout $(ATEST_TIMEOUT) -failfast -run AcceptanceTest -v .
