.PHONY: start-example start-esdb-deps start-mongo-deps start-redis-deps down

start-all-deps:
	docker-compose up -d eventstoredb mongo mongo-express redis redis-commander s3

start-example:
	docker-compose up --build example-app

start-esdb-deps:
	docker-compose up -d eventstoredb

start-mongo-deps:
	docker-compose up -d mongo mongo-express

start-postgres-deps:
	docker-compose up -d postgres

start-redis-deps:
	docker-compose up -d redis redis-commander

start-s3-deps:
	docker-compose up -d s3

start-telemetry:
	docker-compose up -d grafana
	docker-compose up otel-collector jaeger prometheus

down:
	docker-compose down -v

.PHONY: itest itestv atest atestv

ITEST_TIMEOUT ?= 30s

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
