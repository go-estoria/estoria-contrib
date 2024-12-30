.PHONY: start-example start-esdb-deps start-mongo-deps start-redis-deps down

start-all-deps:
	docker-compose up -d eventstoredb mongo mongo-express redis redis-commander s3

start-example:
	docker-compose up --build example-app

start-esdb-deps:
	docker-compose up -d eventstoredb

start-mongo-deps:
	docker-compose up -d mongo mongo-express

start-redis-deps:
	docker-compose up -d redis redis-commander

start-s3-deps:
	docker-compose up -d s3

start-telemetry:
	docker-compose up -d grafana
	docker-compose up otel-collector jaeger prometheus

down:
	docker-compose down -v
