.PHONY: start-example start-esdb-deps start-mongo-deps start-redis-deps down

start-example:
	docker-compose up --build example-app

start-esdb-deps:
	docker-compose up -d eventstoredb

start-mongo-deps:
	docker-compose up -d mongo mongo-express

start-redis-deps:
	docker-compose up -d redis redis-commander

down:
	docker-compose down -v
