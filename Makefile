.PHONY: start-deps start-example down

start-mongo-deps:
	docker-compose up -d mongo mongo-express

start-esdb-deps:
	docker-compose up -d eventstoredb

start-example:
	docker-compose up --build example-app

down:
	docker-compose down -v
