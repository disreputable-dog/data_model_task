build:
	docker build . -t data_modelling

run: build
	docker run --rm -v $(shell pwd)/databases:/app/databases data_modelling

test: build
	docker run --rm -e PYTHONPATH=. data_modelling python3 -m pytest . -vv -rfE -s

connect_db:
	sqlite3 databases/orders.db
