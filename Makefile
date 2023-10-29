build:
	docker build . -t data_modelling

run: build
	docker run --rm -v $(shell pwd)/databases:/app/hello.db data_modelling
	# docker run --name data_modelling data_modelling && \
	# docker cp data_modelling:/app/hello.db hello.db; \
	# docker rm data_modelling

test: build
	docker run --rm -e PYTHONPATH=. data_modelling python3 -m pytest . -vv -rfE -s

connect_db:
	sqlite3 hello.db .tables
