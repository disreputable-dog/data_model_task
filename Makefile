build:
	docker build . -t data_modelling

run: build
	docker run --rm data_modelling

test: build
	docker run --rm -e PYTHONPATH=. data_modelling python3 -m pytest . -vv -rfE -s

