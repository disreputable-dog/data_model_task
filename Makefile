build:
	docker build . -t data_modelling

run: build
	docker run --rm data_modelling
