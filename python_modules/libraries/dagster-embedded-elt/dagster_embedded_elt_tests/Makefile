@PHONY: build run

build:
	docker build -t finance-postgres -f db/Dockerfile ./db

run: build
	docker run --name finance-db -p 5432:5432 -ti --rm finance-postgres
