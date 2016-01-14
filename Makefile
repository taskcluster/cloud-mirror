SHORT_REV=$(shell git rev-parse --revs-only --short --verify HEAD)

.PHONY: build
build:
	npm run compile


.PHONY: build-docker-images
build-docker-images:
	sudo docker build -t cloud-mirror-base:$(SHORT_REV) .
