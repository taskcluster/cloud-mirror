VERSION=$(shell node -e "console.log(require('./package.json').version)")
REV=$(shell git rev-parse --revs-only --short --verify HEAD)
DATE=$(shell date +%Y-%m-%d)
TAG=$(VERSION)-$(DATE)-$(REV)

.PHONY: build
build:
	npm run compile


.PHONY: build-docker-image
build-docker-image:
	docker build -t cloud-mirror:$(TAG) .

.PHONY: push-docker-image
push-docker-image: build-docker-image
	docker pull tutum/haproxy
	docker pull tutum/redis
	docker tag -f tutum/haproxy:latest tutum.co/taskcluster/haproxy:latest
	docker push tutum.co/taskcluster/haproxy:latest
	docker tag -f tutum/redis:latest tutum.co/taskcluster/redis:latest
	docker push tutum.co/taskcluster/redis:latest
	docker tag -f cloud-mirror:$(TAG) cloud-mirror:latest
	docker tag -f cloud-mirror:$(TAG) tutum.co/taskcluster/cloud-mirror:$(TAG)
	docker tag -f cloud-mirror:latest tutum.co/taskcluster/cloud-mirror:latest
	docker push tutum.co/taskcluster/cloud-mirror:latest
	docker push tutum.co/taskcluster/cloud-mirror:$(TAG)

.PHONY: clean-all-docker
clean-all-docker:
	docker rm $(shell docker ps -a -q) || true
	docker rmi --force $(shell docker images -q)
