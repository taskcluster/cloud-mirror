
.PHONY: build
build:
	npm run compile


.PHONY: build-docker-image
build-docker-image:
	sudo docker build -t cloud-mirror .

.PHONY: start-frontend
start-server:
	sudo docker run cloud-mirror node lib/frontend.js
