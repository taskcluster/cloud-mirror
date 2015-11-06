.PHONY: build-docker-image
build-docker-image:
	sudo docker build -t s3-distribute .

start-frontend:
	sudo docker run s3-distribute node lib/frontend.js
