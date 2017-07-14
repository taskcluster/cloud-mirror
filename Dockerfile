FROM node:8.1.4
MAINTAINER John Ford
RUN DEBIAN_FRONTEND=noninteractive apt-get update -y
RUN DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y
RUN mkdir -p /app
WORKDIR /app
ENV NPM_CONFIG_LOGLEVEL=warn
COPY . /app/
RUN yarn

ENTRYPOINT [ "node", "lib/main.js", "backend"]
