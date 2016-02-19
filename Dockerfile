FROM node:5.5
MAINTAINER John Ford
RUN DEBIAN_FRONTEND=noninteractive apt-get update -y
RUN DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y
RUN mkdir -p /app
WORKDIR /app
ENV NPM_CONFIG_LOGLEVEL=warn
COPY package.json /app/
RUN npm install -g npm@latest
RUN npm install .
COPY . /app/

ENTRYPOINT [ "node", "lib/main.js" ]
