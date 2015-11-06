FROM fedora:22
MAINTAINER John Ford

# some of this is from: https://github.com/nodejs/docker-node/blob/d798690bdae91174715ac083e31198674f044b68/0.12/Dockerfile

LABEL description="This image is used to run the cloud mirror"
RUN dnf upgrade -y
RUN dnf install -y /usr/bin/gpg /usr/bin/tar libcurl-devel

ENV NODE_ENV ${NODE_ENV:-development}
ENV PORT ${PORT:-8080}
ENV APPDIR /app
WORKDIR /${APPDIR}

# Environment variables for setting up node
ENV NODE_VERSION ${NODE_VERSION:-0.12.7}
ENV NPM_VERSION ${NPM_VERSION:-2.11.2

RUN set -ex \
	&& for key in \
		7937DFD2AB06298B2293C3187D33FF9D0246406D \
		114F43EE0176B71C7BC219DD50A3051F888C628D \
	; do \
		gpg --keyserver ha.pool.sks-keyservers.net --recv-keys "$key"; \
	done

RUN curl -SLO "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-x64.tar.gz" \
	&& curl -SLO "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt.asc" \
	&& gpg --verify SHASUMS256.txt.asc \
	&& grep " node-v$NODE_VERSION-linux-x64.tar.gz\$" SHASUMS256.txt.asc | sha256sum -c - \
	&& tar -xzf "node-v$NODE_VERSION-linux-x64.tar.gz" -C /usr/local --strip-components=1 \
	&& rm "node-v$NODE_VERSION-linux-x64.tar.gz" SHASUMS256.txt.asc \
	&& npm install -g npm@"$NPM_VERSION" \
	&& npm cache clear

EXPOSE $PORT
ADD . ${APPDIR}

RUN cd ${APPDIR} && npm install .

CMD [ "node" ]
