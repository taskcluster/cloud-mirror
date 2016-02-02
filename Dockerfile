FROM node:5-onbuild
MAINTAINER John Ford
EXPOSE 8080
ENTRYPOINT [ "node", "lib/main.js" ]
