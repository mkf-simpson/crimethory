FROM node:6.9

RUN mkdir -p /opt/crimethory
WORKDIR /opt/crimethory
COPY . /opt/crimethory
COPY ./docker-entrypoint.sh /

RUN chmod +x /docker-entrypoint.sh
RUN /usr/local/bin/npm install

EXPOSE 3000

ENTRYPOINT ["/docker-entrypoint.sh"]
