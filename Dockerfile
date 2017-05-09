FROM node:7.10

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY yarn.lock /usr/src/app/
RUN yarn install

COPY . /usr/src/app

EXPOSE 9092

CMD ["node", "/usr/src/app/src/server.js"]
