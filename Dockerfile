FROM node:latest

RUN mkdir /usr/src/app
COPY package.json /usr/src/app

RUN cd /usr/src/app && npm install
COPY . /usr/src/app

WORKDIR /usr/src/app

EXPOSE 8000
CMD ["npm", "start"]
