var config = require('config-node');

config({env: 'default'});
config();

if (!!process.env.ZOOKEEPER_CONNECT) {
    config.zkConnect = process.env.ZOOKEEPER_CONNECT;
}

if (!!process.env.PORT) {
   config.server.port = process.env.PORT;
}

module.exports = config;
