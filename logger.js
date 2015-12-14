var bunyan = require('bunyan');
var config = require('./config');

var logger = new bunyan.createLogger({
        name: config.logging.logName,
        streams: [
            {
                level: config.logging.level,
                stream : process.stdout
            }]
    });

module.exports = {
    logger : logger
};
