var zkLib       = require('./lib/ZooKeeper.js');
var kafkaLib    = require('./lib/Kafka.js');
var cache       = require('./lib/Cache.js').cache;
var logger      = require('./logger.js').logger;
var config      = require('./config');
var express     = require('express');
var app         = express();
var loadMetadata = function(callback) {
    zkLib.loadConsumerMetaData(callback);
};

var pollKafkaOffsets = function() {
    logger.trace('polling for the latest kafka offsets');
    setTimeout(function() {
        kafkaLib.getTopicOffsets(pollKafkaOffsets);
    }, config.refreshInterval.lag);
};

var pollMetadata = function() {
    logger.trace('polling the latest metadata');
    setTimeout(function() {
        loadMetadata(pollMetadata);
    }, config.refreshInterval.metadata);
};


loadMetadata(function() {
    pollMetadata();
    loadKafkaOffsets(pollKafkaOffsets);
});


app.get('/monitor/refresh', function(req, res) {
    logger.trace('load metadata called from external source');
    loadMetadata(function(err){
        if (err) {
            return res.status(500).json({'error_code': 500, 'message': err});
        }

        logger.trace('completed loading metadata');
        res.status(200).send();
    });
});


app.get('/consumers/:consumer/lag', function(req, res) {
    cache.get(req.params.consumer, function(err, value){
        if (!err) {
            // TODO : sort the response here if required

            res.send(value);
        }
        else {
            res.status(500).send();
        }
    });
});

app.listen(config.server.port);
