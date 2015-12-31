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

var loadKafkaOffsets = function(callback) {
    kafkaLib.getTopicOffsets(callback);
};

var pollKafkaOffsets = function() {
    setTimeout(function() {
        loadKafkaOffsets(pollKafkaOffsets);
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


// CORS headers for external access from JS
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});


app.get('/monitor/refresh', function(req, res) {
    logger.trace('load metadata called from external source');
    loadMetadata(function(err){
        if (err) {
            return res.status(500).json({'error_code': 500, 'message': err});
        }

        logger.trace('completed loading metadata');
        return res.status(200).send();
    });
});


app.get('/consumergroups', function(req, res){
    cache.keys(function(err, keys){
        logger.debug({keys: keys}, 'returned keys');
        res.status(200).send(keys);
    });
});


app.get('/consumers/:consumer/lag', function(req, res) {
    var consumer = req.params.consumer; 

    cache.get(consumer, function(err, value){
        if (!err) {
            // TODO : sort the response here if required
            res.send(value);
        }
        else {
            res.status(500).send();
        }
    });
});

app.listen(8000);
