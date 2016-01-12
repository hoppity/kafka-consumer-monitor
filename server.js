var zkLib       = require('./lib/ZooKeeper.js');
var kafkaLib    = require('./lib/Kafka.js');
var cache       = require('./lib/Cache.js').cache;
var logger      = require('./logger.js').logger;
var config      = require('./config');
var express     = require('express');
var Promise     = require('promise');
var app         = express();

var loadMonitorData = function() {
    logger.trace('loading consumer metadata');
    return zkLib
        .loadConsumerMetaData()
        .then(kafkaLib.getTopicOffsets);
};

var pollMetadata = function() {
    setTimeout(function() {
        logger.debug('polling the latest metadata');
        loadMonitorData()
        .then(pollMetadata);
    }, config.refreshInterval);
};


loadMonitorData()
    .then(pollMetadata);

// CORS headers for external access from JS
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});


app.get('/monitor/refresh', function(req, res) {
    logger.trace('load metadata called from external source');
    loadMonitorData()
        .then(function(){
            logger.trace('completed loading metadata');
            return res.status(200).send();
        })
        .catch(function(err){
            if (err) {
                return res.status(500).json({'error_code': 500, 'message': err});
            }
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

app.listen(config.server.port);
