var Table       = require('cli-table');
var linq        = require('node-linq').LINQ;
var express     = require('express');

var zkLib       = require('./lib/ZooKeeper.js');
var kafkaLib    = require('./lib/Kafka.js');
var cache       = require('./lib/Cache.js').cache;
var logger      = require('./logger.js').logger;
var config      = require('./config');

var app = express();


zkLib.loadConsumerMetaData(function() {
    logger.trace('loading the consumer offsets');
    kafkaLib.getTopicOffsets();
});

var pollKafkaOffsets = function() {
    setTimeout(function() {
        kafkaLib.getTopicOffsets(pollKafkaOffsets);
    }, config.refreshInterval);
};

pollKafkaOffsets();

app.get('/consumers/:consumer/lag', function (req, res) {
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

app.listen(8000);
