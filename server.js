var Table = require('cli-table');
var linq = require('node-linq').LINQ;
var express = require('express');

var zkLib = require('./lib/ZooKeeper.js');
var kafkaLib = require('./lib/Kafka.js');
var cache    = require('./lib/Cache.js').cache;

var app = express();


zkLib.loadConsumerMetaData(function() {
    console.log('loading the consumer offsets');
    kafkaLib.getTopicOffsets();
});

var pollKafkaOffsets = function() {
    setTimeout(function() {
        kafkaLib.getTopicOffsets(pollKafkaOffsets);
    }, 10000);
};

pollKafkaOffsets();

app.get('/consumers/:consumer/lag', function (req, res) {
    cache.get(req.params.consumer, function(err, value){
        if (!err) {
            res.send(value);
        }
        else {
            res.status(500).send();
        }
    });
});

app.listen(8000);
