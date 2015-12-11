var args        = require('yargs').argv;
var kafka       = require('kafka-node');
var nodeCache   = require('node-cache');
var _           = require('lodash');

var cache       = require('./Cache.js').cache;

var getZkConnect = function() {
    var connect = args.zkConnect || process.env.ZOOKEEPER_CONNECT;
    return connect;
};

var kafkaClient = new kafka.Client(getZkConnect(), 'lag-monitor');
var kafkaOffset = new kafka.Offset(kafkaClient);


var getCachedItems = function(cb) {
    cache.keys(function(err, keys){
        cache.mget(keys, function(err, items){
            cb(keys, items);
        });
    });
};


var getOffsetFetch = function(cb) {
    getCachedItems(function(keys, consumers) {
        console.log('cached consumers:', consumers);

        var map = [];
        keys.forEach(function(key) {
            var consumer = consumers[key];
            console.log('consumer is:', consumer);
            map = map.concat(consumer.map(function(consumertopic){
                return {
                    topic: consumertopic.topic,
                    partition: consumertopic.partition,
                    time: Date.now(),
                    maxNum: 1 };
            }));
        });
        console.log('mapped consumers:', map);
        cb(map);
    });
};


var updateCachedItems = function(updates, cb) {
    getCachedItems(function(keys, consumers) {
        keys.forEach(function(key){
            var consumer = consumers[key];
            console.log('update', consumer);
            consumer.forEach(function(consumerTopic){
                // map the update to the consumer
                var offset = updates[consumerTopic.topic][consumerTopic.partition];
                if (offset.length) {
                    consumerTopic.end = parseInt(offset[0]);
                    consumerTopic.lag = consumerTopic.offset - consumerTopic.end;
                }
            });
            //save the consumer back to the cache
            console.log('saving updated consumer:', consumer);
            cache.set(key, consumer);
        });

        if(cb) {
            cb();
        }
    });
};


var getTopicOffsets = function(cb) {
    console.log('get the topic offsets');
    getOffsetFetch(function(items){
        console.log('the offset fetch:', items);
        kafkaOffset.fetch(items, function(err, data){
            console.log('returned items from kafka:', data);
            updateCachedItems(data, cb);
        });
    });
};


module.exports = {

    getTopicOffsets : function(cb) {
        getTopicOffsets(cb);
    }
};
