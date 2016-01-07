var args        = require('yargs').argv;
var kafka       = require('kafka-node');
var nodeCache   = require('node-cache');
var _           = require('lodash');
var config      = require('../config.js');
var cache       = require('./Cache.js').cache;
var logger      = require('../logger.js').logger;

var kafkaClient = new kafka.Client(process.env.ZOOKEEPER_CONNECT || config.zkConnect, 'lag-monitor');
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
        logger.trace(consumers, 'cached consumers');

        var map = [];
        keys.forEach(function(key) {
            var consumer = consumers[key];
            if (!!consumer) {
                logger.trace(consumer, 'offset consumer to update is');
                map = map.concat(consumer.map(function(consumertopic){
                    return {
                        topic: consumertopic.topic,
                        partition: consumertopic.partition,
                        time: -1,
                        maxNum: 1 };
                }));
            }
        });
        logger.trace(map, 'mapped consumers');
        cb(map);
    });
};


var updateCachedItems = function(updates, cb) {
    logger.trace({updates: updates},'updates to be cached');
    if (!updates || updates.length === 0) {
        if (!!cb) {
            cb();
        }
        return;
    }

    getCachedItems(function(keys, consumers) {
        keys.forEach(function(key){
            var consumer = consumers[key];
            logger.trace({consumer: consumer, key: key}, 'update consumer offset');

            if (!!consumer) {
                consumer.forEach(function(consumerTopic){
                    // map the update to the consumer
                    var offset = updates[consumerTopic.topic][consumerTopic.partition];
                    logger.trace({topic: consumerTopic, offset: offset}, 'offset to update');
                    if (offset.length) {
                        consumerTopic.end = parseInt(offset[0]);
                        consumerTopic.lag = consumerTopic.end - consumerTopic.offset;
                    }
                });
                //save the consumer back to the cache
                logger.trace(consumer, 'saving updated consumer');
                cache.set(key, consumer);
            }
        });

        if(cb) {
            cb();
        }
    });
};


var getTopicOffsets = function(cb) {
    logger.trace('get the topic offsets');
    getOffsetFetch(function(items){
        logger.trace(items, 'the offset fetch');
        kafkaOffset.fetch(items, function(err, data){
            if (err) {
                logger.error(err, 'error getting offsets from kafka');
            }

            logger.trace(data, 'returned items from kafka:');
            updateCachedItems(data, cb);
        });
    });
};


module.exports = {

    getTopicOffsets : function(cb) {
        getTopicOffsets(cb);
    }
};
