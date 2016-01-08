var args        = require('yargs').argv;
var zk          = require('node-zookeeper-client');
var nodeCache   = require('node-cache');
var _           = require('lodash');
var config   = require('../config.js');
var cache       = require('./Cache.js').cache;
var logger      = require('../logger.js').logger;
var Promise     = require('promise');

var zkClientState = zk.State;
var zkClient    = null;

zkClient = zk.createClient(process.env.ZOOKEEPER_CONNECT || config.zkConnect);

var denodeify = function (f, that) {
    return function () {
        var args = Array.prototype.slice.call(arguments);
        return new Promise(function (resolve, reject) {
            f.apply(that, args.concat(function (err, data, status) {
                if (err) {
                    logger.error({err:err}, 'denodify error');
                    return reject(err);
                }
                return resolve({ data: data, status: status, args: args });
            }));
        });
    };
};

var getChildren = denodeify(zkClient.getChildren, zkClient);
var getData = denodeify(zkClient.getData, zkClient);
var remove = denodeify(zkClient.remove, zkClient);
var close = denodeify(zkClient.close, zkClient);


var getZkPartitionOffset = function(partition, topicUri, cb) {
    var partitionUri = topicUri + '/' + partition;
    logger.trace('get offset for partition at path:' + partitionUri);

    return getData(partitionUri).then(function(result){
        logger.trace('offset :' + result.data.toString());
        return result.data.toString();
    })
    .catch(function(err){
        logger.warn({partitionUri:partitionUri, err:err}, 'error getting partition offset');
        return null;
    });
};


var getZkTopicPartitions = function(topic, consumerUri, cb) {
    logger.trace('load topic "' + topic + '" data from zookeeper : ' + consumerUri);
    var topicUri = consumerUri + '/' + topic;

    var topicMap = [];
    return getChildren(topicUri)
        .then(function(result){
            logger.trace(result.data, 'zktopic data :');
            return Promise.all(
                result.data.map(function(partition){
                    return getZkPartitionOffset(partition, topicUri).then(function(result){
                        if (!!result) {
                            return {topic: topic, partition: partition, offset: result};
                        }
                    });
                })
            );
        })
        .catch(function(err){
            logger.warn({topicUri: topicUri, err: err}, 'error returning the topic partitions from Zookeeper');
            return topicMap;
        });
};

var getZkConsumerTopics = function(name, cb) {
    logger.trace('load consumer "' + name + '" data from zookeeper');
    var consumerUri = '/consumers/' + name + '/offsets';

    var topicMap = [];
    logger.trace({uri: consumerUri}, 'begin load');
    return getChildren(consumerUri)
        .then(function(result){
            logger.trace(result.data, 'get consumer response');
            return Promise.all(
                result.data.map(function(topic) {
                    logger.trace(topic, 'getting consumer topic details');
                    return getZkTopicPartitions(topic, consumerUri).then(function(topics){
                        logger.trace(topics, 'consumertopics');
                        topics.forEach(function(topic){
                            topic.consumer = name;
                        });
                        topicMap = topics;
                        logger.trace(topics, 'returning the consumertopics');
                        return topicMap;
                    });
                }));
        })
        .catch(function(err){
            logger.warn({consumerOffsetUri: consumerUri, err: err}, 'error returning the consumer topics from Zookeeper');
            return [];
        });
};


// use this if you want to watch the consumers in zookeeper. can make for a chatty app
var getZkConsumers = function(cb) {
    var consumersUri = '/consumers';

    getChildren(consumersUri).then(function(result) {
        logger.trace(result.data, 'registered consumers');
        cb(result.err, result.data);
    })
    .catch(function(err){
        logger.warn({consumersUri: consumersUri, err: err}, 'error returning the consumers from Zookeeper');
    });
};


var getCachedConsumer = function(name, cb) {
    logger.trace('retrieve consumer from cache');
        return getZkConsumerTopics(name).then(function(consumer) {
            cache.set(name, consumer[0], function(err, success){
                if (err || !success) {
                    logger.trace('unable to set cache for consumer ' + name);
                }

                logger.debug({name: name, consumer: consumer}, 'setting the consumer in the cache');
                if (!!cb) {
                    cb();
                }

                return;
            });
        });
};


var connectZooKeeper = function(cb) {
    logger.trace('connecting to zookeeper');
    if (zkClient.getState() === zkClientState.SYNC_CONNECTED) {
        logger.trace('Connection to zk already open');
        return cb();
    }

    zkClient.once('connected', function () {
        logger.trace('connection to zookeeper established');
        return cb();
    });

    zkClient.connect();
};


var getConsumers = function(cb)  {
    connectZooKeeper(function() {
        getZkConsumers(function(err, data) {
            cb(err, data);
        });
    });
};


module.exports = {
    loadConsumerMetaData : function(cb) {
        getConsumers(function(err, consumers){
            if (!!err) {
                logger.error({err: err}, 'error loading consumer metadata');
                cb(err);
            }

            if (!!consumers) {

                Promise.all(
                    consumers.map(function(consumer) {
                        if (consumer.indexOf('schema-registry') == -1) {
                            return getCachedConsumer(consumer);
                        }

                        return;
                    })
                ).then(function() {
                    logger.trace('finished loading the metadata');
                    return cb();
                });
            }
        });
    }
};
