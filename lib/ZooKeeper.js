var args        = require('yargs').argv;
var zk          = require('node-zookeeper-client');
var _           = require('lodash');
var config      = require('../config.js');
var cache       = require('./Cache.js').cache;
var logger      = require('../logger.js').logger;

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


var watchZkPartitionOffset = function(partitionUri) {
    zkClient.getData(partitionUri,
        function(event) {
            // watch the event
            logger.trace(event,'received event for path');
            getZkPartitionOffset(partition, topicUri);
        },
        function(error, data, status){
            if (err) {
                logger.error(err, 'error on watcher');
            }
            else {
                // read the data and update the cache item
                var parts = partitionUri.split('/');
                var consumer = {consumer : parts[2], topic: parts[4], partition: partition};

                var cachedConsumer = cache.get(consumer.consumer);
                _.extend(cachedConsumer, consumer);
                cache.set(consumer.consumer, cachedConsumer);
            }
        });
};


var getZkPartitionOffset = function(partition, topicUri, cb) {
    var partitionUri = topicUri + '/' + partition;
    logger.trace('get offset for partition at path:' + partitionUri);

    return getData(partitionUri).then(function(result){
        logger.trace('offset :' + result.data.toString());
        return result.data.toString();
    });
};


var getZkTopicPartitions = function(topic, consumerUri, cb) {
    logger.trace('load topic "' + topic + '" data from zookeeper : ' + consumerUri);
    var topicUri = consumerUri + '/' + topic;

    var topicMap = [];
    return getChildren(topicUri).then(function(result){
        logger.trace(result.data, 'zktopic data :');
        return Promise.all(
            result.data.map(function(partition){
                return getZkPartitionOffset(partition, topicUri).then(function(result){
                    return {topic: topic, partition: partition, offset: result};
                });
            })
        );
    });
};

var getZkConsumerTopics = function(name, cb) {
    logger.trace('load consumer "' + name + '" data from zookeeper');
    var consumerUri = '/consumers/' + name + '/offsets';

    var topicMap = [];
    logger.trace({uri: consumerUri}, 'begin load');
    return getChildren(consumerUri).then(function(result){
        logger.trace(result.data, 'get consumer response');
        return Promise.all(
            result.data.map(function(topic) {
                logger.trace(topic, 'getting consumer topic details');
                return getZkTopicPartitions(topic, consumerUri).then(function(topics){
                    logger.trace(topics, 'consumertopics :');
                    topics.forEach(function(topic){
                        topic.consumer = name;
                    });
                    topicMap = topics;
                    logger.trace(topics, 'returning the consumertopics:');
                    return topicMap;
                });
            }));
    });
};


// use this if you want to watch the consumers in zookeeper. can make for a chatty app
var getZkConsumers = function(cb) {
    var consumersUri = '/consumers';

    new Promise(function(resolve, reject){
        zkClient.getChildren(consumersUri, function(event){
            logger.trace(event, 'Consumer received event');
            getZkConsumers(cb);
        }, function(err, data, status) {
            logger.trace(data, 'getconsumers response');

            if (data === undefined) {
                err = {
                    internal: err,
                    type: 'error',
                    message: 'unable to read consumer data from zk'
                };
            }
            return resolve({err: err, data: data, status: status});
        });
    }).then(function(result) {
        logger.trace(result.data, 'registered consumers');
        cb(result.err, result.data);
    });
};


var getCachedConsumer = function(name, cb) {
    logger.trace('retrieve consumer from cache');
    var consumer = cache.get(name);

    if (consumer === undefined) {
        logger.trace('consumer "' + name + '" not in cache, reload from ZooKeeper');
        return getZkConsumerTopics(name).then(function(consumer) {
            cache.set(name, consumer[0], function(err, success){
                if (err || !success) {
                    logger.trace('unable to set cache for consumer ' + name);
                }

                if (cb) {
                    cb();
                }
            });
        }
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

            Promise.all(
                consumers.map(function(consumer) {
                    if (!consumer.startsWith('schema-registry')) {
                        return getCachedConsumer(consumer);
                    }
                })
            ).then(function() {
                logger.trace('finished loading the metadata');
                cb();
            });
        });
    }
};
