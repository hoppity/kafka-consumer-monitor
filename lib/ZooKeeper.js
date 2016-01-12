var args        = require('yargs').argv;
var zk          = require('node-zookeeper-client');
var nodeCache   = require('node-cache');
var _           = require('lodash');
var config      = require('../config.js');
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
                    logger.error({err:err, args: args }, 'denodify error');
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

var getZkPartitionOffset = function(partition, topicUri) {
    var partitionUri = topicUri + '/' + partition;
    logger.trace('get offset for partition at path:' + partitionUri);

    return getData(partitionUri).then(function(result){
        logger.trace({offset: result.data.toString(), uri: partitionUri}, 'result from partition offset');
        return result.data.toString();
    })
    .catch(function(err){
        logger.warn({partitionUri:partitionUri, err:err}, 'error getting partition offset');
        return null;
    });
};

var getZkPartitionOwner = function(options) {
    var ownerUri = '/consumers/' + options.consumer + '/owners/' + options.topic + '/' + options.partition;
    logger.trace('get owner for partition at path:' + ownerUri);

    return getData(ownerUri).then(function(result){
        logger.trace({owner : result.data.toString(), uri: ownerUri}, 'owner for the partition');
        options.owner = result.data.toString();
        return options;
    })
    .catch(function(err){
        logger.warn({ownerUri:ownerUri, err:err}, 'error getting partition owner');
    }).then(function(){
        return options;
    });
};


var getZkTopicPartitions = function(consumer, topic, consumerUri) {
    var topicUri = consumerUri + '/' + topic;
    logger.trace({consumer: consumer, topic: topic, uri: topicUri}, 'load topic data from zookeeper');

    var topicMap = [];
    return getChildren(topicUri)
        .then(function(result){
            logger.trace(result.data, 'zktopic partitions');
            return Promise.all(
                result.data.map(function(partition){
                    return getZkPartitionOffset(partition, topicUri)
                    .then(function(offset){
                        if (!!offset) {
                            return {consumer: consumer, topic: topic, partition: partition, offset: offset};
                        }

                        return null;
                    })
                    .then(getZkPartitionOwner)
                    .then(function(item){
                        topicMap.push(item);
                    })
                    .catch(function(err){
                        logger.error({err: err}, 'unable to load data ');
                    });
                })
            );
        })
        .catch(function(err){
            logger.warn({topicUri: topicUri, err: err}, 'error returning the topic partitions from Zookeeper');
            return topicMap;
        }).then(function(){
            return topicMap;
        });
};

var getZkConsumerTopics = function(name) {
    logger.trace('load consumer "' + name + '" data from zookeeper');
    var consumerUri = '/consumers/' + name + '/offsets';

    logger.trace({uri: consumerUri}, 'begin load');
    return getChildren(consumerUri)
        .then(function(result){
            logger.trace(result.data, 'get consumer response');
            return Promise.all(
                result.data.map(function(topic) {
                    logger.trace(topic, 'getting consumer topic details');
                    return getZkTopicPartitions(name, topic, consumerUri);
                }));
        })
        .catch(function(err){
            logger.warn({consumerOffsetUri: consumerUri, err: err}, 'error returning the consumer topics from Zookeeper');
        });
};


// use this if you want to watch the consumers in zookeeper. can make for a chatty app
var getZkConsumers = function() {
    var consumersUri = '/consumers';

    return getChildren(consumersUri).then(function(result) {
        logger.trace(result.data, 'registered consumers');
        return result.data;
    })
    .catch(function(err){
        logger.warn({consumersUri: consumersUri, err: err}, 'error returning the consumers from Zookeeper');
    });
};


var connectZooKeeper = function() {
    logger.trace('connecting to zookeeper');
    if (zkClient.getState() === zkClientState.SYNC_CONNECTED) {
        logger.trace('Connection to zk already open');
        return;
    }

    zkClient.once('connected', function () {
        logger.trace('connection to zookeeper established');
        return;
    });

    zkClient.connect();
};


var loadData = function() {

    return getZkConsumers()
        .then(function(consumers){
            return Promise.all(
                consumers
                .filter(function(name) {
                    return (name.indexOf('schema-registry') == -1);
                })
                .map(function(consumer){
                    return getZkConsumerTopics(consumer)
                        .then(function(consumerMap){
                            if (!consumerMap || consumerMap.length === 0) {
                                logger.debug({consumer: consumer}, 'no data returned from this consumer');
                            }
                            else {
                                consumerMap = _.flatten(consumerMap);
                                logger.trace({consumer: consumer, consumerMap: consumerMap}, 'saving consumer to cache');
                                cache.set(consumer, consumerMap);
                            }
                        });
                })
            );
        });
}


module.exports = {
    loadConsumerMetaData : function() {
        connectZooKeeper(); 
        return loadData();
    }
};
