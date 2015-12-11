var args        = require('yargs').argv;
var zk          = require('node-zookeeper-client');
var nodeCache   = require('node-cache');
var _           = require('lodash');
var zkClientState = zk.State;

var zkClient    = null;
var cache       = require('./Cache.js').cache;

var getZkConnect = function() {
    var connect = args.zkConnect || process.env.ZOOKEEPER_CONNECT;
    return connect;
};
zkClient = zk.createClient(getZkConnect());


var denodeify = function (f, that) {
    return function () {
        var args = Array.prototype.slice.call(arguments);
        return new Promise(function (resolve, reject) {
            f.apply(that, args.concat(function (err, data, status) {
                if (err) return reject(err);
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
            console.log('received event for path: ', event);
            getZkPartitionOffset(partition, topicUri);
        },
        function(error, data, status){
            if (err) {
                console.log('error on watcher : ', err);
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
    console.log('get offset for partition at path:', partitionUri);

    return getData(partitionUri).then(function(result){
        console.log('offset :', result.data.toString());
        return result.data.toString();
    });
};


var getZkTopicPartitions = function(topic, consumerUri, cb) {
    console.log('load topic "' + topic + '" data from zookeeper : ', consumerUri);
    var topicUri = consumerUri + '/' + topic;

    var topicMap = [];
    return getChildren(topicUri).then(function(result){
        console.log('zktopic data :', result.data);
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
    console.log('load consumer "' + name + '" data from zookeeper');
    var consumerUri = '/consumers/' + name + '/offsets';

    var topicMap = [];
    return getChildren(consumerUri).then(function(result){
        console.log('get consumer response: ', result.data);
        return Promise.all(
            result.data.map(function(topic) {
                return getZkTopicPartitions(topic, consumerUri).then(function(topics){
                    console.log('consumertopics :', topics);
                    topics.forEach(function(topic){
                        topic.consumer = name;
                    });
                    topicMap = topics;
                    console.log('returning the consumertopics:', topics);
                    return topicMap;
                });
            }));
    });
};


var getZkConsumers = function(cb) {
    var consumersUri = '/consumers';

    new Promise(function(resolve, reject){
        zkClient.getChildren(consumersUri, function(event){
            console.log('received event :', event);
            getZkConsumers(cb);
        }, function(err, data, status) {
            console.log('getconsumers response: ', data);
            return resolve({err: err, data: data, status: status});
        });
    }).then(function(result) {
        console.log('registered consumers:', result.data);
        cb(result.data);
    });
};

var getCachedConsumer = function(name, cb) {
    console.log('retrieve consumer from cache');
    var consumer = cache.get(name);

    if (consumer === undefined) {
        console.log('consumer "' + name + '"" not in cache, reload from ZooKeeper');
        return getZkConsumerTopics(name).then(function(consumer) {
            cache.set(name, consumer[0], function(err, success){
                if (err || !success) {
                    console.error('unable to set cache for consumer ' + name);
                }

                if (cb) {
                    cb();
                }
            });
        });
    }
};


var connectZooKeeper = function(cb) {
    console.log('connecting to zookeeper');
    if (zkClient.getState() === zkClientState.SYNC_CONNECTED) {
        console.log('Connection to zk already open');
        cb();
    }

    zkClient.once('connected', function () {
        console.log('connection to zookeeper established');
        cb();
    });

    zkClient.connect();
};


var getConsumers = function(cb)  {
    connectZooKeeper(function() {
        getZkConsumers(function(data) {
            cb(data);
        });
    });
};


module.exports = {
    loadConsumerMetaData : function(cb) {
        getConsumers(function(consumers){
            Promise.all(
                consumers.map(function(consumer) {
                    if (!consumer.startsWith('schema-registry')) {
                        return getCachedConsumer(consumer);
                    }
                })
            ).then(function() {
                console.log('finished loading the metadata');
                cb();
            });
        });
    }
};
