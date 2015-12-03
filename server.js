var args = require('yargs').argv,
    zk = require('node-zookeeper-client'),
    kafka = require('kafka-node'),
    Table = require('cli-table'),
    linq = require('node-linq').LINQ,
    express = require('express');

if (!args.zkConnect) {
    console.error('zkConnect not specified');
    return;
}

var app = express();

app.get('/consumers/:consumer/lag', function (req, res) {
    var consumer = req.params.consumer,
        zkClient = zk.createClient(args.zkConnect),
        kafkaClient = new kafka.Client(args.zkConnect, 'lagger'),
        kafkaOffset = new kafka.Offset(kafkaClient),

        denodeify = function (f, that) {
            return function () {
                var args = Array.prototype.slice.call(arguments);
                return new Promise(function (resolve, reject) {
                    f.apply(that, args.concat(function (e, d, s) {
                        if (e) return reject(e);
                        return resolve({ d: d, s: s, args: args });
                    }));
                });
            };
        },

        getChildren = denodeify(zkClient.getChildren, zkClient),
        getData = denodeify(zkClient.getData, zkClient),
        remove = denodeify(zkClient.remove, zkClient),
        close = denodeify(zkClient.close, zkClient);

        get = function () {
            return getChildren('/consumers/' + consumer + '/offsets')
                .then(function (topicsResult) {
                    var topics = topicsResult.d;
                    return Promise.all(topics.map(function (t) {
                        return getChildren(topicsResult.args[0] + '/' + t)
                            .then(function (partitionsResult) {
                                var partitions = partitionsResult.d;
                                return partitions.map(function (p) {
                                    return {
                                        consumer: consumer,
                                        topic: t,
                                        partition: p
                                    };
                                })
                            });
                    }));
                })
                .then(function (topicPartitions) {
                    var partitions = [];
                    topicPartitions.forEach(function (tp) { return partitions = partitions.concat(tp) });
                    return Promise.all(partitions.map(function (p) {
                        return getData('/consumers/' + p.consumer + '/offsets/' + p.topic + '/' + p.partition)
                            .then(function (offsetResult) {
                                p.offset = offsetResult.d.toString();
                                return p;
                            })
                            .then(function (partition) {
                                var t = partition.topic,
                                    p = partition.partition;
                                return new Promise(function (resolve, reject) {
                                    kafkaOffset.fetch([{
                                        topic: t,
                                        partition: p,
                                        time: -1,
                                        maxNum: 1
                                    }], function (e, d) {
                                        if (e) {
                                            partition.end = -1;
                                            partition.lag = -1;
                                            resolve(parition);
                                        }

                                        var value = d[t][p];
                                        if (value.length) value = value[0];
                                        else value = undefined;

                                        partition.end = value;
                                        partition.lag = value - partition.offset;

                                        resolve(partition);
                                    });
                                });
                            });
                    }));
                })
                .then(function (r) {
                    var result = new linq(r)
                        .OrderBy(function(r) { return r.consumer + '__' + r.topic + '__' + r.partition })
                        .ToArray();
                    res.send(r);
                })
                .catch(function (e) {
                    res.status(500).send({ error: e });
                });
        };

        zkClient.once('connected', function () {
            get();
        });

    zkClient.connect();

});

app.listen(8000);