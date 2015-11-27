var args = require('yargs').argv,
    zk = require('node-zookeeper-client'),
    kafka = require('kafka-node'),
    Table = require('cli-table'),
    linq = require('node-linq').LINQ;

if (!args.zkConnect) {
    console.error('zkConnect not specified');
    return;
}
if (!args.consumers) {
    console.error('consumers not specified');
    return;
}

var zkClient = zk.createClient(args.zkConnect),
    kafkaClient = new kafka.Client(args.zkConnect, 'lagger'),
    kafkaOffset = new kafka.Offset(kafkaClient),

    poll = function (cb) {

        var ops = 1,
            start = Date.now(),
            result = [];

        args.consumers.split(',').forEach(function (c) {
            ops++;

            zkClient.getChildren('/consumers/' + c + '/offsets', function (e, d, z) {
                if (e) return ops--;
                
                d.forEach(function (t) {
                    ops++;


                    zkClient.getChildren('/consumers/' + c + '/offsets/' + t, function (e, d, z) {
                        if (e) return ops--;

                        d.forEach(function (p) {
                            ops++;

                            zkClient.getData('/consumers/' + c + '/offsets/' + t + '/' + p, function (e, d, z) {
                                if (e) return ops--;

                                ops++;
                                var offset = d.toString();

                                kafkaOffset.fetch([{
                                    topic: t,
                                    partition: p,
                                    time: -1,
                                    maxNum: 1
                                }], function (e, d) {
                                    if (e) return ops--;

                                    var value = d[t][p];
                                    if (value.length) value = value[0];
                                    else value = undefined;

                                    result.push({
                                        consumer: c,
                                        topic: t,
                                        partition: p,
                                        end: value,
                                        offset: offset,
                                        lag: value - offset
                                    });

                                    ops--;
                                });

                                ops--;
                            });
                        });

                        ops--;
                    });
                });

                ops--;
            });

            ops--;
        });
            
        var interval = setInterval(function () {
            if (ops > 0) return;
            cb(result);
            clearInterval(interval);
        }, 10);
    };

zkClient.once('connected', function () {

    var go = function () {
        poll(function (result) {
            var table = new Table({ head : ['Consumer', 'Topic', 'Partition', 'End', 'Offset', 'Lag']});
            new linq(result)
                .OrderBy(function(r) { return r.consumer + '__' + r.topic + '__' + r.partition })
                .ToArray()
                .forEach(function (r) { table.push([ r.consumer, r.topic, r.partition, r.end, r.offset, r.lag ]); });
            console.log(table.toString());
            console.log(Date.now());
            go();
        });
    };

    go();

});

zkClient.connect();
