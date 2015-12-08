# kafka-consumer-monitor

A small node application that continuously outpus information about consumers -

Consumer | Topic | Partition | End | Offset | Lag
--- | --- | --- | --- | --- | ---
ConsumerGroupA | TopicX | 0 | 123 | 3 | 120
ConsumerGroupA | TopicX | 1 | 101 | 11 | 90
ConsumerGroupB | TopicY | 0 | 23 | 3 | 20
ConsumerGroupB | TopicY | 1 | 1 | 1 | 0
ConsumerGroupB | TopicY | 2 | 11 | 2 | 9

## Getting Started

    npm install
    node app.js --zkConnect=192.168.33.10:2181 --consumers=ConsumerGroupA,ConsumerGroupB

### Arguments

* zkConnect - the zookeeper connection string
* consumers - a comma separated list of the name of the consumer groups registered and tracked in zookeeper


##Docker
The docker file will need to be built
```
docker build -t wh/monitor .
```

To run the contained docker image you will need to set the environment variables from the command line.
```
docker run -d -p 8000:8000 -e ZOOKEEPER_CONNECT="192.168.33.10:2181" -v "$PWD":/usr/src/app wh/monitor npm start
```
