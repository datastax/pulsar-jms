#/bin/bash

HERE=$(dirname $0)
HERE=$(realpath $HERE)
docker rm -f pulsar-jms-runner
docker run --name pulsar-jms-runner -v $HERE/conf:/pulsar/conf -d -p 8080:8080 -p 6650:6650 apachepulsar/pulsar:2.7.1 /pulsar/bin/pulsar standalone 
sleep 10
docker exec pulsar-jms-runner bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone
sleep 5



