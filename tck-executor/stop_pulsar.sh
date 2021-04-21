#/bin/bash


HERE=$(dirname $0)
HERE=$(realpath "$HERE")
docker logs pulsar-jms-runner 2>&1 > $HERE/target/pulsar.log
docker rm -f pulsar-jms-runner
