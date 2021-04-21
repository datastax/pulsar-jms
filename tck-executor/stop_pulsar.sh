#/bin/bash


HERE=$(dirname $0)
HERE=$(realpath "$HERE")
docker logs pulsar-jms-runner > $HERE/target/pulsar.log
docker rm -f pulsar-jms-runner
