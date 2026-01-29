#/bin/bash

# Use podman if available, otherwise fall back to docker
if command -v podman &> /dev/null; then
    CONTAINER_CMD=podman
else
    CONTAINER_CMD=docker
fi

HERE=$(dirname $0)
HERE=$(realpath "$HERE")
$CONTAINER_CMD logs pulsar-jms-runner 2>&1 > $HERE/target/pulsar.log
$CONTAINER_CMD rm -f pulsar-jms-runner
