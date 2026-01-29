#/bin/bash

set -x -e

IMAGENAME=${PULSAR_IMAGE_NAME:-datastax/lunastreaming:4.0.7_2}

# Use podman if available, otherwise fall back to docker
if command -v podman &> /dev/null; then
    CONTAINER_CMD=podman
else
    CONTAINER_CMD=docker
fi

HERE=$(dirname $0)
HERE=$(realpath "$HERE")
FILTERSDIRECTORY=$HERE/../pulsar-jms-filters/target
$CONTAINER_CMD rm -f pulsar-jms-runner
$CONTAINER_CMD run --name pulsar-jms-runner -v $FILTERSDIRECTORY:/pulsar/filters -v $HERE/conf:/pulsar/conf -d -p 8080:8080 -p 6650:6650 $IMAGENAME /pulsar/bin/pulsar standalone -nss -nfw
# Wait for pulsar to start
echo "Waiting 15 seconds"

wget -O - http://localhost:8080/admin/v2/clusters

sleep 10

# Initialize transaction coordinator metadata
# Commenting out as it may fail if ZooKeeper is not ready yet
# $CONTAINER_CMD exec pulsar-jms-runner bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone || echo "Transaction coordinator already initialized or failed to initialize"



