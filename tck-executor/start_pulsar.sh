#/bin/bash

set -x -e

IMAGENAME=${PULSAR_IMAGE_NAME:-eolivelli/pulsar:2.11.0-SNAPSHOT-1}

HERE=$(dirname $0)
HERE=$(realpath "$HERE")
FILTERSDIRECTORY=$HERE/../pulsar-jms-filters/target
docker rm -f pulsar-jms-runner
docker run --name pulsar-jms-runner -v $FILTERSDIRECTORY:/pulsar/filters -v $HERE/conf:/pulsar/conf -d -p 8080:8080 -p 6650:6650 $IMAGENAME bash -c "sleep 100000"
# Wait for pulsar to start
echo "Waiting 15 seconds"

wget -O - http://localhost:8080/admin/v2/clusters

sleep 10

# This step is needed on 2.7.x in order to start the Transaction Coordinator
#docker exec pulsar-jms-runner bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone



