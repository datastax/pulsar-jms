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

echo "Waiting for Pulsar admin API to be ready"
for i in $(seq 1 60); do
  CLUSTERS=$(wget -4 -q -O - http://127.0.0.1:8080/admin/v2/clusters || true)
  echo "$CLUSTERS"
  if [ "$CLUSTERS" = '["standalone"]' ]; then
    echo "Pulsar admin API is ready"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "Pulsar admin API did not become ready in time"
    $CONTAINER_CMD logs pulsar-jms-runner
    exit 1
  fi
  sleep 2
done

# NOTE: standalone mode here uses a local RocksDB metadata store (no
# ZooKeeper). Running `bin/pulsar initialize-transaction-coordinator-metadata`
# as a separate process while the broker is up fails with a RocksDB lock
# error, since only one process can hold the metadata store at a time:
#   Caused by: org.rocksdb.RocksDBException: While lock file: .../LOCK: Unknown error 11
# The broker initializes the transaction coordinator topics itself on
# startup; run_tck_group.sh polls for that readiness before running tests.

