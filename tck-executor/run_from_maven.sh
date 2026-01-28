#/bin/bash

set -x
HERE=$(dirname $0)
HERE=$(realpath "$HERE")

CONFIGURATION_FILE=${1:-ts.jte}
echo "CONFIGURATION_FILE is $CONFIGURATION_FILE"

unzip -o $HERE/jakarta-messaging-tck-3.0.1.zip -d $HERE/target

TS_HOME=target/messaging-tck

VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout | tail -n1)
cp ts.* $TS_HOME/bin
# overwrite ts.jte
cp $CONFIGURATION_FILE $TS_HOME/bin/ts.jte
echo "jms.home=$HERE" >> $TS_HOME/bin/ts.jte
echo "jms.classes=\${jms.home}/target/tck-executor-$VERSION.jar" >> $TS_HOME/bin/ts.jte

# start Pulsar with docker/podman
$HERE/start_pulsar.sh

# Use podman if available, otherwise fall back to docker
if command -v podman &> /dev/null; then
    CONTAINER_CMD=podman
else
    CONTAINER_CMD=docker
fi

$CONTAINER_CMD logs pulsar-jms-runner
$CONTAINER_CMD inspect pulsar-jms-runner
netstat -nlp

set -e

wget -O - http://localhost:8080/lookup/v2/topic/persistent/pulsar/system/transaction_coordinator_assign-partition-0

# move to the directory that contains the test you want to run
cd $TS_HOME/src/com/sun/ts/tests


ant runclient
ANTEXITCODE=$?
echo "Ant exit code $ANTEXITCODE"

# stopping Pulsar
$HERE/stop_pulsar.sh

exit $ANTEXITCODE

