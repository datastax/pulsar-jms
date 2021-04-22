#/bin/bash

set -x
HERE=$(dirname $0)
HERE=$(realpath "$HERE")

unzip -o $HERE/jakarta-messaging-tck-2.0.0.zip -d $HERE/target

TS_HOME=target/messaging-tck

cp ts.* $TS_HOME/bin
echo "jms.home=$HERE" >> $TS_HOME/bin/ts.jte
echo "jms.classes=\${jms.home}/target/tck-executor-1.0.0-SNAPSHOT.jar" >> $TS_HOME/bin/ts.jte

# start Pulsar with docker
$HERE/start_pulsar.sh

docker logs pulsar-jms-runner
docker inspect pulsar-jms-runner
netstat -nlp

# move to the directory that contains the test you want to run
cd $TS_HOME/src/com/sun/ts/tests

ant runclient
ANTEXITCODE=$?
echo "Ant exit code $ANTEXITCODE"

# stopping Pulsar
$HERE/stop_pulsar.sh

exit $ANTEXITCODE

