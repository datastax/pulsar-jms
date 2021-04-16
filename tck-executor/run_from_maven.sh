#/bin/bash

set -x -e
HERE=$(dirname $0)
HERE=$(realpath $HERE)

unzip $HERE/jakarta-messaging-tck-2.0.0.zip -d $HERE/target

tar zxvf apache-pulsar.tar.gz --directory $HERE/target

cp standalone.conf target/apache-pulsar-2.8.0-SNAPSHOT/conf
$HERE/target/apache-pulsar-2.8.0-SNAPSHOT/bin/pulsar standalone --wipe-data 2>&1 1>$HERE/target/pulsar.log &
PULSAR_PID=$!

TS_HOME=target/messaging-tck
PATH=$PATH:$TS_HOME/tools/ant/bin

cp ts.* $TS_HOME/bin
echo "jms.home=$HERE" >> $TS_HOME/bin/ts.jte
echo "jms.classes=\${jms.home}/target/tck-executor-1.0.0-SNAPSHOT.jar" >> $TS_HOME/bin/ts.jte

cd $TS_HOME/src/com/sun/ts/tests/jms

ant runclient || kill -KILL $PULSAR_PID

