#/bin/bash

set -x -e
HERE=$(dirname $0)
HERE=$(realpath $HERE)

unzip -o $HERE/jakarta-messaging-tck-2.0.0.zip -d $HERE/target


TS_HOME=target/messaging-tck
PATH=$PATH:$TS_HOME/tools/ant/bin

cp ts.* $TS_HOME/bin
echo "jms.home=$HERE" >> $TS_HOME/bin/ts.jte
echo "jms.classes=\${jms.home}/target/tck-executor-1.0.0-SNAPSHOT.jar" >> $TS_HOME/bin/ts.jte

$HERE/start_pulsar.sh

cd $TS_HOME/src/com/sun/ts/tests

ant runclient || $HERE/stop_pulsar.sh

