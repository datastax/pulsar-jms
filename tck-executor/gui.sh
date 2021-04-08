#/bin/bash


set -x -e
TS_HOME=/Users/enrico.olivelli/dev/messaging-tck
JMS_HOME=/Users/enrico.olivelli/dev/pulsar-jms/tck-executor
PATH=$PATH:$JMS_HOME/bin:$TS_HOME/tools/ant/bin

cp ts.* $TS_HOME/bin

cd $TS_HOME/src/com/sun/ts/tests/jms
ant runclient

