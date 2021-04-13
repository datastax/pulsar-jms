#/bin/bash


set -x -e
TS_HOME=/Users/enrico.olivelli/dev/messaging-tck
JMS_HOME=/Users/enrico.olivelli/dev/pulsar-jms/tck-executor
PATH=$PATH:$JMS_HOME/bin:$TS_HOME/tools/ant/bin

cp ts.* $TS_HOME/bin

cd $TS_HOME/src/com/sun/ts/tests/jms
#cd $TS_HOME/src/com/sun/ts/tests/jms/core/appclient/topictests
cd $TS_HOME/src/com/sun/ts/tests/jms/core
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/connectionfactorytests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/runtimeexceptiontests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/jmsconsumertests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/jmscontextqueuetests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/jmscontexttopictests

#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/jmsproducerqueuetests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/jmsproducertopictests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/messageproducertests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/sessiontests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/appclient

ant -DpriorStatus="fail,error" runclient

#ant runclient

