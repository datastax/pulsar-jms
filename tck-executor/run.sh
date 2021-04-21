#/bin/bash

# This script is intended for manual run of the TCK

# set the absolute path of the TCK executor
TS_HOME=/Users/enrico.olivelli/dev/messaging-tck

set -x -e
HERE=$(dirname $0)
HERE=$(realpath "$HERE")

PATH=$PATH:$TS_HOME/tools/ant/bin

# Copy configuration files
cp ts.* $TS_HOME/bin

# move to the directory that contains the test you want to run
cd $TS_HOME/src/com/sun/ts/tests/jms
cd $TS_HOME/src/com/sun/ts/tests/jms/core/exceptionQueue
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/appclient/jmscontexttopictests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core20/sessiontests
#cd $TS_HOME/src/com/sun/ts/tests/jms/core
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

# This command runs only the tests that previously failed
#ant -DpriorStatus="fail,error" runclient

# Run all of the tests in the selected directory (and subdirs)
ant runclient

