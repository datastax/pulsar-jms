#!/bin/bash

set -x
HERE=$(dirname $0)
HERE=$(realpath "$HERE")

CONFIGURATION_FILE=${1:-ts.jte}
TEST_GROUP=${2:-all}
SUBDIR_PATTERN=${3:-""}

echo "CONFIGURATION_FILE is $CONFIGURATION_FILE"
echo "TEST_GROUP is $TEST_GROUP"
echo "SUBDIR_PATTERN is $SUBDIR_PATTERN"

unzip -o $HERE/jakarta-messaging-tck-2.0.0.zip -d $HERE/target

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

# Determine which test directory to run based on TEST_GROUP
case $TEST_GROUP in
  core)
    echo "Running JMS Core tests (jms/core)"
    cd $TS_HOME/src/com/sun/ts/tests/jms/core
    ;;
  core20)
    echo "Running JMS 2.0 tests (jms/core20)"
    cd $TS_HOME/src/com/sun/ts/tests/jms/core20
    ;;
  others)
    echo "Running other tests (excluding jms directory)"
    cd $TS_HOME/src/com/sun/ts/tests
    # If no pattern provided, default to excluding jms directory
    if [ -z "$SUBDIR_PATTERN" ]; then
      SUBDIR_PATTERN="^./[^j]|^./j[^m]|^./jm[^s]"
    fi
    ;;
  all)
    echo "Running all tests"
    cd $TS_HOME/src/com/sun/ts/tests
    ;;
  *)
    echo "Unknown test group: $TEST_GROUP"
    echo "Valid groups: core, core20, others, all"
    echo "Optional: Add SUBDIR_PATTERN as 3rd argument (regex pattern for subdirectories)"
    $HERE/stop_pulsar.sh
    exit 1
    ;;
esac

# Initialize exit code to success
ANTEXITCODE=0

# If a subdirectory pattern is provided, run only matching subdirectories
if [ -n "$SUBDIR_PATTERN" ]; then
  echo "Running tests matching pattern: $SUBDIR_PATTERN"
  # Find all subdirectories matching the pattern and run tests in each
  for dir in $(find . -maxdepth 1 -type d -name "*" | grep -E "$SUBDIR_PATTERN" | sort); do
    if [ -d "$dir" ] && [ "$dir" != "." ]; then
      # Check if build.xml exists before trying to run ant
      if [ -f "$dir/build.xml" ]; then
        echo "Running tests in: $dir"
        cd "$dir"
        ant runclient
        CURRENT_EXIT=$?
        if [ $CURRENT_EXIT -ne 0 ]; then
          ANTEXITCODE=$CURRENT_EXIT
        fi
        cd ..
      else
        echo "Skipping $dir (no build.xml found)"
      fi
    fi
  done
else
  # Run all tests in the current directory
  ant runclient
  ANTEXITCODE=$?
fi

echo "Ant exit code $ANTEXITCODE"

# stopping Pulsar
$HERE/stop_pulsar.sh

exit $ANTEXITCODE

# Made with Bob
