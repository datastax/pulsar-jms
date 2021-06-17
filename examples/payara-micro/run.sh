#/bin/bash

echo "Running Payara Micro, please ensure the Pulsar Standalone is started on localhost"
mvn fish.payara.maven.plugins:payara-micro-maven-plugin:start
