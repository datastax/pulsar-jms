# TLS Certificate Testing

In order to run complete and correct tests for TLS, there are generated certificates in this directory. They were
generated using the following script:

```shell
set -e

openssl genrsa -out ca.key 2048

openssl req -x509 -subj '/CN=localhost' -new -nodes -key ca.key \
            -sha256 -days 1024 -out ca.pem

openssl pkcs12 -export -name broker-cert \
               -in ca.pem -inkey ca.key \
               -out broker-keystore.p12 -password pass:password

keytool -importkeystore -destkeystore broker.keystore \
        -srckeystore broker-keystore.p12 -srcstoretype pkcs12 \
        -alias broker-cert -srcstorepass password -deststorepass password

keytool -import -alias client-cert \
        -file ca.pem -keystore broker.truststore -storepass password
```