## Payara Micro Example

In order to run the example start Pulsar standalone locally

```
`docker run -p 8080:800 -p 6650:6650 apachepulsar/pulsar:latest bin/pulsar standalone`
```

and then run Payara in another terminal window 

```
mvn fish.payara.maven.plugins:payara-micro-maven-plugin:start
```
