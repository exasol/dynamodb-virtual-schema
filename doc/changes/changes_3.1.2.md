# Virtual Schema for Amazon DynamoDB 3.1.2, released 2023-07-04

Code name: Updated documentation and dependencies on top of 3.1.1

## Summary

This release adds a known limitation to the User Guide and fixes vulnerability CVE-2023-34462 in transitive dependency `io.netty:netty-handler:jar:4.1.86.Final` by updating `software.amazon.awssdk:dynamodb`.

## Security

* #177: Updated dependencies
* #158: Updated documentation

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:9.4.0` to `9.4.1`
* Updated `software.amazon.awssdk:dynamodb:2.20.62` to `2.20.98`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.5.2` to `6.6.0`
* Updated `com.exasol:udf-debugging-java:0.6.8` to `0.6.9`
* Updated `com.exasol:virtual-schema-common-document:9.4.0` to `9.4.1`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.14.1` to `3.14.3`
* Updated `org.mockito:mockito-junit-jupiter:5.3.1` to `5.4.0`
* Updated `org.testcontainers:junit-jupiter:1.18.0` to `1.18.3`
