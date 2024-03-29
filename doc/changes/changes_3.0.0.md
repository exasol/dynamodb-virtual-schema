# Virtual Schema for Amazon DynamoDB 3.0.0, released 2022-05-13

Code name: Dependency Updates

## Summary

This release has a breaking API change: We changed the format of the connection string. Now all properties are passed as JSON in the `IDENTIFIED BY` part of the connection. By that the connection syntax is now compatible with our new standard for connection properties.

## Features

* #165: Upgraded to virtual-schema-common-document 9.0.0

## Documentation

* #162: Removed SQL_DIALECT property from documentation

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:error-reporting-java:0.4.0` to `0.4.1`
* Updated `com.exasol:hamcrest-resultset-matcher:1.5.0` to `1.5.1`
* Updated `com.exasol:virtual-schema-common-document:6.1.0` to `9.0.0`
* Updated `org.slf4j:slf4j-jdk14:1.7.32` to `1.7.36`
* Updated `software.amazon.awssdk:dynamodb:2.17.47` to `2.17.181`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:5.1.0` to `6.1.1`
* Updated `com.exasol:test-db-builder-java:3.2.1` to `3.3.2`
* Updated `com.exasol:udf-debugging-java:0.4.0` to `0.6.0`
* Updated `com.exasol:virtual-schema-common-document:6.1.0` to `9.0.0`
* Updated `org.jacoco:org.jacoco.agent:0.8.7` to `0.8.8`
* Updated `org.jacoco:org.jacoco.core:0.8.7` to `0.8.8`
* Updated `org.junit.jupiter:junit-jupiter-engine:5.8.1` to `5.8.2`
* Updated `org.junit.jupiter:junit-jupiter-params:5.8.1` to `5.8.2`
* Updated `org.mockito:mockito-junit-jupiter:3.12.4` to `4.5.1`
* Updated `org.testcontainers:junit-jupiter:1.16.0` to `1.17.1`

### Plugin Dependency Updates

* Updated `com.exasol:artifact-reference-checker-maven-plugin:0.3.1` to `0.4.0`
* Updated `com.exasol:error-code-crawler-maven-plugin:0.6.0` to `1.1.0`
* Updated `com.exasol:project-keeper-maven-plugin:1.2.0` to `2.3.2`
* Updated `io.github.zlika:reproducible-build-maven-plugin:0.13` to `0.15`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.8.1` to `3.9.0`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:2.8` to `3.2.0`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.0.0-M3` to `3.0.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.0.0-M3` to `3.0.0-M5`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.0.0-M3` to `3.0.0-M5`
* Added `org.codehaus.mojo:flatten-maven-plugin:1.2.7`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.7` to `2.8.1`
* Added `org.projectlombok:lombok-maven-plugin:1.18.20.0`
* Added `org.sonarsource.scanner.maven:sonar-maven-plugin:3.9.1.2184`
