# dynamodb-virtual-schema 2.0.0, released 2021-08-10

Code name: Dependency updates

# Summary

In this release we updated the virtual-schema-common-document version to 5.0.0. By that we fixed some bugs and improved performance.

The dependency update cause the following API change:

* The UDF definition changed (see [user_guide.md](../user-guide/user_guide.md))

## Features / Enhancements

* #148: Use virtual-schema-common-documents 3.0.0

## Refactoring

* #149: Refactored integration tests
* #152: Added error-codes to error messages

## Dependency Updates

### Compile Dependency Updates

* Added `com.exasol:error-reporting-java:0.4.0`
* Added `com.exasol:hamcrest-resultset-matcher:1.4.1`
* Updated `com.exasol:virtual-schema-common-document:1.0.0` to `5.0.0`
* Updated `org.slf4j:slf4j-jdk14:1.7.30` to `1.7.32`
* Updated `software.amazon.awssdk:dynamodb:2.13.67` to `2.17.15`

### Runtime Dependency Updates

* Added `org.jacoco:org.jacoco.agent:0.8.7`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:2.1.0` to `4.0.0`
* Added `com.exasol:test-db-builder-java:3.2.1`
* Added `com.exasol:udf-debugging-java:0.4.0`
* Updated `com.exasol:virtual-schema-common-document:1.0.0` to `5.0.0`
* Removed `org.jacoco:org.jacoco.agent:0.8.5`
* Updated `org.jacoco:org.jacoco.core:0.8.5` to `0.8.7`
* Updated `org.junit.jupiter:junit-jupiter-engine:5.6.2` to `5.7.2`
* Updated `org.junit.jupiter:junit-jupiter-params:5.6.2` to `5.7.2`
* Removed `org.junit.platform:junit-platform-runner:1.6.2`
* Added `org.mockito:mockito-junit-jupiter:3.11.2`
* Updated `org.testcontainers:junit-jupiter:1.14.3` to `1.16.0`

### Plugin Dependency Updates

* Added `com.exasol:error-code-crawler-maven-plugin:0.5.1`
* Added `com.exasol:project-keeper-maven-plugin:0.10.0`
* Added `io.github.zlika:reproducible-build-maven-plugin:0.13`
* Updated `org.apache.maven.plugins:maven-jar-plugin:2.4` to `3.2.0`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.5` to `0.8.7`