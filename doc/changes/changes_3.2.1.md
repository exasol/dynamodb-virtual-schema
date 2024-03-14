# Virtual Schema for Amazon DynamoDB 3.2.1, released 2024-03-14

Code name: Fix CVE-2024-25710 and CVE-2024-26308 in test dependency

## Summary

This release fixes CVE-2024-25710 and CVE-2024-26308 in test dependency `org.apache.commons:commons-compress`

## Security

* #184: Fixed CVE-2024-25710 in `org.apache.commons:commons-compress:jar:1.24.0:test`
* #185: Fixed CVE-2024-26308 in `org.apache.commons:commons-compress:jar:1.24.0:test`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:10.1.0` to `10.1.1`
* Updated `software.amazon.awssdk:dynamodb:2.21.42` to `2.25.9`

### Runtime Dependency Updates

* Updated `org.slf4j:slf4j-jdk14:2.0.9` to `2.0.12`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:7.0.0` to `7.0.1`
* Updated `com.exasol:hamcrest-resultset-matcher:1.6.3` to `1.6.5`
* Updated `com.exasol:test-db-builder-java:3.5.3` to `3.5.4`
* Updated `com.exasol:udf-debugging-java:0.6.11` to `0.6.12`
* Updated `com.exasol:virtual-schema-common-document:10.1.0` to `10.1.1`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.15.4` to `3.15.8`
* Updated `org.junit.jupiter:junit-jupiter-params:5.10.1` to `5.10.2`
* Updated `org.mockito:mockito-junit-jupiter:5.8.0` to `5.11.0`
* Updated `org.testcontainers:junit-jupiter:1.19.3` to `1.19.7`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:1.3.1` to `2.0.1`
* Updated `com.exasol:project-keeper-maven-plugin:2.9.17` to `4.2.0`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.11.0` to `3.12.1`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.2.2` to `3.2.5`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.2.2` to `3.2.5`
* Added `org.apache.maven.plugins:maven-toolchains-plugin:3.1.0`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.5.0` to `1.6.0`
