# Virtual Schema for Amazon DynamoDB 3.1.1, released 2023-05-10

Code name: Update dependencies on Top of 3.1.0

## Summary

This release updates dependencies on top of 3.1.0.

## Documentation

* #175: Fixed broken links

## Dependency Updates

### Compile Dependency Updates

* Removed `com.exasol:hamcrest-resultset-matcher:1.5.2`
* Updated `com.exasol:virtual-schema-common-document:9.2.0` to `9.4.0`
* Removed `commons-codec:commons-codec:1.15`
* Removed `org.slf4j:slf4j-jdk14:2.0.6`
* Updated `software.amazon.awssdk:dynamodb:2.20.5` to `2.20.62`

### Runtime Dependency Updates

* Added `org.slf4j:slf4j-jdk14:2.0.7`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.5.1` to `6.5.2`
* Added `com.exasol:hamcrest-resultset-matcher:1.6.0`
* Updated `com.exasol:virtual-schema-common-document:9.2.0` to `9.4.0`
* Added `nl.jqno.equalsverifier:equalsverifier:3.14.1`
* Updated `org.jacoco:org.jacoco.agent:0.8.8` to `0.8.9`
* Removed `org.jacoco:org.jacoco.core:0.8.8`
* Removed `org.junit.jupiter:junit-jupiter-engine:5.9.2`
* Updated `org.junit.jupiter:junit-jupiter-params:5.9.2` to `5.9.3`
* Updated `org.mockito:mockito-junit-jupiter:5.1.1` to `5.3.1`
* Updated `org.testcontainers:junit-jupiter:1.17.6` to `1.18.0`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:1.2.2` to `1.2.3`
* Updated `com.exasol:project-keeper-maven-plugin:2.9.3` to `2.9.7`
* Updated `org.apache.maven.plugins:maven-assembly-plugin:3.4.2` to `3.5.0`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.10.1` to `3.11.0`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.1.0` to `3.3.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.0.0-M8` to `3.0.0`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.0.0-M8` to `3.0.0`
* Added `org.basepom.maven:duplicate-finder-maven-plugin:1.5.1`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.3.0` to `1.4.1`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.14.2` to `2.15.0`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.8` to `0.8.9`
* Removed `org.projectlombok:lombok-maven-plugin:1.18.20.0`
