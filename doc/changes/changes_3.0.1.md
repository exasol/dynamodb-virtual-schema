# Virtual Schema for Amazon DynamoDB 3.0.1, released 2022-06-24

Code name: Dependency Updates

## Summary

This release fixes the following vulnerabilities by updating dependencies: CVE-2016-5002, CVE-2016-5003, CVE-2016-5004, CVE-2021-37136, CVE-2021-37137, CVE-2021-43797, CVE-2022-24823, sonatype-2012-0050, sonatype-2020-0026, sonatype-2021-0789.

## Bugfixes

* #169: Fixed vulnerabilities reported by ossindex-maven-plugin

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:9.0.0` to `9.0.2`
* Updated `software.amazon.awssdk:dynamodb:2.17.181` to `2.17.218`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.1.1` to `6.1.2`
* Updated `com.exasol:test-db-builder-java:3.3.2` to `3.3.3`
* Updated `com.exasol:udf-debugging-java:0.6.0` to `0.6.2`
* Updated `com.exasol:virtual-schema-common-document:9.0.0` to `9.0.2`
* Updated `org.mockito:mockito-junit-jupiter:4.5.1` to `4.6.1`
* Updated `org.testcontainers:junit-jupiter:1.17.1` to `1.17.2`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:1.1.0` to `1.1.1`
* Updated `com.exasol:project-keeper-maven-plugin:2.3.2` to `2.4.6`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.9.0` to `3.10.1`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:3.2.0` to `3.3.0`
* Updated `org.apache.maven.plugins:maven-jar-plugin:3.2.0` to `3.2.2`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.8.1` to `2.10.0`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.7` to `0.8.8`
* Updated `org.sonatype.ossindex.maven:ossindex-maven-plugin:3.1.0` to `3.2.0`
