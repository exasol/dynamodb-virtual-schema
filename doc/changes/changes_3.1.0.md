# Virtual Schema for Amazon DynamoDB 3.1.0, released 2023-02-21

Code name: Maintenance

## Summary

Fixed vulnerabilities
* CVE-2022-45688 by updating dependency to `virtual-schema-common-document`
* sonatype-2012-0050 by overriding transitive dependency `commons-codec:commons-codec` of `software.amazon.awssdk:dynamodb`.

Removed references to discontinued repository `maven.exasol.com` and renamed error codes from `VS-DY` to `VSDY`.

## Bugfixes

* #174: Fixed vulnerabilities in transitive dependencies
* #172: Removed references to discontinued repository `maven.exasol.com`

## Refactorings

* #171: Renamed error codes from `VS-DY` to `VSDY`

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:error-reporting-java:0.4.1` to `1.0.1`
* Updated `com.exasol:hamcrest-resultset-matcher:1.5.1` to `1.5.2`
* Updated `com.exasol:virtual-schema-common-document:9.0.2` to `9.2.0`
* Added `commons-codec:commons-codec:1.15`
* Updated `org.slf4j:slf4j-jdk14:1.7.36` to `2.0.6`
* Updated `software.amazon.awssdk:dynamodb:2.17.218` to `2.20.5`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.1.2` to `6.5.1`
* Updated `com.exasol:test-db-builder-java:3.3.3` to `3.4.2`
* Updated `com.exasol:udf-debugging-java:0.6.2` to `0.6.8`
* Updated `com.exasol:virtual-schema-common-document:9.0.2` to `9.2.0`
* Updated `org.junit.jupiter:junit-jupiter-engine:5.8.2` to `5.9.2`
* Updated `org.junit.jupiter:junit-jupiter-params:5.8.2` to `5.9.2`
* Updated `org.mockito:mockito-junit-jupiter:4.6.1` to `5.1.1`
* Updated `org.testcontainers:junit-jupiter:1.17.2` to `1.17.6`

### Plugin Dependency Updates

* Updated `com.exasol:artifact-reference-checker-maven-plugin:0.4.0` to `0.4.2`
* Updated `com.exasol:error-code-crawler-maven-plugin:1.1.1` to `1.2.2`
* Updated `com.exasol:project-keeper-maven-plugin:2.4.6` to `2.9.3`
* Updated `io.github.zlika:reproducible-build-maven-plugin:0.15` to `0.16`
* Updated `org.apache.maven.plugins:maven-assembly-plugin:3.3.0` to `3.4.2`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:3.3.0` to `3.5.0`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.0.0` to `3.1.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.0.0-M5` to `3.0.0-M8`
* Updated `org.apache.maven.plugins:maven-jar-plugin:3.2.2` to `3.3.0`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.0.0-M5` to `3.0.0-M8`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.2.7` to `1.3.0`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.10.0` to `2.14.2`
