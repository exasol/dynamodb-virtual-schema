# Virtual Schema for Amazon DynamoDB 2.1.0, released 2021-09-28

Code name: New Mapping Types

## Summary

This release integrates the new features from the virtual-schema-common-document 6.1.0:

> In this release we added the following new mapping types:
>
> * `toDoubleMapping`
> * `toBoolMapping`
> * `toDateMapping`
> * `toTimestampMapping`
>
> In order to use the new features, please update you EDML definitions to version `1.3.0` (no breaking changes).

For details see the [changelog of the virtual-schema-common-document](https://github.com/exasol/virtual-schema-common-document/blob/main/doc/changes/changes_6.1.0.md).

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:hamcrest-resultset-matcher:1.4.1` to `1.5.0`
* Updated `com.exasol:virtual-schema-common-document:5.0.0` to `6.1.0`
* Updated `software.amazon.awssdk:dynamodb:2.17.15` to `2.17.47`

### Runtime Dependency Updates

* Removed `org.jacoco:org.jacoco.agent:0.8.7`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:4.0.0` to `5.1.0`
* Updated `com.exasol:virtual-schema-common-document:5.0.0` to `6.1.0`
* Added `org.jacoco:org.jacoco.agent:0.8.7`
* Updated `org.junit.jupiter:junit-jupiter-engine:5.7.2` to `5.8.1`
* Updated `org.junit.jupiter:junit-jupiter-params:5.7.2` to `5.8.1`
* Updated `org.mockito:mockito-junit-jupiter:3.11.2` to `3.12.4`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:0.5.1` to `0.6.0`
* Updated `com.exasol:project-keeper-maven-plugin:0.10.0` to `1.2.0`
