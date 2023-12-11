# Virtual Schema for Amazon DynamoDB 4.0.0, released 2023-12-12

Code name: Add tests with Exasol v8

## Summary

This release integrates the following new features from `virtual-schema-common-document` 10.1.0:

### Remove support for `TIMESTAMP WITH LOCAL TIME ZONE`

This release adds support for Exasol 8 by removing support for data type `TIMESTAMP WITH LOCAL TIME ZONE`. This type caused problems with the stricter type checks enabled by default in Exasol, causing pushdown queries for document based virtual schemas to fail with the following error:

```
Data type mismatch in column number 5 (1-indexed).Expected TIMESTAMP(3) WITH LOCAL TIME ZONE, but got TIMESTAMP(3).
```

We fixed this error by removing support `TIMESTAMP WITH LOCAL TIME ZONE` completely. Please update you EDML definitions to version `2.0.0`.

### Add support for `ALTER VIRTUAL SCHEMA SET`

This release adds support for `ALTER VIRTUAL SCHEMA SET`. This will allow changing properties like `MAPPING` of document based virtual schemas without dropping and re-creating the virtual schema:

```sql
-- Update EDML mapping of the virtual schema
ALTER VIRTUAL SCHEMA MY_VIRTUAL_SCHEMA SET MAPPING = '...';

-- Enable remote logging or change the log level
ALTER VIRTUAL SCHEMA MY_VIRTUAL_SCHEMA SET DEBUG_ADDRESS = 'host:3000' LOG_LEVEL = 'FINEST';
ALTER VIRTUAL SCHEMA MY_VIRTUAL_SCHEMA SET LOG_LEVEL = 'INFO';
```

See the [`ALTER SCHEMA` documentation](https://docs.exasol.com/db/latest/sql/alter_schema.htm) for details.

## Features

* #182: Added integration tests with Exasol version 8

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-document:9.4.2` to `10.1.0`
* Updated `software.amazon.awssdk:dynamodb:2.20.156` to `2.21.42`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.6.2` to `7.0.0`
* Updated `com.exasol:hamcrest-resultset-matcher:1.6.1` to `1.6.3`
* Updated `com.exasol:test-db-builder-java:3.5.1` to `3.5.3`
* Updated `com.exasol:virtual-schema-common-document:9.4.2` to `10.1.0`
* Updated `nl.jqno.equalsverifier:equalsverifier:3.15.2` to `3.15.4`
* Updated `org.jacoco:org.jacoco.agent:0.8.10` to `0.8.11`
* Updated `org.junit.jupiter:junit-jupiter-params:5.10.0` to `5.10.1`
* Updated `org.mockito:mockito-junit-jupiter:5.5.0` to `5.8.0`
* Updated `org.testcontainers:junit-jupiter:1.19.0` to `1.19.3`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:1.3.0` to `1.3.1`
* Updated `com.exasol:project-keeper-maven-plugin:2.9.12` to `2.9.17`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:3.6.0` to `3.6.1`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.4.0` to `3.4.1`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.1.2` to `3.2.2`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.1.2` to `3.2.2`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.16.0` to `2.16.2`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.10` to `0.8.11`
* Updated `org.sonarsource.scanner.maven:sonar-maven-plugin:3.9.1.2184` to `3.10.0.2594`
