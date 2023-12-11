# Virtual Schema for AWS DynamoDB

[![Build Status](https://github.com/exasol/dynamodb-virtual-schema/actions/workflows/ci-build.yml/badge.svg)](https://github.com/exasol/dynamodb-virtual-schema/actions/workflows/ci-build.yml)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=security_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=sqale_index)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=code_smells)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=coverage)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adynamodb-virtual-schema&metric=ncloc)](https://sonarcloud.io/dashboard?id=com.exasol%3Adynamodb-virtual-schema)

## Overview

This adapter allows you to access document data that is stored in a [Amazon's DynamoDB](https://aws.amazon.com/dynamodb/) from inside of the Exasol analytical database. It abstracts over the different interfaces so that you can access the document data just like any regular Exasol table.

## Features

* Read access to data stored in DynamoDB
* Maps unstructured document data to Exasol tables that you can access using regular SQL statements
* Pushes down queries to the remote source
* Distributes the data loading over the whole Exasol Cluster, what makes queries more than six times faster compared to using a JDBC adapter
* Flexible definition of the schema mapping using the [Exasol Document Mapping Language](https://github.com/exasol/virtual-schema-common-document/blob/main/doc/user_guide/edml_user_guide.md)

## Limitations

* In this version the adapter does not support comparisons between two columns

## Information for Users

* [User Guide](doc/user-guide/user_guide.md)
* [Exasol Document MApping Language User Guide](https://github.com/exasol/virtual-schema-common-document/blob/main/doc/user_guide/edml_user_guide.md)
* [Changelog](doc/changes/changelog.md)
* [Dependencies](dependencies.md)

## Information for Developers

* [Developer Guide](doc/development/developer_guide.md)

<!-- @formatter:off -->
[virtual-schema-common-document]: https://github.com/exasol/virtual-schema-common-document
[artifact-reference-checker-plugin]: https://github.com/exasol/artifact-reference-checker-maven-plugin
