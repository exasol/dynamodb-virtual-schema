# Virtual Schema for AWS DynamoDB

[![Build Status](https://travis-ci.com/exasol/dynamodb-virtual-schema.svg?branch=main)](https://travis-ci.com/exasol/dynamodb-virtual-schema)

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

This adapter allows you to access document data that is stored in a [Amazon's DynamoDB]((https://aws.amazon.com/dynamodb/)) from inside of the Exasol analytical database.
It abstracts over the different interfaces so that you can access the document data just like any regular Exasol table.

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
* [Schema mapping language user guide](https://github.com/exasol/virtual-schema-common-document/blob/main/doc/user_guide/edml_user_guide.md)
* [Schema mapping language reference](https://exasol.github.io/virtual-schema-common-document/schema_doc/edml_1.1.0/index.html)

## Information for Developers

* [Developers Guide](doc/development/developers_guide.md)

## Dependencies

### Run Time Dependencies

Running the DynamoDB Virtual Schema requires a Java Runtime version 11 or later.

| Dependency                                                                          | Purpose                                                     | License                          |
|-------------------------------------------------------------------------------------|-------------------------------------------------------------|----------------------------------|
| [Exasol Virtual Schema Common Document][virtual-schema-common-document]                 | Common module of Exasol Virtual Schemas adapters            | MIT License  
| [AWS SDK for Java](https://aws.amazon.com/de/sdk-for-java/)                         | DynamoDB interface                                          | Apache License 2.0

### Test Dependencies

| Dependency                                                                          | Purpose                                                | License                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|----------------------------------|
| [Apache Maven](https://maven.apache.org/)                                           | Build tool                                             | Apache License 2.0               |
| [Exasol Testcontainers][exasol-testcontainers]                                      | Exasol extension for the Testcontainers framework      | MIT License                      |
| [Java Hamcrest](http://hamcrest.org/JavaHamcrest/)                                  | Checking for conditions in code via matchers           | BSD License                      |
| [JUnit](https://junit.org/junit5)                                                   | Unit testing framework                                 | Eclipse Public License 1.0       |
| [Testcontainers](https://www.testcontainers.org/)                                   | Container-based integration tests                      | MIT License                      |
| [SLF4J](http://www.slf4j.org/)                                                      | Logging facade                                         | MIT License                      |

### Maven Plug-ins

| Plug-in                                                                             | Purpose                                                | License                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|----------------------------------|
| [Maven Compiler Plugin](https://maven.apache.org/plugins/maven-compiler-plugin/)    | Setting required Java version                          | Apache License 2.0               |
| [Maven Exec Plugin](https://www.mojohaus.org/exec-maven-plugin/)                    | Executing external applications                        | Apache License 2.0               |
| [Maven Assembly Plugin](https://maven.apache.org/plugins/maven-assembly-plugin/)    | Creating JAR                                           | Apache License 2.0               |
| [Maven Enforcer Plugin][maven-enforcer-plugin]                                      | Controlling environment constants                      | Apache License 2.0               |
| [Maven Failsafe Plugin](https://maven.apache.org/surefire/maven-surefire-plugin/)   | Integration testing                                    | Apache License 2.0               |
| [Maven Javadoc Plugin](https://maven.apache.org/plugins/maven-javadoc-plugin/)      | Creating a Javadoc JAR                                 | Apache License 2.0               |
| [Maven Jacoco Plugin](https://www.eclemma.org/jacoco/trunk/doc/maven.html)          | Code coverage metering                                 | Eclipse Public License 2.0       |
| [Maven Surefire Plugin](https://maven.apache.org/surefire/maven-surefire-plugin/)   | Unit testing                                           | Apache License 2.0               |
| [Maven Dependency Plugin](https://maven.apache.org/plugins/maven-dependency-plugin/)| Unpacking jacoco agent                                 | Apache License 2.0               |
| [Sonatype OSS Index Maven Plugin][sonatype-oss-index-maven-plugin]                  | Checking Dependencies Vulnerability                    | ASL2                             |
| [Versions Maven Plugin][versions-maven-plugin]                                      | Checking if dependencies updates are available         | Apache License 2.0               |
| [Artifact Reference Checker Plugin][artifact-reference-checker-plugin]              | Check if artifact is referenced with correct version   | MIT License                      |

[exasol-testcontainers]: https://github.com/exasol/exasol-testcontainers
[maven-enforcer-plugin]: http://maven.apache.org/enforcer/maven-enforcer-plugin/
[sonatype-oss-index-maven-plugin]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[versions-maven-plugin]: https://www.mojohaus.org/versions-maven-plugin/
[edml-doc]: https://exasol.github.io/virtual-schema-common-ducument/schema_doc/index.html
[virtual-schema-common-document]: https://github.com/exasol/virtual-schema-common-document
[artifact-reference-checker-plugin]: https://github.com/exasol/artifact-reference-checker-maven-plugin
