# Virtual Schema for AWS DynamoDB

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
Using this Virtual Schema you can access to [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) from Exasol.
SonarCloud results:

 ## Installation
Upload the latest available release of this adapter to BucketFS.

Then create a schema to hold the adapter script.

```
CREATE SCHEMA ADAPTER;
```

Next create the Adapter Script:
 ```
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.DYNAMODB_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/bfsdefault/default/dynamodb-virtual-schemas-adapter-dist-0.2.1.jar;
/
```

In addition to the adapter script we must create a UDF function that will handle the loading of the data:
```
CREATE OR REPLACE JAVA SET SCRIPT ADAPTER.IMPORT_DOCUMENT_DATA(
  DOCUMENT_FETCHER VARCHAR(2000000),
  REMOTE_TABLE_QUERY VARCHAR(2000000),
  CONNECTION_NAME VARCHAR(500))
  EMITS(...) AS
    %scriptclass com.exasol.adapter.dynamodb.ImportDocumentData;
    %jar /buckets/bfsdefault/default/dynamodb-virtual-schemas-adapter-dist-0.2.1.jar;
/
```

## Creating a Virtual Schema
 
For creating a Virtual Schema you need a connection either to AWS or to a local DynamoDB.

For AWS use:

 ```
CREATE CONNECTION DYNAMO_CONNECTION
    TO 'aws:<REGION>'
    USER '<AWS_ACCESS_KEY_ID>'
    IDENTIFIED BY '<AWS_SECRET_ACCESS_KEY>';
```

As a region use for example `eu-central-1`.

For creating a connection to a local [AWS testing instance](https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBLocal.html) use:

```
CREATE CONNECTION DYNAMO_CONNECTION
    TO 'http://localhost:8000'
    USER 'fakeMyKeyId'
    IDENTIFIED BY 'fakeSecretAccessKey';

```

Before creating a Virtual Schema you need to [create mapping definitions](doc/gettingStartedWithSchemaMappingLanguage.md) and upload them to a BucketFS bucket.

Finally create the Virtual Schema using:

```
CREATE VIRTUAL SCHEMA DYNAMODB_TEST USING ADAPTER.DYNAMODB_ADAPTER WITH
    CONNECTION_NAME = 'DYNAMO_CONNECTION'
    SQL_DIALECT     = 'DYNAMO_DB'
    MAPPING         = '/bfsdefault/default/path/to/mappings/in/bucketfs';
```
 

## First Steps
Start with the [mapping definition example](doc/gettingStartedWithSchemaMappingLanguage.md).

# Documentation
* [Schema mapping language reference](https://exasol.github.io/dynamodb-virtual-schema/schema_doc/index.html)
* [Schema mapping software architecture](doc/schemaMappingArchitecture.md)

# Limitations
* In this version the adapter does not support comparisons between two columns

# Logging & Debugging
* [Logging for Virtual Schemas](https://github.com/exasol/virtual-schemas/blob/master/doc/development/remote_logging.md)
* [Remote debugging Virtual Schemas](https://github.com/exasol/virtual-schemas/blob/master/doc/development/remote_debugging.md)
