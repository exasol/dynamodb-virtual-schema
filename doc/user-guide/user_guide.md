# DynamoDB Virtual Schema User Guide

This user guide helps with getting started with the DynamoDB Virtual Schemas.

### Installation
 
Upload the latest available [release of this adapter](https://github.com/exasol/dynamodb-virtual-schema/releases) to BucketFS.
See [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) and [Upload the driver to BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/accessfiles.htm) for details.

Then create a schema to hold the adapter script.

```
CREATE SCHEMA ADAPTER;
```

Next create the Adapter Script:
 ```
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.DYNAMODB_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/bfsdefault/default/dynamodb-virtual-schemas-adapter-dist-0.4.0.jar;
/
```

In addition to the adapter script we must create a UDF function that will handle the loading of the data:
```
CREATE OR REPLACE JAVA SET SCRIPT ADAPTER.IMPORT_DOCUMENT_DATA(
  DOCUMENT_FETCHER VARCHAR(2000000),
  REMOTE_TABLE_QUERY VARCHAR(2000000),
  CONNECTION_NAME VARCHAR(500))
  EMITS(...) AS
    %scriptclass com.exasol.adapter.document.ImportDocumentData;
    %jar /buckets/bfsdefault/default/dynamodb-virtual-schemas-adapter-dist-0.4.0.jar;
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

Before creating a Virtual Schema you need to [create mapping definitions](../gettingStartedWithSchemaMappingLanguage.md) and upload them to a BucketFS bucket.

Finally create the Virtual Schema using:

```
CREATE VIRTUAL SCHEMA DYNAMODB_TEST USING ADAPTER.DYNAMODB_ADAPTER WITH
    CONNECTION_NAME = 'DYNAMO_CONNECTION'
    SQL_DIALECT     = 'DYNAMO_DB'
    MAPPING         = '/bfsdefault/default/path/to/mappings/in/bucketfs';
```

The CREATE VIRTUAL SCHEMA command accepts the following properties:

| Property          | Mandatory   |  Default      |   Description                                                                 |
|-------------------|-------------|---------------|-------------------------------------------------------------------------------|
|`MAPPING`          | Yes         |               | Path to the mapping definition file(s)                                        |
|`MAX_PARALLEL_UDFS`| No          | -1            | Maximum number of UDFs that are executed in parallel. -1 represents unlimited.|
 
Now browse the data using your favorite SQL client.
