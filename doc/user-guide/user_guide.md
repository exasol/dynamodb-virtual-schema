# DynamoDB Virtual Schema User Guide

This user guide helps with getting started with the DynamoDB Virtual Schemas.

### Installation

Upload the latest available [release of this adapter](https://github.com/exasol/dynamodb-virtual-schema/releases) to BucketFS. See [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) and [Upload the driver to BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/accessfiles.htm) for details.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

Next create the Adapter Script:

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.DYNAMODB_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/bfsdefault/default/document-virtual-schema-dist-11.0.3-dynamodb-3.2.6.jar;
/
```

In addition to the adapter script we must create a UDF function that will handle the loading of the data:
The UDF must be defined in the same schema as the `ADAPTER SCRIPT` (e.g. `ADAPTER`).

```sql
CREATE OR REPLACE JAVA SET SCRIPT ADAPTER.IMPORT_FROM_DYNAMO_DB(
  DATA_LOADER VARCHAR(2000000),
  SCHEMA_MAPPING_REQUEST VARCHAR(2000000),
  CONNECTION_NAME VARCHAR(500))
  EMITS(...) AS
    %scriptclass com.exasol.adapter.document.UdfEntryPoint;
    %jar /buckets/bfsdefault/default/document-virtual-schema-dist-11.0.3-dynamodb-3.2.6.jar;
/
```

## Creating a Connection

For creating a Virtual Schema you need a connection either to AWS or to a local DynamoDB.

For AWS use:

```sql
CREATE CONNECTION DYNAMO_CONNECTION
    TO ''
    USER ''
    IDENTIFIED BY '{
        "awsAccessKeyId": "<AWS ACCESS KEY ID>",
        "awsSecretAccessKey": "<AWS SECRET KEY ID>",
        "awsRegion": "<AWS REGION>"
    }';
```

The connection stores all connection details as JSON in the `IDENTIFIED BY` part. There you can use the following keys:

| Key                   | Default        |  Required  | Example                  |
|-----------------------|----------------|:----------:|--------------------------|
| `awsAccessKeyId`      |                |     ✓      | `"ABCDABCDABCDABCD1234"` |
| `awsSecretAccessKey`  |                |     ✓      |                          |
| `awsRegion`           |                |     ✓      | `"eu-central-1"`         |
| `awsSessionToken`     |                |     ✘      |                          |
| `awsEndpointOverride` | _AWS endpoint_ |     ✘      | `"s3.my-company.de"`     |
| `useSsl`              | `true`         |     ✘      | `false`                  |

By setting `awsSessionToken` you can use two-factor authentication with this Virtual Schema adapter. However, please keep in mind that the token will expire within few hours. So usually it's better to create a machine user without two-factor authentication enabled.

For creating a connection to a local [AWS testing instance](https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBLocal.html) use:

```sql
CREATE CONNECTION DYNAMO_CONNECTION
    TO ''
    USER ''
    IDENTIFIED BY '{
        "awsAccessKeyId": "fakeMyKeyId",
        "awsSecretAccessKey": "fakeSecretAccessKey",
        "awsRegion": "eu-central-1",
        "awsEndpointOverride": "localhost:8000",
        "useSsl": false
    }';
```

## Defining the Schema Mapping

Before creating a Virtual Schema you need to create a mapping definition that defines how the document data is mapped to Exasol tables.

For that we use the Exasol Document Mapping Language (EDML). It is universal over all document virtual schemas. To learn how to define such EDML definitions check the [user guide in the common repository for all document virtual schemas](https://github.com/exasol/virtual-schema-common-document/blob/main/doc/user_guide/edml_user_guide.md).

In the definitions you have to define the `source` property. For DynamoDB you use the name of the DynamoDB table you want to map there.

## Creating the Virtual Schema

Finally create the Virtual Schema using:

```sql
CREATE VIRTUAL SCHEMA DYNAMODB_TEST USING ADAPTER.DYNAMODB_ADAPTER WITH
    CONNECTION_NAME = 'DYNAMO_CONNECTION'
    MAPPING         = '/bfsdefault/default/path/to/mappings/in/bucketfs';
```

The CREATE VIRTUAL SCHEMA command accepts the following properties:

| Property          | Mandatory   |  Default      |   Description                                                                 |
|-------------------|-------------|---------------|-------------------------------------------------------------------------------|
|`MAPPING`          | Yes         |               | Path to the mapping definition file(s)                                        |
|`MAX_PARALLEL_UDFS`| No          | -1            | Maximum number of UDFs that are executed in parallel. -1 represents unlimited.|

Now browse the data using your favorite SQL client.

## Known Limitations

When mapping integer values in DynamoDB to Exasol columns of type `CHAR` or `VARCHAR`
then `WHERE`-clauses in SQL statements may produce wrong results.

Look at the following mapping (see also [EDML user guide for document-based virtual schemas](https://github.com/exasol/virtual-schema-common-document/blob/main/doc/user_guide/edml_user_guide.md#supported-conversion)):

```json
"mapping": {
  "fields": {
    "id": {
      "toStringMapping": {
        "nonStringBehaviour": "CONVERT_OR_ABORT"
      } } } }
```


The `WHERE`-clause of the following SQL statement will return an empty result set:
```sql
SELECT ID FROM MY_SCHEMA.MY_TABLE WHERE ID = '1'
```

This reason for this behavior is that Exasol database pushes down the `WHERE`-clause, while the
DynamoDB virtual schema will not find any values matching the VARCHAR expression `'1'`.

To fix this use `toDecimalMapping` for integer values in DynamoDB.
