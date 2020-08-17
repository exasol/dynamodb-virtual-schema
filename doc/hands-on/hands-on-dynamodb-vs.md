# Hands on DynamoDB Virtual Schema

In this Hands on guide we show you how to explore the new Virtual Schema for DynamoDB.

We will create a mapping from DynamoDB's semi structured documents to relational exasol tables like:

**IMAGE- MISSING**

## DynamoDB Setup

First we need a DynamoDB.
For that there are two options:

* DynamoDB on AWS
  * free in [AWS Free Tier](https://aws.amazon.com/de/free/) (credit card required)
* Local test DynamoDB
  * free
  * for testing only

You can use both options to follow this guide. 

### DynamoDB on AWS
1. Get an AWS Account ([AWS Free Tier](https://aws.amazon.com/de/free/))
1. [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
1. [Create an access](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey) key for your AWS account
1. create `~/.aws/credentials` and fill in:
    ```
    [default]
        aws_access_key_id = <YOUR_ACCESS_KEY>
        aws_secret_access_key = <YOUR_SECRET_ACCESS_KEY>
        region = aws-region
    ```
   *Don't forget to replace `<YOUR_ACCESS_KEY>` and `<YOUR_SECRET_ACCESS_KEY>` by you keys from the previous steps.*

1. Verify the setup by typing:
    ``` shell script
   aws help
   ```

### Local DynamoDB

Amazon offers a local version of DynamoDB for testing purposes.
You can run the local DynamoDB using plain Java, Maven or as a docker container (see [AWS documentation]((https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html))).
In this guide we will use the docker version.
You can however also use a different method.

Steps for setup:
 
1. Install DynamoDB:
    ```shell script
    docker run -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath .
    ``` 
1. [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
1. create `~/.aws/credentials` and fill in:
   ```
   [default]
       aws_access_key_id = fakeMyKeyId
       aws_secret_access_key = fakeSecretAccessKey
       region = eu-central-1
   ```
   *The local DynamoDB does not check credentials. So we just need to add something for passing the checks in the CLI.*
   
In the following steps you have to append `aws help --endpoint-url http://localhost:8000` to all `aws dynamodb` commands. 


## Setup sample data

Now we are going to load some example data into the DynamoDB.
For that we first of all need to create a DynamoDB table.
We will do that suing the AWS CLI.
For DynamoDB on AWS there is also a GUI.

You can create the table by simply running the following command in a shell.

``` shell script
aws dynamodb create-table \
    --table-name Books \
    --attribute-definitions \
        AttributeName=Title,AttributeType=S \
    --key-schema \
        AttributeName=Title,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=1,WriteCapacityUnits=1
```

You can verify that the table was created by running:

```shell script
aws dynamodb list-tables
```

The output should contain the `Books` table, we just created.

### Load example data

Now we will load the example data into the newly created table.

Steps:

1. [Download example data](./exampleData.json)
2. Load the data into DynamoDB by running:

``` shell script
aws dynamodb batch-write-item --request-items file://./exampleData.json
```  

## Setup an Exasol database
Now we need an Exasol database. In this guide we will use a local Exasol VM.
Basically you can however select choose between the following options:

* **Local Exasol VM (recommended)**
    * free
    * simple
* [Exasol docker-db](https://github.com/exasol/docker-db)
    * free
    * only runs on linux
* [Exasol public demo](https://docs.exasol.com/get_started/publicdemo/publicdemosystem.htm)
    *  Only applicable with DynamoDB on AWS
* Run [Exasol in the Cloud](https://docs.exasol.com/cloud_platforms/aws/cloud_wizard.htm)
    * causes costs
    * Only applicable with DynamoDB on AWS
    
Independent of which setup you choose it is important that the Exasol database can read the DynamoDB over network.
Hence you can not use an Exasol DB running in the cloud in combination with an local DynamoDB (ok, it would be possible if you can open a port on your firewall, but probably you don't want to do so). 

## Install the Virtual Schema Adapter

In this step we are going to install the dynamodb-virtual-schema adapter.

Steps:

1. [Download latest adapter release (.jar)](https://github.com/exasol/dynamodb-virtual-schema/releases/)
1. [Create a Bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the adapter to the BucketFS:
    ``` shell script
   curl -I -X PUT -T dynamodb-virtual-schemas-adapter-dist-0.4.0.jar
   ```
1. Create a schema to hold the adapter script:
    ```sql
   CREATE SCHEMA ADAPTER;
   ```
1. Create the Adapter Script:
    ```sql
    CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.DYNAMODB_ADAPTER AS
       %scriptclass com.exasol.adapter.RequestDispatcher;
       %jar /buckets/bfsdefault/default/dynamodb-virtual-schemas-adapter-dist-0.4.0.jar;
    /
    ```
1. Create UDF:
    ```sql
    CREATE OR REPLACE JAVA SET SCRIPT ADAPTER.IMPORT_FROM_DYNAMODB(
      DOCUMENT_FETCHER VARCHAR(2000000),
      REMOTE_TABLE_QUERY VARCHAR(2000000),
      CONNECTION_NAME VARCHAR(500))
      EMITS(...) AS
        %scriptclass com.exasol.adapter.dynamodb.ImportDocumentData;
        %jar /buckets/bfsdefault/default/dynamodb-virtual-schemas-adapter-dist-0.4.0.jar;
    /
   ```
   
## Create a Mapping Definition
Now we need to tell the adapter how to map the DynamoDB documents to Exasol tables.
For that we create a file with the [Exasol Document Mapping Language](../gettingStartedWithSchemaMappingLanguage.md).

You can cretae the file wherever you want. We will later upload it to BucketFS. 

`firstMapping.json`:
```json
{
  "$schema": "../../main/resources/mappingLanguageSchema.json",
  "srcTable": "Books",
  "destTable": "BOOKS",
  "description": "Mapping for the Books table",
  "mapping": {
    "fields": {
      "Title": {
        "toVarcharMapping": {
          "varcharColumnSize": 254
        }
      }
    }
  }
}
``` 

Now upload the mapping to BucketFS:

```shell script
curl -I -X PUT -T firstMapping.json http://w:writepw@<YOUR_DB_IP>:2580/default/mappings/firstMapping.json
```

## Create Virtual Schema

Nwo we can create the Virtual Schema.

Steps:

1. Create a connection to DynamoDB
    * For DynamoDB on AWS use:
       ```sql
      CREATE CONNECTION DYNAMO_CONNECTION
         TO 'aws:<REGION>'
         USER '<AWS_ACCESS_KEY_ID>'
         IDENTIFIED BY '<AWS_SECRET_ACCESS_KEY>';
      ```
    * For a local DynamoDB use:
       ```sql
      CREATE CONNECTION DYNAMO_CONNECTION
          TO 'http://<YOUR_DYNAMODB_IP>:8000'
          USER 'fakeMyKeyId'
          IDENTIFIED BY 'fakeSecretAccessKey';
      ```
      
2. Create Virtual Schema:
    ```sql
   CREATE VIRTUAL SCHEMA DYNAMODB_TEST USING ADAPTER.DYNAMODB_ADAPTER WITH
       CONNECTION_NAME = 'DYNAMO_CONNECTION'
       SQL_DIALECT     = 'DYNAMO_DB'
       MAPPING         = '/bfsdefault/default/mappings/firstMapping.json';
   ```
 
 ## First Results
 
 ## Next Steps
 In the next part of this series we will show how to create more complex mappings.
 