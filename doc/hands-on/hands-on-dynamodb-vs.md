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
You can run the local DynamoDB using plain Java, Maven or as a docker container.

Steps for setup:
 
1. [Install local DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html)
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
Now we need an Exasol database.
You can select between the following options:

* Local Exasol VM (recommended)
    * free
    * simple
* Docker container
    * free
    * only runs on linux
* Use the public demo db
    *  Only applicable with DynamoDB on AWS
* Run DynamoDB on AWS
    * causes costs
    * Only applicable with DynamoDB on AWS
    
Independent of which setup you choose it is important that the Exasol database can read the DynamoDB over network.
Hence you can not use an Exasol DB running in the cloud in combination with an local DynamoDB (ok, it would be possible if you can open a port on your firewall, but probably you don't want to do so). 
