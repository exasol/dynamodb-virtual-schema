# Virtual Schema for DynamoDB
Using this Virtual Schema you can access to Amazons DynamoDB from Exasol 

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
    %jar /buckets/bfsdefault/default/dynamodb-virtual-schemas-adapter-dist-0.0.1.jar;
/
```

## Creating a Virtual Schema
 
For creating a virtual schema you need a connection either to aws or to a local DynamoDB.

For aws use: 
 ```
CREATE CONNECTION DYNAMO_CONNECTION
    TO 'aws'
    USER '<AWS_ACCESS_KEY_ID>'
    IDENTIFIED BY '<AWS_SECRET_ACCESS_KEY>';
```

For creating a connection to a local [aws testing instance](https://docs.aws.amazon.com/de_de/amazondynamodb/latest/developerguide/DynamoDBLocal.html) use:
```
CREATE CONNECTION DYNAMO_CONNECTION
    TO 'http://localhost:8000'
    USER 'fakeMyKeyId'
    IDENTIFIED BY 'fakeSecretAccessKey';

```

Finally create the Virtual Schema using:
```
CREATE VIRTUAL SCHEMA DYNAMODB_TEST USING ADAPTER.DYNAMODB_ADAPTER WITH
    CONNECTION_NAME = 'DYNAMO_CONNECTION'
    SQL_DIALECT     = 'Dynamodb';
```

## First steps:
start for example with:
```
SELECT * FROM ASDF."testTable";
```

# Debugging
For receiving virtual schema logs during integration tests use:
```
    nc -lkp 3000
```

