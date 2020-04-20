# Getting Started With the DynamoDB to Exasol Mapping Language

For creating a Virtual Schema for DynamoDB you have to define a mapping 
of the DynamoDB document structure to a relational structure.
This is done using the mapping language 
([reference](https://exasol.github.io/dynamodb-virtual-schema/schema_doc/index.html)).
You have to defined the mapping in a JSON document, upload it to a bucket in BucketFS and reference 
in the `CREATE VIRTUAL SCHEMA` call.  
For mapping multiple DynamoDB tables, you can create multiple files, 
upload them to a folder and BucketFS and reference this folder. 

The structure of the mapping follows the structure of the data in the DynamoDB.

## Example

Given a DynamoDB table called `MY_BOOKS` that contains the following objects:

```
{
  "isbn": {
    "S": "1763413749"
  },
  "name": {
    "S": "Accessing NoSQL-Databases in Exasol using Virtual Schemas"
  },
  "author": {
    "M": {
      "name": {
        "S": "Jakob Braun"
      }
    }
  }
}
```
We want to map this table to an Exasol table with the following structure:

```
CREATE TABLE BOOKS (
    ISBN        VARCHAR(20),
    NAME        VARCHAR(100),
    AUTHOR_NAME  VARCHAR(20)
);
```
The nested property `author.name` shall be mapped to the column `AUTHOR_NAME`. 

In order to let this adapter create the described mapping we create the following mapping definition:

```
{
  "$schema": "https://raw.githubusercontent.com/exasol/dynamodb-virtual-schema/master/src/main/resources/mappingLanguageSchema.json",
  "srcTable": "MY_BOOKS",
  "destTable": "BOOKS",
  "description": "Maps MY_BOOKS to BOOKS",
  "mapping": {
    "fields": {
      "isbn": {
        "toStringMapping": {
          "maxLength": 20,
          "description": "The isbn is mapped to a string with max length of 20",
          "overflow": "ABORT",
          "required": true
        }
      },
      "name": {
        "toStringMapping": {
          "maxLength": 100,
          "description": "The name is mapped to a string with max length of 100",
          "overflow": "TRUNCATE"
        }
      },
      "author": {
        "fields": {
          "name": {
            "toStringMapping": {
              "maxLength": 20,
              "destName": "AUTHOR_NAME",
              "description": "Maps the nested property authors.name to column authorName"
            }
          }
        }
      }
    }
  }
}
```

Next we save this definition to a file, upload it to a bucket in 
BucketFS and reference it in the `CREATE VIRTUAL SCHEMA` call.

After running [creating a virtual schema](../README.md) (for example with the schema named `BOOKSHOP`) we can query the table using:

```
SELECT * FROM BOOKSHOP.BOOKS;
```

### More Examples
* [Example for toJsonMapping](exampleWithToJson.md)
* [Example for toTableMapping](exampleWithToTable.md)

## Reference
[Schema mapping language reference](https://exasol.github.io/dynamodb-virtual-schema/schema_doc/index.html)
