# Reference of the DynamoDB to Exasol mapping language

Using the mapping language a mapping from a DynamoDB table to an Exasol table can be specified. 
Its syntax is based on JSON Schemas.

The mappings are defined in a JSON document. This document must contain exactly one object. 
For mapping multiple DynamoDB tables, you can create multiple files. 

The structure of the mapping follows the structure of the data in the DynamoDB.

## Example

Given a DynamoDB table called `MY_BOOKS` containing objects like:

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

this table will be mapped to an Exasol Table with the following structure:

```
CREATE TABLE BOOKS (
    ISBN        VARCHAR(20),
    NAME        VARCHAR(100),
    AUTHOR_NAME  VARCHAR(20)
);
```
Note that the nested property `author.name` is mapped to `AUTHOR_NAME`. 

Mapping definition:

```
{
  "$schema": "../../main/resources/mappingLanguageSchema.json",
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

After [creating a virtual schema](../README.md) (for example named `BOOKSHOP`) you can query the table using:

```
SELECT * FROM BOOKSHOP.BOOKS;
```

### More Examples
* [Example for toJSON mapping](exampleWithToJson.md)
* [Example for toTable mapping](exampleWithToTable.md)

## Reference
[Schema documentation reference](https://exasol.github.io/dynamodb-virtual-schema/schema_doc/index.html)
