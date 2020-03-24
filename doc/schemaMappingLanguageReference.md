# Reference of the DynamoDB to Exasol mapping language
Using the mapping language a mapping from one DynamoDB table to Exasol can be specified. 
Its syntax is based on the JSON Schemas.

The mappings are defined in a json document. This document must contain exactly one object. 
For mapping multiple DynamoDB tables, create multiple files. 

The structure of the mapping follows the structure of the data in the DynamoDB.

## Example
given a DynamoDB table called `MY_BOOKS` containing objects like:

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

this table shall be mapped to an Exasol Table with the following structure:

```
CREATE TABLE BOOKS (
    isbn        VARCHAR(20),
    name        VARCHAR(100),
    authorName  VARCHAR(20)
);
```
Note that the nested property `author.name` is mapped to `authorName`. 

Mapping definition:

```
{
  "$schema": "../../main/resources/mappingLanguageSchema.json",
  "srcTable": "MY_BOOKS",
  "destTableName": "BOOKS",
  "description": "Maps MY_BOOKS to BOOKS",
  "children": {
    "isbn": {
      "toStringMapping": {
        "maxLength": 20,
        "description": "The isbn is mapped to a string with max length of 20",
        "overflow": "ABORT"
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
      "children": {
        "name": {
          "toStringMapping": {
            "maxLength": 20,
            "destName": "authorName",
            "description": "Maps the nested property authors.name to column authorName"
          }
        }
      }
    }
  }
}
```

### More Examples
* [Example for toJSON mapping](exampleWithToJson.md)
* [Example for toTable mapping](exampleWithToTable.md)

## Reference
[Full reference](schema/index.html)
  
