# Example for `toTable` mapping

Given a DynamoDB table called `MY_BOOKS` containing objects like:

```
{
  "isbn": {
    "S": "1763413749"
  },
  "name": {
    "S": "Accessing NoSQL-Databases in Exasol using Virtual Schemas"
  },
  "topics": {
    "L": [
      {
        "S": "DynamoDB"
      },
      {
        "S": "Exasol"
      },
    ]
  }
}
```

The primary key for the DynamoDB table is `isbn`.

This shall be normalized to the following relational structure:

![Class diagram](diagrams/mappingToTable.png)

Note that BOOKS_TOPICS uses isbn as FOREIGN KEY.

Mapping definition:
```
{
  "$schema": "https://github.com/exasol/dynamodb-virtual-schema/tree/master/schemaMapping/schema",
  "srcTable": "MY_BOOKS",
  "destTableName": "BOOKS",
  "description": "Maps MY_BOOKS to BOOKS with toJSON",
  "children": {
    "isbn": {
      "mapping": "toString",
      "maxLength": 20,
      "overflow": "ABORT"
    },
    "name": {
      "mapping": "toString",
      "maxLength": 100,
      "overflow": "TRUNCATE"
    },
    "topics":{
      "mapping": "toTable",
      "description": "Maps the strings in this list to rows in the separate table BOOKS_TOPICS",
      "children": {
        "name": {
          "mapping": "toString",
          "maxLength": 20
        }
      }
    }
  }
}
```
