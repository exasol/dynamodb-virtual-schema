# Example for `toJSON` mapping

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

This will be mapped to a Exasol table like:

```
CREATE TABLE BOOKS (
    isbn        VARCHAR(20),
    name        VARCHAR(100),
    topics      VARCHAR(200)
);
```
With topics containing a JSON-style array of strings like `["DynamoDB", "Exasol"]`.

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
      "mapping": "toJSON",
      "description": "Maps the sub document of this property to a JSON string",
      "maxLength": 200
    }
  }
}
```
