# Example of `toJsonMapping`

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

We want to map this DynamoDB table to an Exasol table in the following way:

```
CREATE TABLE BOOKS (
    ISBN        VARCHAR(20),
    NAME        VARCHAR(100),
    TOPICS      VARCHAR(200)
);
```
 
`TOPICS` shall be a `VARCHAR` column containing a JSON strings like `["DynamoDB", "Exasol"]`.

To achieve this we create the following mapping definition:  

```
{
  "$schema": "https://raw.githubusercontent.com/exasol/dynamodb-virtual-schema/master/src/main/resources/mappingLanguageSchema.json",
  "srcTable": "MY_BOOKS",
  "destTable": "BOOKS",
  "description": "Maps MY_BOOKS to BOOKS with toJSON",
  "mapping": {
    "fields": {
      "isbn": {
        "toVarcharMapping": {
          "varcharColumnSize": 20,
          "overflowBehaviour": "ABORT"
        }
      },
      "name": {
        "toVarcharMapping": {
          "varcharColumnSize": 100,
          "overflowBehaviour": "TRUNCATE"
        }
      },
      "topics": {
        "toJsonMapping": {
          "description": "Maps the sub document of this property to a JSON string",
          "varcharColumnSize": 200
        }
      }
    }
  }
}
```

The toJsonMapping will map the nested document `topics` to a JSON string in a `TOPICS` column.
