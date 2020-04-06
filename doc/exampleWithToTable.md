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

It will be normalized to the following relational structure:

![Class diagram](diagrams/mappingToTable.png)

Note that `BOOKS_TOPICS` uses `ISBN` as FOREIGN KEY.

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
          "overflow": "ABORT"
        }
      },
      "name": {
        "toStringMapping": {
          "maxLength": 100,
          "overflow": "TRUNCATE"
        }
      },
      "topics": {
        "toTableMapping": {
          "mapping": {
            "toStringMapping": {
              "destName": "TOPIC_NAME"
            }
          }
        }
      }
    }
  }
}
```
