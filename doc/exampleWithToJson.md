# Example of `toJSON` Mapping

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

We map this table to an Exasol table in the following way:

```
CREATE TABLE BOOKS (
    ISBN        VARCHAR(20),
    NAME        VARCHAR(100),
    TOPICS      VARCHAR(200)
);
```
With topics containing a JSON-style array of strings like `["DynamoDB", "Exasol"]`.

Mapping definition:

```
{
  "$schema": "../../main/resources/mappingLanguageSchema.json",
  "srcTable": "MY_BOOKS",
  "destTable": "BOOKS",
  "description": "Maps MY_BOOKS to BOOKS with toJSON",
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
        "toJsonMapping": {
          "description": "Maps the sub document of this property to a JSON string",
          "maxLength": 200
        }
      }
    }
  }
}
```
