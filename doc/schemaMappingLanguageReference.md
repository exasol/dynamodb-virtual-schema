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
  "$schema": "https://github.com/exasol/dynamodb-virtual-schema/tree/master/schemaMapping/schema",
  "srcTable": "MY_BOOKS",
  "destTableName": "BOOKS",
  "description": "Maps MY_BOOKS to BOOKS",
  "children": {
    "isbn": {
      "mapping": "toString",
      "maxLength": 20,
      "description": "The isbn is mapped to a string with max length of 20",
      "overflow": "ABORT"
    },
    "name": {
      "mapping": "toString",
      "maxLength": 100,
      "description": "The name is mapped to a string with max length of 100",
      "overflow": "TRUNCATE"
    },
    "author":{
      "children":{
        "name": {
          "mapping": "toString",
          "maxLength": 20,
          "destName": "authorName",
          "description": "Maps the nested property authors.name to column authorName"
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

### General properties
Each object defines the following properties. Depending on the value of `mapping` other properties are possible or required.

| name | required | default | description |
|---|:---:|---|---|
| `children` | for root | | object with child definitions
| `description` | | | just for documentation. Not parsed.
| `mapping` | for leaves | for root: `toTable`| specifies how the type is mapped. For details see Mappings section.

### Root Object
The root object has the following special properties:

| name | required | default | description |
|---|:---:|---|---|
|`$schema` | ✓ | | pointing to the schema URL (https://github.com/exasol/dynamodb-virtual-schema/tree/master/schemaMapping/schema).
| `srcTable` | ✓ | | the identifier of the DynamoDB table
| `destTableName` | ✓ | | the name of the resulting table in Exasol. The name must only contain [a-z A-Z 0-9].

### Mappings
 
 #### `toString`
 Maps the selected DynamoDB property to a string.
 
 Supported DynamoDB types:  String (S), Number (N), Boolean (BOOL)
 
 | name | required | default | description |
 |---|:---:|---|---|
 | `maxLength` | | 254 | length of the exasol `VARCHAR`
 | `overflow`  | | `TRUNCATE` | Behaviour if the read value from DynamoDB is longer than the specified maxLength. Possible values: `TRUNCATE` (truncates the string to the given length); `ABORT` aborts the query
 | `destName` | | name of the DynamoDB property | Name of the destination column. Must be unique.  
 
 #### `toDecimal`
 *not yet implemented*
 
 #### `toJSON`
 Maps the selected DynamoDB property and all its descendants to a JSON string.
 
 Supported DynamoDB types:  Map (M), List (L). The root document is also supported.
 
 | name | required | default | description |
 |---|:---:|---|---|
 | `maxLength` | | 254 | length of the exasol `VARCHAR`; If result is to long column will be set to NULL.
 | `overflow`  | | `TRUNCATE` | Behaviour if the result JSON is longer than the specified maxLength. Possible values: `NULL` (set result column to `NULL`); `ABORT` aborts the query
 | `destName` | | name of the DynamoDB property | Name of the destination column. Must be unique.
 
 #### `toTable` 
 This mapping is used for normalizing documents from DynamoDB. 
 It creates a new table in resulting virtual schema named: <NAME_OF_PARENT>_<GIVEN_NAME>. 
 The columns of that new table are defined using the child definitions. 
 If the parent table defined a key using one or more toString mappings for a 
 DynamoDB primary key or secondary index this will be used as foreign key.  
  
 Supported DynamoDB types:  List (L).
  
 Properties:
  
 | name | required | default | description |
 |---|:---:|---|---| 
 | `destTableName` |  | name of this DynamoDB property| the name of the resulting table in Exasol. The name must only contain [a-z A-Z 0-9].

#### `pickFromList`
Picks an specific element from a DynamoDB list.

Supported DynamoDB types:  List (L).

*not yet implemented*
  
