# dynamodb-virtual-schema 0.5.0, released 2020-08-XX
 
Code name: Moved common document code to separate repository 
 
## Summary

Moved common document code to separate repository and some minor enhancements and fixes

## Features / Enhancements
 
* #45 Separated DynamoDB and document code 
* #112 Changed repository of serial-version-uid-change-checker
* #113: Schema mapping language changes
    * `destTable` -> `destinationTable`
    * `srcTable` -> `source`
* #71: Allow underscores in Table names
* #119: Removed XML-RPC dependency 
* #121: Updated version schema
* #127: Fixed bug than hid failing tests and fixed failing tests
* #130: Migrated from version.sh to artifact-reference-checker-maven-plugin

## Dependency updates

* Added com.exasol:virtual-schema-common-document:0.3.0
* Added com.exasol:artifact-reference-checker-maven-plugin:0.2.0