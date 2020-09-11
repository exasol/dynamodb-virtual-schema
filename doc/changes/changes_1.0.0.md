# dynamodb-virtual-schema 1.0.0, released 2020-09-11
 
Code name: First production ready release
 
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

## Documentation

* #132 Updated doc for changed EDML version

## Dependency updates

* Added com.exasol:virtual-schema-common-document:1.0.0
* Added com.exasol:artifact-reference-checker-maven-plugin:0.3.1