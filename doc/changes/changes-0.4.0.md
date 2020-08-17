# dynamodb-virtual-schema 0.4.0, released 2020-08-XX
 
## Summary

This pre-release introduces selection & projection pushdown for DynamoDB query generation 
and query parallelization. 

## Features / Enhancements
 
* #11: Added toDecimalMapping
* #48: EDML: renamed destName -> destinationName    
    * destName -> destinationTable
    * toStringMapping -> toVarcharMapping
    * maxLength -> varcharColumnSize
    * overflow -> overflowBehaviour
* #81: Support for any column projection 
* #94 Dependency updates
* #93 Dependency monitoring
* #105 Refactored PropertyToJsonColumnMapping and PropertyToVarcharColumnMapping to use a builder
 
