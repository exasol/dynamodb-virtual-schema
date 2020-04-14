# Schema Mapping Software Architecture

The `JsonMappingFactory` reads the schema mapping from a [schema mapping file](https://exasol.github.io/dynamodb-virtual-schema/schema_doc/index.html) 
. It builds a mapping representation using the following class structure:

![Class diagram](diagrams/mappingDefinition.png)

This structure is used:
* By the `SchemaMappingDefinitionToSchemaMetadataConverter` for generating `SchemaMetadata` that is send to Exasol at `CREATE VIRTUAL SCHEMA` or `REFRESH`. 
* For Mapping the remote attribute values to Exasol values according to this definition.

## Mapping Remote Attributes

Remote values are mapped to Exasols `ValueExpression`s using a `ValueMapper`:

![Class diagram](diagrams/valueMapper.png)

A `ValueMapper` corresponding to a specific `AbstractColumnMappingDefinition` is built using a `ValueMapperFactory`:

![Class diagram](diagrams/valueMapperFactory.png)


