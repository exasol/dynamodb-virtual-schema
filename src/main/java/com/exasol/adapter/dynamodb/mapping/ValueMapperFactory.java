package com.exasol.adapter.dynamodb.mapping;

public interface ValueMapperFactory<DocumentVisitorType> {
    public AbstractValueMapper<DocumentVisitorType> getValueMapperForColumn(
            final AbstractColumnMappingDefinition column);
}
