package com.exasol.adapter.dynamodb.mapping;

/**
 * Factory for {@link ValueExtractor}s.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface AbstractValueMapperFactory<DocumentVisitorType> {
    /**
     * Builds a ValueMapper fitting into a ColumnMappingDefinition.
     *
     * @param column ColumnMappingDefinition for which to build the ValueMapper
     * @return built ValueMapper
     */
    public ValueExtractor<DocumentVisitorType> getValueMapperForColumn(final PropertyToColumnMapping column);
}
