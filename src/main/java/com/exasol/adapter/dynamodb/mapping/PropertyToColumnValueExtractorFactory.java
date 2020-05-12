package com.exasol.adapter.dynamodb.mapping;

/**
 * Factory for {@link ColumnValueExtractor}s. In contrast to {@link ColumnValueExtractorFactory} this interface for the
 * subset of {@link PropertyToColumnMapping} columns.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface PropertyToColumnValueExtractorFactory<DocumentVisitorType> {
    /**
     * Builds a ValueMapper fitting into a ColumnMappingDefinition.
     *
     * @param column ColumnMappingDefinition for which to build the ValueMapper
     * @return built ValueMapper
     */
    public ColumnValueExtractor<DocumentVisitorType> getValueExtractorForColumn(final PropertyToColumnMapping column);
}
