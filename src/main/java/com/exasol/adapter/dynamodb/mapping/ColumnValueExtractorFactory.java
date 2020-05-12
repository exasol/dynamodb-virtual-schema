package com.exasol.adapter.dynamodb.mapping;

/**
 * This class is a factory for {@link ColumnValueExtractor}s.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class ColumnValueExtractorFactory<DocumentVisitorType> {
    private final PropertyToColumnValueExtractorFactory<DocumentVisitorType> propertyToColumnValueExtractorFactory;

    /**
     * Creates an instance of {@link ColumnValueExtractorFactory}.
     * 
     * @param propertyToColumnValueExtractorFactory {@link PropertyToColumnValueExtractorFactory} for building
     *                                              {@link AbstractPropertyToColumnValueExtractor}s
     */
    public ColumnValueExtractorFactory(
            final PropertyToColumnValueExtractorFactory<DocumentVisitorType> propertyToColumnValueExtractorFactory) {
        this.propertyToColumnValueExtractorFactory = propertyToColumnValueExtractorFactory;
    }

    /**
     * Builds a {@link ColumnValueExtractor} for a given {@link ColumnMapping}.
     * 
     * @param columnMapping {@link ColumnMapping} do build the {@link ColumnValueExtractor} for.
     * @return built {@link ColumnValueExtractor}
     */
    public ColumnValueExtractor<DocumentVisitorType> getValueExtractorForColumn(final ColumnMapping columnMapping) {
        final Visitor visitor = new Visitor();
        columnMapping.accept(visitor);
        return visitor.getExtractor();
    }

    private class Visitor implements ColumnMappingVisitor {
        private ColumnValueExtractor<DocumentVisitorType> extractor;

        @Override
        public void visit(final PropertyToColumnMapping propertyToColumnMapping) {
            this.extractor = ColumnValueExtractorFactory.this.propertyToColumnValueExtractorFactory
                    .getValueExtractorForColumn(propertyToColumnMapping);
        }

        @Override
        public void visit(final IterationIndexColumnMapping iterationIndexColumnDefinition) {
            this.extractor = new IterationIndexColumnValueExtractor<>(iterationIndexColumnDefinition);
        }

        public ColumnValueExtractor<DocumentVisitorType> getExtractor() {
            return this.extractor;
        }
    }
}
