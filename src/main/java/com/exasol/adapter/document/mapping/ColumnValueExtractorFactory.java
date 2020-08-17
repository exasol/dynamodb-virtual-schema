package com.exasol.adapter.document.mapping;

/**
 * This class is a factory for {@link ColumnValueExtractor}s.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class ColumnValueExtractorFactory<DocumentVisitorType> {
    private final PropertyToColumnValueExtractorFactory<DocumentVisitorType> propertyToColumnValueExtractorFactory;

    /**
     * Create an instance of {@link ColumnValueExtractorFactory}.
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
     * @param columnMapping {@link ColumnMapping} that builds the {@link ColumnValueExtractor}.
     * @return built {@link ColumnValueExtractor}
     */
    public ColumnValueExtractor<DocumentVisitorType> getValueExtractorForColumn(final ColumnMapping columnMapping) {
        final Visitor<DocumentVisitorType> visitor = new Visitor<>(this.propertyToColumnValueExtractorFactory);
        columnMapping.accept(visitor);
        return visitor.getExtractor();
    }

    private static class Visitor<DocumentVisitorType> implements ColumnMappingVisitor {
        private final PropertyToColumnValueExtractorFactory<DocumentVisitorType> propertyToColumnValueExtractorFactory;
        private ColumnValueExtractor<DocumentVisitorType> extractor;

        private Visitor(
                final PropertyToColumnValueExtractorFactory<DocumentVisitorType> propertyToColumnValueExtractorFactory) {
            this.propertyToColumnValueExtractorFactory = propertyToColumnValueExtractorFactory;
        }

        @Override
        public void visit(final PropertyToColumnMapping propertyToColumnMapping) {
            this.extractor = this.propertyToColumnValueExtractorFactory
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
