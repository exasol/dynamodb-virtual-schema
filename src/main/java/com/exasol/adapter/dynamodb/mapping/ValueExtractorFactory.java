package com.exasol.adapter.dynamodb.mapping;

/**
 * This class is a factory for {@link ValueExtractor}s.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class ValueExtractorFactory<DocumentVisitorType> {
    private final AbstractValueMapperFactory<DocumentVisitorType> valueMapperFactory;

    /**
     * Creates an instance of {@link ValueExtractorFactory}.
     * 
     * @param valueMapperFactory {@link AbstractValueMapperFactory} for building {@link AbstractValueMapper}s
     */
    public ValueExtractorFactory(final AbstractValueMapperFactory<DocumentVisitorType> valueMapperFactory) {
        this.valueMapperFactory = valueMapperFactory;
    }

    /**
     * Builds a {@link ValueExtractor} for a given {@link ColumnMapping}.
     * 
     * @param columnMapping {@link ColumnMapping} do build the {@link ValueExtractor} for.
     * @return built {@link ValueExtractor}
     */
    public ValueExtractor<DocumentVisitorType> getValueExtractorForColumn(final ColumnMapping columnMapping) {
        final Visitor visitor = new Visitor();
        columnMapping.accept(visitor);
        return visitor.getExtractor();
    }

    private class Visitor implements ColumnMappingVisitor {
        private ValueExtractor<DocumentVisitorType> extractor;

        @Override
        public void visit(final PropertyToColumnMapping propertyToColumnMapping) {
            this.extractor = ValueExtractorFactory.this.valueMapperFactory
                    .getValueMapperForColumn(propertyToColumnMapping);
        }

        @Override
        public void visit(final IterationIndexColumnMapping iterationIndexColumnDefinition) {
            this.extractor = new IterationIndexValueExtractor<>(iterationIndexColumnDefinition);
        }

        public ValueExtractor<DocumentVisitorType> getExtractor() {
            return this.extractor;
        }
    }
}
