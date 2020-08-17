package com.exasol.adapter.document.mapping;

import java.util.Optional;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentpath.DocumentPathWalker;
import com.exasol.adapter.document.documentpath.PathIterationStateProvider;
import com.exasol.sql.expression.NullLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * This class is the abstract basis for mapping a property of a document to an Exasol column. It provides functionality
 * for extracting the the property described by the path in the {@link PropertyToColumnMapping}. The conversion of the
 * value is delegated to the implementation using the abstract method {@link #mapValue(DocumentNode)}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class AbstractPropertyToColumnValueExtractor<DocumentVisitorType>
        implements ColumnValueExtractor<DocumentVisitorType> {
    private final PropertyToColumnMapping column;

    /**
     * Create an instance of {@link AbstractPropertyToColumnValueExtractor} for extracting a value specified parameter
     * column from a DynamoDB row.
     *
     * @param column {@link PropertyToColumnMapping} defining the mapping
     */
    AbstractPropertyToColumnValueExtractor(final PropertyToColumnMapping column) {
        this.column = column;
    }

    @Override
    public ValueExpression extractColumnValue(final DocumentNode<DocumentVisitorType> document,
            final PathIterationStateProvider arrayAllIterationState) {
        final DocumentPathWalker<DocumentVisitorType> walker = new DocumentPathWalker<>(
                this.column.getPathToSourceProperty(), arrayAllIterationState);
        final Optional<DocumentNode<DocumentVisitorType>> dynamodbProperty = walker.walkThroughDocument(document);
        if (dynamodbProperty.isEmpty()) {
            if (this.column.getLookupFailBehaviour() == MappingErrorBehaviour.NULL) {
                return NullLiteral.nullLiteral();
            } else {
                throw new SchemaMappingException("Could not find required property ("
                        + this.column.getPathToSourceProperty() + ") in the source document.");
            }
        } else {
            return mapValue(dynamodbProperty.get());
        }
    }

    /**
     * Converts a document property into an Exasol {@link ValueExpression}.
     *
     * @param documentValue the document value specified in the columns path expression to be converted
     * @return the conversion result
     * @throws ColumnValueExtractorException if the value can't be mapped
     */
    protected abstract ValueExpression mapValue(DocumentNode<DocumentVisitorType> documentValue);
}
