package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalker;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalkerException;
import com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider;
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
     * Creates an instance of {@link AbstractColumnValueExtractor} for extracting a value specified parameter column
     * from a DynamoDB row.
     *
     * @param column {@link PropertyToColumnMapping} defining the mapping
     */
    AbstractPropertyToColumnValueExtractor(final PropertyToColumnMapping column) {
        this.column = column;
    }

    @Override
    public ValueExpression extractColumnValue(final DocumentNode<DocumentVisitorType> document,
            final PathIterationStateProvider arrayAllIterationState) {
        try {
            final DocumentPathWalker<DocumentVisitorType> walker = new DocumentPathWalker<>(
                    this.column.getPathToSourceProperty(), arrayAllIterationState);
            final DocumentNode<DocumentVisitorType> dynamodbProperty = walker.walkThroughDocument(document);
            return mapValue(dynamodbProperty);
        } catch (final DocumentPathWalkerException | ColumnValueExtractorLookupException exception) {
            if (this.column.getLookupFailBehaviour() == LookupFailBehaviour.DEFAULT_VALUE) {
                return this.column.getExasolDefaultValue();
            } else {
                throw exception;
            }
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
