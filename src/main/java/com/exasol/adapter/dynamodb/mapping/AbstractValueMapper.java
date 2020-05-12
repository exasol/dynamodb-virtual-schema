package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalker;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalkerException;
import com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider;
import com.exasol.sql.expression.ValueExpression;

/**
 * This class is the abstract basis for mapping a property of an document to an Exasol column.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class AbstractValueMapper<DocumentVisitorType> implements ValueExtractor<DocumentVisitorType> {
    private final PropertyToColumnMapping column;

    /**
     * Creates an instance of {@link ValueExtractor} for extracting a value specified parameter column from a DynamoDB
     * row.
     *
     * @param column {@link PropertyToColumnMapping} defining the mapping
     */
    AbstractValueMapper(final PropertyToColumnMapping column) {
        this.column = column;
    }

    public ValueExpression mapRow(final DocumentNode<DocumentVisitorType> document,
            final PathIterationStateProvider arrayAllIterationState) {
        try {
            final DocumentPathWalker<DocumentVisitorType> walker = new DocumentPathWalker<>(
                    this.column.getPathToSourceProperty(), arrayAllIterationState);
            final DocumentNode<DocumentVisitorType> dynamodbProperty = walker.walkThroughDocument(document);
            return mapValue(dynamodbProperty);
        } catch (final DocumentPathWalkerException | LookupValueMapperException exception) {
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
     * @throws ValueMapperException if the value can't be mapped
     */
    protected abstract ValueExpression mapValue(DocumentNode<DocumentVisitorType> documentValue);
}
