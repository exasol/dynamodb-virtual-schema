package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalker;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathWalkerException;
import com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider;
import com.exasol.sql.expression.ValueExpression;

/**
 * Abstract class for extracting a value specified in an ColumnMappingDefinition from a DynamoDB row.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class AbstractValueMapper<DocumentVisitorType> {
    private final ColumnMappingDefinition column;

    /**
     * Creates an instance of {@link AbstractValueMapper} for extracting a value specified parameter column from a
     * DynamoDB row.
     * 
     * @param column ColumnMappingDefinition defining the mapping
     */
    AbstractValueMapper(final ColumnMappingDefinition column) {
        this.column = column;
    }

    /**
     * Extracts {@link #column}s values from the given document.
     *
     * @param document               to extract the value from
     * @param arrayAllIterationState array all iteration state used for extracting the correct values for nested lists
     * @return {@link ValueExpression}
     * @throws DocumentPathWalkerException if specified property was not found and {@link LookupFailBehaviour} is set to
     *                                     {@code EXCEPTION }
     * @throws ValueMapperException        if specified property can't be mapped and {@link LookupFailBehaviour} is set
     *                                     to {@code EXCEPTION }
     */
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
     * Converts the DynamoDB property into an Exasol {@link ValueExpression}.
     *
     * @param dynamodbProperty the DynamoDB property to be converted
     * @return the conversion result
     * @throws ValueMapperException if the value can't be mapped
     */
    protected abstract ValueExpression mapValue(DocumentNode<DocumentVisitorType> dynamodbProperty);
}
